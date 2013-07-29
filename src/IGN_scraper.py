# Scrapes title title, path, platform, publisher, score, 
# and release date for each game in IGN's index. Optionally generates a csv
# with this scraped data.
#
# IGN exposes its index of games through:
#
# http://www.ign.com/games/all-ajax?startIndex=[index]
# 
# where [index] is any non-negative multiple of 50 less than some
# maximum -- the maximum grows as IGN's catalog of games grows.
# 
# In order to use this script, you must provide it with
# an end index. The script will fetch data from the start index
# (0 by default) to the end index, inclusive. 
#
# Written by Akshay Agrawal
# July 28, 2013

import urllib2 				  # For URL opening
from bs4 import BeautifulSoup # For HTML parsing
import sys
import argparse
import threading 

INDEX_INCREMENT = 50 # IGN's index increments by this fixed amount.
MAX_NUM_THREADS = 8
num_threads_allowed = threading.Semaphore(MAX_NUM_THREADS)
outfile_lock = threading.Lock()
urlopen_lock = threading.Lock()
stderr_lock = threading.Lock()
print_lock = threading.Lock()

# Entries for games are encoded as follows:
# <div class="clear itemList-itemShort">
# 	<div class-"grid_7 alpha">
#    	 <div class="item_title">
# 	    	  <a href="[path to IGN's page on this game"]>
# 		    	  [name of game] </a>
# 	  	  	<span class="item-platform [platform abbr.]">[platform]</span>
#		 </div>
# 	</div>
# 		<div class="publisher grid_3">
#				[publisher]  </div>
#	   	<div class="grid_3">
#	   			[score]  </div>
#	   	<div class="releaseDate grid_3 omega">
#				[Release Date] </div>
# </div>
# Note: The square brackets are not included in the HTML
def parse_page(outfile, page_contents, score_flag, verbose):
	htmltree = BeautifulSoup(page_contents) 
	titles_links_platforms = htmltree.find_all("div", class_="item-title") 
	publishers_scores_dates = htmltree.find_all("div", "grid_3")
	publishers_list = []
	scores_list = []
	release_dates_list = []

	# Extract the publisher, score, and release date
	# for each game from their respective tags.
	for tag in publishers_scores_dates:
		# Based on the number of fields in the class tag, we know
		# whether we're looking at a score, publisher, or release date
		numFields = len(tag['class'])
		if numFields == 1:
			scores_list.append(tag.string.strip())
		elif numFields == 2:
			publishers_list.append(tag.string.strip())
		elif numFields == 3:
			release_dates_list.append(tag.string.strip())
	
	# For each tag in titles_links_platforms, extract the game title, link,
	# and platform data, and join them together with the publisher, score and
	# release date. Publish the data to the outfile if requested. 
	NO_SCORE = "NR"
	count = 0
	for tag in titles_links_platforms:
		 # Optionally omit non-rated games. 
		if ((score_flag and scores_list[count] == NO_SCORE)):
			count += 1
			continue

		# Extract the title and platform from the item-title tag. 
		title_plat = list(tag.stripped_strings) 
		title = "\"" + title_plat[0] + "\""
		if len(title_plat) > 1:	    
			plat = "\"" + title_plat[1] + "\""
		# Some games might not have a platform. See index 15750, Devil's Third.
		else:
			plat = "<unknown>"
		# Obtain the <a> tag and extract the link from its href attribute. 
		# Note that tag.contents[0] is a newline. 
		link_tag = tag.contents[1]
		link = link_tag['href']

		# Format the publisher, score, and release date. 
		#
		# The count-th entry in each of publishers_list, scores_list, and
		# release_dates_list corresponds to the count-th tag
		# in titles_links_platforms.
		publisher = "\"" + publishers_list[count] + "\""
		# Non-rated games are assigned a score of -1.
		score = scores_list[count] if scores_list[count] != NO_SCORE else "-1"
		release = "\"" + release_dates_list[count] + "\""

		# Publish the data.
		if outfile != None:
			outfile_lock.acquire()
			outfile.write(("%s,%s,%s,%s,%s,%s\n" % (title, link, plat,
				publisher, score, release)).encode("utf-8"))
			outfile_lock.release()
		if verbose: 
			print_lock.acquire()
			print ("Title: %s\nURL: %s\nPlatform: %s\nScore: %s\nRelease: %s\n"
				% (title, link, plat, score, release)).encode("utf-8")
			print_lock.release()
		count += 1

# Open the url, read its contents and send it for parsing.
# If we fail to open the url, print an error message indicating as much.
def open_url_and_parse(*args):
	outfile, curr_url, score_flag, force_max, verbose = args
	IGN_ERROR_MESSAGE = "No Results"
	page_contents = ""
	urlfile = None
	# As a heuristic, try to open the URL force_max times before giving up.
	run = 0;
	while run < force_max:
		if verbose:
			print_lock.acquire()
			print "Opening url: %s" % curr_url
			print "%d threads are working" % (threading.active_count() - 1)
			print_lock.release()
		try:
			urlopen_lock.acquire()
			urlfile = urllib2.urlopen(curr_url);
			urlopen_lock.release()
		except urllib2.HTTPError as e:
			stderr_lock.acquire()
			sys.stderr.write("HTTP error({0}): {1} with url {2}\n".format(
				e.errno, e.strerror, curr_url))
			stderr_lock.release()
			urlopen_lock.release()
			run += 1
			continue
		except urllib2.URLError as e:
			stderr_lock.acquire()
			sys.stderr.write("URLError({0}): {1} with url {2}\n".format(
				e.errno, e.strerror, curr_url))
			stderr_lock.release()
			urlopen_lock.release()
			run += 1
			continue
		# IGN_ERROR_MESSAGE is served a) at invalid indices but also b) on
		# a request that fails. b) is unpredictable.
		page_contents = urlfile.read();
		if page_contents == "" or page_contents.startswith(IGN_ERROR_MESSAGE):	
			run += 1
			continue
		break
	if urlfile is not None:
		urlfile.close()
	if (run >= force_max):
		stderr_lock.acquire()
		sys.stderr.write("Failed to open url: %s\n" % curr_url)
		stderr_lock.release()
		num_threads_allowed.release()
		return
	parse_page(outfile, page_contents, score_flag, verbose)
	num_threads_allowed.release()

# For each page in the index, spawn a thread to open the page,
# parse it, and publish the extracted data. 
def launch_workers(outfile, start_index, end_index, score_flag, force, verbose):
	BASE_URL = "http://www.ign.com/games/all-ajax?startIndex="
	curr_index = start_index;
	threads = []
	if (outfile != None):
		outfile.write("title,link,platform,publisher,score,date\n")
	while curr_index <= end_index:
		curr_url = BASE_URL + str(curr_index)
	 	parser_thread = threading.Thread(None, open_url_and_parse,
	 		None, (outfile, curr_url, score_flag, force, verbose))
	 	threads.append(parser_thread)
	 	if verbose:
			print_lock.acquire()
			print "Launching worker for url: %s" % curr_url
			print_lock.release()
	 	num_threads_allowed.acquire()
	 	parser_thread.start()
	 	curr_index += INDEX_INCREMENT; 
	for t in threads:
	 	t.join()
	 	
def main():
	# args: [-h] [-si STARTINDEX] [-w OUTFILE] [-s] [-f MAX_ATTEMPTS] [-v] 
	# 		endindex
	FORCE_DEF = 5
	parser = argparse.ArgumentParser(description="scrape IGN for scores and " \
		"game data.")
	parser.add_argument("endindex", type=int, 
		help="the index through which to parse. ")
	parser.add_argument("-si", "--startindex", default=0, type=int, 
		help="the index at which to start. ")
	parser.add_argument("-w", "--writetofile", type=str,
		help="the file in which you would like the data to be saved")
	parser.add_argument("-so", "--scoresonly", default=False, action='store_true',
		help="only retrieve data for games that have non-NR scores")
	parser.add_argument("-f", "--force", default=FORCE_DEF, type=int,
		help="attempt to open each URL x number of times before assuming " \
		"that it is invalid")
	parser.add_argument("-v", "--verbosity", default=False, action='store_true', 
		help="print logging data")

	outfile = None
	args = parser.parse_args()
	# The script's work is worthless if neither -w or -v is specified.
	if (args.writetofile is None) and (not args.verbosity):
		sys.stderr.write("At least one of -w or -v must be specified\n")
		return 1
	# Vet the start index.
	if (args.startindex < 0) or (args.startindex % INDEX_INCREMENT != 0) \
		or (args.startindex > args.endindex):
		sys.stderr.write("invalid start index: %d\n" \
		"start index must be non-negative, divisible by %d, and " \
		"greater than the end index" %
		(args.startindex, INDEX_INCREMENT))
		return 1
	# Vet the end index.
	if (args.endindex < 0) or (args.endindex % INDEX_INCREMENT != 0):
		sys.stderr.write("invalid end index: %d\n" \
			"end index must be non-negative and divisible by %d\n" %
			(args.endindex, INDEX_INCREMENT))
		return 1
	# Open the outfile.
	if args.writetofile is not None:
		try:
			outfile = open(args.writetofile, "w")
		except IOError as e:
			sys.stderr.write("I/O error({0}): {1}\n".format(
				e.errno, e.strerror))
			return 1

	launch_workers(outfile, args.startindex, args.endindex, args.scoresonly,
		args.force, args.verbosity)

	if outfile is not None:
		outfile.close()

if __name__ == "__main__":
	main()