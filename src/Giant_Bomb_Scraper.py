# Pulls video game metadata from Giantbomb's developer API.
#
# Written by Akshay Agrawal
# August 22, 2013


import argparse
import json
import sys
import urllib2 # For URL opening

def get_game_name(result):
	try:
		name = result["game"]["name"].encode("utf-8")
		if name.find("\"") != -1:
			name = name.replace("\"", "\"\"")
	except Exception as e:
		name = ""
	return name

def get_deck(result):
	try:
		deck = result["deck"].encode("utf-8")
		if deck.find("\"") != -1:
			deck = deck.replace("\"", "\"\"")
	except Exception as e:
		deck = ""
	return deck

def get_date(result):
	try:
		date = result["publish_date"].encode("utf-8")
		if date.find("\"") != -1:
			date = date.replace("\"", "\"\"")
	except Exception as e:
		date = ""
	return date

def get_reviewer(result):
	try:
		reviewer = result["reviewer"].encode("utf-8")
		if reviewer.find("\"") != -1:
			reviewer = reviewer.replace("\"", "\"\"")
	except Exception as e:
		reviewer = ""
	return reviewer

def get_score(result):
	try:
		score = str(result["score"])
	except Exception as e:
		score = ""
	return score

def get_site_url(result):
	try:
		url = result["site_detail_url"].encode("utf-8")
		if url.find("\"") != -1:
			url = url.replace("\"", "\"\"")
	except Exception as e:
		url = ""
	return url

def get_data_line(result):
	line = "\"{}\",\"{}\",\"{}\",\"{}\",{},\"{}\"\n".format(
			get_game_name(result),
			get_deck(result),
			get_date(result),
			get_reviewer(result),
			get_score(result),
			get_site_url(result))
	return line

def parse_page(outfile, page_contents, verbose):
	# Store the game title, deck, publish_date, reviewer, score, and site url
	# Ideally, I'd go fetch the platform.
	# (Well, ideally, I'd store everything ... )
	json_dict = json.loads(page_contents)
	json_results = json_dict["results"]

	# Copy over the results
	i = 0
	list_len = len(json_results)
	# Second check is a hack! :(
	while i < list_len and json_results[i] is not None:
		line = get_data_line(json_results[i])
		if outfile is not None:
			outfile.write(line)
		if verbose:
			print line
		i += 1
	return 0

def get_page_contents(curr_url, verbose):
	force_max = 5
	run = 0;
	urlfile = None
	page_contents = ""
	if verbose:
		print "Getting page contents for url {}".format(curr_url)
	while run < force_max:
		try:
			urlfile = urllib2.urlopen(curr_url)
		except urllib2.HTTPError as e:
			sys.stderr.write("HTTP error({0}): {1} with url {2}\n".format(
				e.errno, e.strerror, curr_url))
			run += 1
			continue
		except urllib2.URLError as e:
			sys.stderr.write("URLError({0}): {1} with url {2}\n".format(
				e.errno, e.strerror, curr_url))
			run += 1
			continue
		page_contents = urlfile.read();
		if page_contents == "":	
			run += 1
			continue
		break
	if urlfile is not None:
		urlfile.close()
	if (run >= force_max):
		sys.stderr.write("Failed to open url: %s\n" % curr_url)
		return None
	return page_contents

def get_end_index(curr_url, verbose):
	if verbose:
			print "Getting end index from first page"
	page_contents = get_page_contents(curr_url, verbose)
	if page_contents is None:
		if verbose:
			sys.stderr.write("Failed to get end index from: %s\n" % curr_url)
 	 	return -1
 	json_dict = json.loads(page_contents)
	return json_dict["number_of_total_results"]

def open_urls_and_parse(outfile, api_key, start_index, end_index, verbose):
	# Constants
	BASE_URL = "http://www.giantbomb.com/api/reviews/?api_key=" + api_key + \
		"&format=JSON&field_list" + \
		"=deck,game,publish_date,release,reviewer,score,site_detail_url" + \
	 	"&offset="
	OFFSET = 100

	# Write the CSV categories
	# TODO: This shouldn't be hard-coded (should adhere to field list param,
	# if such a param existed
	if outfile != None:
	 	outfile.write("title,deck,date,reviewer,score,url\n")

	# Calculate the end_index if necessary.
	if end_index < 0:
		end_index = get_end_index(BASE_URL + str(0), verbose)
		# If we failed to read the first page, something must be wrong.
		if end_index < 0:
			return -1

	# Send each page to the parser
	curr_index = start_index;
	while curr_index <= end_index:
		# The current url consists of the base plus the offset
		curr_url = BASE_URL + str(curr_index)

		# Retrieve the page contents
		if verbose:
			print "Opening url {} to send to parser".format(curr_url)
		page_contents = get_page_contents(curr_url, verbose)

		# Parse the page contents
		if page_contents is not None:
			if verbose:
				print "Sending page contents to parser"
		parse_page(outfile, page_contents, verbose)
	 	
	 	# Increment the offset
 		curr_index += OFFSET;
 	return 0


def main():
	# args: [-h] [-st STARTINDEX] [-end ENDINDEX] [-w OUTFILE] [-v] api_key
	parser = argparse.ArgumentParser(description="Retrieve review scores and" \
		" game metadata via GiantBomb\'s API")
	parser.add_argument("api_key", type=str, help="Your GiantBomb API key.")
	parser.add_argument("-st", "--startindex", default=0, type=int, 
		help="the index at which to start. ")
	parser.add_argument("-end", "--endindex", default=-1, type=int,
		help="the index at which to end; if negative, then parse all reviews ")
	parser.add_argument("-w", "--writetofile", type=str,
		help="the file in which you would like the data to be saved")
	parser.add_argument("-v", "--verbosity", default=False, action='store_true', 
		help="print logging data")
	# TODO add field list options

	args = parser.parse_args()
	
	# The script's work is worthless if neither -w or -v is specified.
	if (args.writetofile is None) and (not args.verbosity):
		sys.stderr.write("At least one of -w or -v must be specified\n")
		return 1
	
	# Minimal checking of the start / end index
	if (args.startindex < 0) or ((args.endindex > 0) and
		(args.startindex > args.endindex)):
		sys.stderr.write("invalid start index: %d\n" \
		"start index must be non-negative, and " \
		"greater than the end index (if supplied)" % (args.startindex))
		return -1
	
	# Open the outfile.
	outfile = None
	if args.writetofile is not None:
		try:
			outfile = open(args.writetofile, "w")
		except IOError as e:
			sys.stderr.write("I/O error({0}): {1}\n".format(
				e.errno, e.strerror))
			return 1

	# Do the parsing
	if open_urls_and_parse(outfile, args.api_key,
		args.startindex, args.endindex, args.verbosity) != 0:
		print "Failed to parse pages"
		return 1

	return 0

if __name__ == "__main__":
	main()
