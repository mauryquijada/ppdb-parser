import argparse
import math
import re
import sys

PPDB_DELIMITER = " ||| "
POS_MAPPING_REGEX = r'\[[^\]]+\]\s*'
LOG_BASE = 10

"""
Given a line in a PPDB file, process it appropriately and return a dictionary
of all the features of interest.
"""
def process_ppdb_line(line):
	line_elements = line.split(PPDB_DELIMITER)
	paraphrase = {}

	paraphrase["lhs"] = line_elements[0]
	paraphrase["source"] = re.sub(POS_MAPPING_REGEX, r'', line_elements[1]).strip()
	paraphrase["target"] = re.sub(POS_MAPPING_REGEX, r'', line_elements[2]).strip()

	features = line_elements[3].split(' ')
	feature_dict = {}
	for feature in features:
		key, value = feature.split('=')
		feature_dict[key] = value
	paraphrase["features"] = feature_dict

	paraphrase["alignment"] = line_elements[4]

	return paraphrase

def main(ppdb_filename):
	with open(ppdb_filename) as opened_ppdb_file:
		paraphrase_probability = {}
		count = 0
		total_count = 6977679
		for line in opened_ppdb_file:
			count += 1
			line_elements = line.split(PPDB_DELIMITER)
			paraphrase = process_ppdb_line(line)

			if paraphrase["target"] not in paraphrase_probability:
				paraphrase_probability[paraphrase["target"]] = {}

			prob = float(paraphrase["features"]["p(f|e)"])
			paraphrase_probability[paraphrase["target"]][paraphrase["source"]] =\
			    LOG_BASE ** (-1 * prob)

			# sys.stdout.write("\rNow %f %% done creating the probability table." % (count * 1.0 / total_count) * 100)
			# sys.stdout.flush()

			# count = 0
			# for target, sources in paraphrase_probability.iteritems():
			# 	sum = 0
			# 	count += 1
			# 	sys.stdout.write("\rNow %f %% done normalizing probabilities." % (count * 1.0 / total_count) * 100)
			# 	sys.stdout.flush()

			# 	for _, source_probability in sources.iteritems():
			# 		sum += source_probability

			# 	for source, _ in sources.iteritems():
			# 		paraphrase_probability[target][source] /= sum * 1.0

			# 	prob = float(paraphrase["features"]["p(f|e)"])
			# 	paraphrase_probability[paraphrase["target"]][paraphrase["source"]] =\
			# 	    10 ** (-1 * prob)

			# count = 0
			# for target, sources in paraphrase_probability.iteritems():
			# 	count += 1
			# 	sys.stdout.write("\rNow %f %% done writing to table." % (count * 1.0 / total_count) * 100)
			# 	sys.stdout.flush()

			# 	for source, source_probability in sources.iteritems():
			# 		new_phrasetable_file.write(source + PPDB_DELIMITER +\
			# 			target + PPDB_DELIMITER +\
			# 			str(source_probability) + PPDB_DELIMITER +\
			# 		"|||\n")



if __name__ == "__main__":
	ppdb_filename = None
	parser = argparse.ArgumentParser(description="Parses a given PPDB file into a Moses phrase table.")
	parser.add_argument("ppdb_filename")

	args = parser.parse_args()
	main(args.ppdb_filename)