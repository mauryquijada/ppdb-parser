import argparse
import csv
import math

PPDB_DELIMITER = " ||| "

def main(ppdb_filename):
	with open('hai.txt', 'w') as new_phrasetable_file:
		with open(ppdb_filename) as opened_ppdb_file:
			paraphrases = []

			for line in opened_ppdb_file:
				line_elements = line.split(PPDB_DELIMITER)
				paraphrase = {}
				paraphrase["lhs"] = line_elements[0]
				paraphrase["source"] = line_elements[1]
				paraphrase["target"] = line_elements[2]

				features = line_elements[3].split(' ')
				feature_dict = {}
				for feature in features:
					key, value = feature.split('=')
					feature_dict[key] = value
				paraphrase["features"] = feature_dict

				paraphrase["alignment"] = line_elements[4]

				# new_phrasetable_file.write(paraphrase["source"] + PPDB_DELIMITER +\
				# 	paraphrase["target"] + PPDB_DELIMITER +\
				# 	str(-1 * math.log(float(paraphrase["features"]["p(f|e)"]))) + PPDB_DELIMITER +\
				# 	"|||")
				print paraphrase["source"] + PPDB_DELIMITER +\
					paraphrase["target"] + PPDB_DELIMITER +\
					str(-1 * math.log(float(paraphrase["features"]["p(f|e)"]))) + PPDB_DELIMITER +\
					"|||"


if __name__ == "__main__":
	ppdb_filename = None
	parser = argparse.ArgumentParser(description="Parses a given PPDB file into a Moses phrase table.")
	parser.add_argument('ppdb_filename')

	args = parser.parse_args()
	main(args.ppdb_filename)