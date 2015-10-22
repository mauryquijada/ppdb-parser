import argparse
import math
import re
import sys
import time

from multiprocessing import Pool

PPDB_DELIMITER = " ||| "
POS_MAPPING_REGEX = r'\[[^\]]+\]\s*'
LOG_BASE = 2

def get_file_chunks(file_name, size=1024 ** 2):
	with open(file_name) as f:
		f.seek(0, 1)
		while True:
			start = f.tell()
			f.seek(size, 1)
			# Make sure that we have the entirety of the last line.
			s = f.readline()

			yield file_name, start, f.tell() - start
			if not s:
				break

def process_file_chunk(input):
	file_name, begin_byte, end_byte = input

	with open(file_name) as f:
		f.seek(begin_byte)
		paraphrase_probabilities = {}
		for line in f.read(end_byte).splitlines():
			paraphrase = process_ppdb_line(line)
			source_paraphrase = paraphrase["source"]
			target_paraphrase = paraphrase["target"]

			if target_paraphrase not in paraphrase_probabilities:
				paraphrase_probabilities[target_paraphrase] = {}

			probability = float(paraphrase["features"]["p(f|e)"])
			paraphrase_probabilities[target_paraphrase][source_paraphrase] =\
			    LOG_BASE ** (-1 * probability)

		return paraphrase_probabilities

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

"""
Given a list of dictionaries of the form {target_paraphrase:
{possible_source_paraphrase: 0.xxx}, ...}, merge each dictionary together into one
large dictionary.
"""
def merge_paraphrase_probabilities(list_paraphrase_probs):
	all_paraphrases_probs = {}
	for paraphrase_probs in list_paraphrase_probs:
		for target_paraphrase, source_paraphrases in paraphrase_probs.iteritems():
			if target_paraphrase in all_paraphrases_probs:
				all_paraphrases_probs[target_paraphrase].update(source_paraphrases)
			else:
				all_paraphrases_probs[target_paraphrase] = source_paraphrases
	return all_paraphrases_probs

"""
Given a dictionary of {possible_source_paraphrase: 0.xxx, }, normalize the probabilities.
"""
def normalize_phrase_probabilities(target_dict_entry):
	target, possible_source_paraphrase = target_dict_entry
	probability_sum = 0

	for _, source_probability in possible_source_paraphrase.iteritems():
		probability_sum += source_probability

	for source, _ in possible_source_paraphrase.iteritems():
		possible_source_paraphrase[source] /= probability_sum * 1.0

	return {target: possible_source_paraphrase}

def main(ppdb_filename):
	process_pool = Pool()
	results = process_pool.map_async(process_file_chunk, get_file_chunks(ppdb_filename))
	process_pool.close()

	print "Extracting paraphrases into a table in memory..."
	while not results.ready():
		remaining = results._number_left
		sys.stdout.write("\r")
		print "Waiting for", remaining, "paraphrase extraction tasks to complete..."
		time.sleep(5)

	all_paraphrases = merge_paraphrase_probabilities(results.get())
	print "Extracted paraphrases into a table in memory."

	process_pool = Pool()
	results = process_pool.map_async(normalize_phrase_probabilities, all_paraphrases.items())
	process_pool.close()

	print "Normalizing paraphrase probabilities..."
	while not results.ready():
		remaining = results._number_left
		sys.stdout.write("\r")
		print "Waiting for", remaining, "paraphrase normalization tasks to complete..."
		time.sleep(5)

	all_paraphrases = merge_paraphrase_probabilities(results.get())
	print "Normalized paraphrase probabilities."

	# Write to file.
	print "Writing paraphrases to file..."
	with open("output.txt", "w") as new_phrasetable_file:
		for target, sources in all_paraphrases.iteritems():
			for source, source_probability in sources.iteritems():
				new_phrasetable_file.write(source + PPDB_DELIMITER +\
					target + PPDB_DELIMITER +\
					str(source_probability) + PPDB_DELIMITER +\
				"|||\n")

if __name__ == "__main__":
	ppdb_filename = None
	parser = argparse.ArgumentParser(description="Parses a given PPDB file into a Moses phrase table.")
	parser.add_argument("ppdb_filename")

	args = parser.parse_args()
	main(args.ppdb_filename)