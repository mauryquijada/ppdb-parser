import argparse
import collections
import itertools
import math
import re
import sys
import time

from multiprocessing import Pool

INDIRECT_PROBABILITY_FEATURE_NAME = "p(f|e)"
PPDB_COLUMN_DELIMITER = " ||| "
PHRASE_TABLE_COLUMN_DELIMITER = " ||| " 
POS_MAPPING_REGEX = r'\[[^\]]+\]\s*'
LOG_BASE = math.e

class Paraphrase:
	def __init__(self):
		self.lhs = None
		self.source = None
		self.target = None
		self.features = None
		self.alignment = None

def get_file_chunks(file_name, size=1024 ** 2):
	with open(file_name) as f:
		f.seek(0, 1)
		while True:
			start_byte = f.tell()
			f.seek(size, 1)
			# Make sure that we have the entirety of the last line.
			s = f.readline()
			bytes_to_read = f.tell() - start_byte
			end_of_last_line = f.tell()

			f.seek(start_byte)
			lines = f.read(bytes_to_read).splitlines()

			last_line_target_word = get_ppdb_line_target(lines[-1].split(PPDB_COLUMN_DELIMITER))

			# This is where the sorting comes in. Since we want to have all source
			# phrases for a given target word in the same chunk (so that normalization
			# happens properly) we need to keep reading lines until we find a new
			# target word.
			next_line = f.readline()
			if not next_line:
				yield lines
				break

			next_line_target_word = get_ppdb_line_target(next_line.split(PPDB_COLUMN_DELIMITER))
			while last_line_target_word == next_line_target_word:
				lines.append(next_line)
				end_of_last_line = f.tell()
				next_line = f.readline()
				next_line_target_word = get_ppdb_line_target(next_line.split(PPDB_COLUMN_DELIMITER))

			f.seek(end_of_last_line, 0)

			yield lines
			if not s:
				break

def process_file_chunk(lines):
	paraphrase_probabilities = collections.defaultdict(dict)

	for line in lines:
		paraphrase = process_ppdb_line(line)
		source_paraphrase = paraphrase.source
		target_paraphrase = paraphrase.target

		paraphrase_probabilities[target_paraphrase][source_paraphrase] = paraphrase

	for target_phrase, source_phrases in paraphrase_probabilities.iteritems():
		paraphrase_probabilities[target_phrase] =\
	 		normalize_phrase_probabilities(source_phrases)

	return paraphrase_probabilities

"""
Given a line in a PPDB file, process it appropriately and return a dictionary
of all the features of interest.
"""
def process_ppdb_line(line):
	line_elements = line.split(PPDB_COLUMN_DELIMITER)
	paraphrase = Paraphrase()

	paraphrase.lhs = get_ppdb_line_lhs(line_elements)
	paraphrase.source = get_ppdb_line_source(line_elements)
	paraphrase.target = get_ppdb_line_target(line_elements)
	paraphrase.features = get_ppdb_line_features(line_elements)
	paraphrase.alignment = get_ppdb_line_alignment(line_elements)

	return paraphrase

"""
Given a PPDB line properly split, return the LHS.
"""
def get_ppdb_line_lhs(split_line):
	return split_line[0]

"""
Given a PPDB line properly split, return the source.
"""
def get_ppdb_line_source(split_line):
	s = re.sub(POS_MAPPING_REGEX, r'', split_line[1]).strip()
	return escape_moses_characters(s)

"""
Given a PPDB line properly split, return the target.
"""
def get_ppdb_line_target(split_line):
	s = re.sub(POS_MAPPING_REGEX, r'', split_line[2]).strip()
	return escape_moses_characters(s)

"""
Given a PPDB line properly split, return the features.
"""
def get_ppdb_line_features(split_line):
	features = split_line[3].split(' ')
	feature_dict = {}
	for feature in features:
		key, value = feature.split('=')
		if key == INDIRECT_PROBABILITY_FEATURE_NAME:
			feature_dict[key] = LOG_BASE ** (-1 * float(value))

	return feature_dict

"""
Given a PPDB line properly split, return the alignment.
"""
def get_ppdb_line_alignment(split_line):
	return split_line[4].rstrip('\n')

"""
Given a PPDB source or target phrase, escapes characters that Moses considers
special.
"""
def escape_moses_characters(s):
	s = re.sub(r'^ | $|[\000-\037]|\s+', r'', s)
	s = re.sub(r'&', r'&amp;', s) # Escape escape.
	s = re.sub(r'\|', r'&#124;', s) # factor separator
	s = re.sub(r'<', r'&lt;', s) # xml
	s = re.sub(r'>', r'&gt;', s) # xml
	s = re.sub(r'\'', r'&apos;', s) # xml
	s = re.sub(r'"', r'&quot;', s) # xml
	s = re.sub(r'\[', r'&#91;', s) # Syntax non-terminal
	s = re.sub(r'\]', r'&#93;', s) # Syntax non-terminal

	return s

"""
Given a dictionary of {source_phrase_1: 0.xxx, source_phrase_2: 0.xxx, ...},
normalize the probabilities.
"""
def normalize_phrase_probabilities(source_phrases):
	probability_sum = 0

	features_to_normalize = [INDIRECT_PROBABILITY_FEATURE_NAME]

	for feature in features_to_normalize:
		for _, probability in source_phrases.iteritems():
			probability_sum += probability.features[feature]

		for source_phrase, _ in source_phrases.iteritems():
			source_phrases[source_phrase].features[feature] /= probability_sum * 1.0

	return source_phrases

def create_phrase_table_line(paraphrase):
	return paraphrase.source + PHRASE_TABLE_COLUMN_DELIMITER +\
			paraphrase.target + PHRASE_TABLE_COLUMN_DELIMITER +\
			str(paraphrase.features[INDIRECT_PROBABILITY_FEATURE_NAME]) + PHRASE_TABLE_COLUMN_DELIMITER +\
			paraphrase.alignment + " " + PHRASE_TABLE_COLUMN_DELIMITER.strip() + "\n"

def main(ppdb_filename, output_filename):
	process_pool = Pool()

	# Create a new iterator wrapper that passes along the producer queue with lines
	# to process_file_chunk.
	results = process_pool.imap(process_file_chunk,
		get_file_chunks(ppdb_filename), chunksize=1)
	process_pool.close()

	with open(output_filename, "w") as output:
		for result in results:
			for target_phrase, source_phrases in result.iteritems():
				for source_phrase, paraphrase in source_phrases.iteritems():
						output.write(create_phrase_table_line(paraphrase))

if __name__ == "__main__":
	ppdb_filename = None
	parser = argparse.ArgumentParser(description="Parses a given PPDB file into a Moses phrase table.")
	parser.add_argument("ppdb_filename", help="Filepath to PPDB database. Note that this tool expects" +
		" the file to be sorted by TARGET field.")
	parser.add_argument("output_filename", help="Output phrase table filename.")

	args = parser.parse_args()
	main(args.ppdb_filename, args.output_filename)
