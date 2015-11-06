# ppdb-parser
A Python-based tool that parses text files from the PPDB website
(http://www.cis.upenn.edu/~ccb/ppdb/), and writes the information to a
file that Moses (an open-source statistical machine translation tool)
can read. Note that this tool expects text files to have lines of the
form "LHS ||| SOURCE ||| TARGET ||| (FEATURE=VALUE )* ||| ALIGNMENT".

Furthermore, to parallelize processing, the tool also expects the
input text file to be sorted by TARGET.
