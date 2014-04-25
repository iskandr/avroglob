
from os import listdir
from os.path import isdir, join, expanduser
import cgi
from glob import glob 
import argparse

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


parser = argparse.ArgumentParser(description='Flatten directory of text files into an AVRO file')
parser.add_argument('input_path', type = str, help='Path to source files')

parser.add_argument('--output', type = str, default = 'output.avro', help='AVRO file')


SCHEMA_TEXT = \
"""
{
 	"type": "record",
 	"name": "TextFile",
 	"fields": [
     	{"name": "filename", "type": "string"},
     	{"name": "contents",  "type": "string"}
 	]
}
 """

SCHEMA = avro.schema.parse(SCHEMA_TEXT) 


def collect_filenames(path):
	wildcard_paths = path.split(',')
	filenames = []
	for wildcard_path in wildcard_paths:
		wildcard_path = expanduser(wildcard_path)
		if isdir(wildcard_path):
			curr_filenames = [join(wildcard_path, filename) for filename in listdir(wildcard_path)]
		else:
			curr_filenames = glob(wildcard_path)
		# drop hidden files 
		curr_filenames = [filename for filename in curr_filenames if not filename.startswith('.')]
		filenames.extend(curr_filenames)
	return filenames 

if __name__ == '__main__':
	args = parser.parse_args()
	with open(args.output, 'w') as output_file:
		writer = DataFileWriter(output_file, DatumWriter(), SCHEMA)
		for i, filename in enumerate(collect_filenames(args.input_path)):
			print "Processing #%d %s" % (i, filename)
			with open(filename, 'r') as input_filename:
				raw = input_filename.read()
				utf8 = raw.decode('utf8')
				ascii = utf8.encode('ascii', 'xmlcharrefreplace')
				escaped = cgi.escape(ascii)
				
			writer.append({"filename":filename, "contents": escaped})
		writer.close()
