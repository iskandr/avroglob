from os.path import isdir, join
from glob import glob 
import argparse

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter



SCHEMA = \
 """
 {
 	"type": "record",
 	"name": "TextFile",
 	"fields": [
     	{"name": "filename", "type": "string"},
     	{"name": "contents",  "type": "string"},
 	]
}
 """


parser = argparse.ArgumentParser(description='Flatten directory of text files into an AVRO file')
parser.add_argument('input_path', type = str, required = True, help='Path to source files')

parser.add_argument('--output', type = str, default = 'output.avro', help='AVRO file')

def collect_filenames(path):
	wildcard_paths = path.split(',')
	filenames = []
	for wildcard_path in wildcard_paths:
		if isdir(wildcard_path):
			wildcard_path = join(wildcard_path, "*")
		filenames.extend(glob(wildcard_path))
	return filenames 

if __name __ == '__main__':
	args = parser.parse_args()
	with open(args.output, 'w') as output_file:
		writer = DataFileWriter(output_file, DatumWriter(), SCHEMA)
		for i, filename in enumerate(collect_filenames(args.input_path)):
			print "Processing #%d %s" % (i, filename)
			with open(filename, 'r') as input_filename:
				contents = input_filename.read()
			writer.append({"filename":filename, "contents": contents})
		writer.close()
