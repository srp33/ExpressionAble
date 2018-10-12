import gzip

import pandas as pd


def is_gzipped(inputFile):
	if len(inputFile)<3:
		return False
	if (inputFile[len(inputFile) -3] == '.' and inputFile[len(inputFile)-2] =='g' and inputFile[len(inputFile)-1] =='z'):
		return True
	return False;

def load_tsv(inputFile):
	if is_gzipped(inputFile):
		return pd.read_csv(gzip.open(inputFile), sep="\t")
	else:
		return pd.read_csv(inputFile, sep="\t")

def build_parquet(inputFiles, outputFile):
	"""
	Builds a parquet file by merging any number of TSV or gzipped TSV files

	:type inputFiles: list of strings
	:param inputFiles: list of filenames that represent files to be merged together into a parquet file. If a file is gzipped, it must be indicated by the extension '.gz'

	:type outputFile: string
	:param outputFile: name of a parquet file that will contain the product of the merged files
	"""
	if len(inputFiles)<1:
		print("Error: there must be at least one input file to build a parquet file")
		return
	
	df1 = load_tsv(inputFiles[0])
	
	if len(inputFiles)==1:
		df1.to_parquet(outputFile)
		return
	for i in range(0,len(inputFiles)-1):
		df2=load_tsv(inputFiles[i + 1])
		df1 = pd.merge(df1,df2,how='inner')
	df1.to_parquet(outputFile)

if __name__ == '__main__':
	build_parquet(["TsvData/METABRIC/data.tsv", "TsvData/METABRIC/converted_metadata.txt"], "metabric.pq")
