import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import gzip
import sys

def isGzipped(inputFile):
	if len(inputFile)<3:
		return False
	if (inputFile[len(inputFile) -3] == '.' and inputFile[len(inputFile)-2] =='g' and inputFile[len(inputFile)-1] =='z'):
		return True
	return False;

def loadTSV(inputFile):
	if isGzipped(inputFile):
		return pd.read_csv(gzip.open(inputFile), sep="\t")
	else:
		return pd.read_csv(inputFile, sep="\t")

def buildParquet(inputFiles, outputFile):
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
	
	df1 = loadTSV(inputFiles[0])
	
	if len(inputFiles)==1:
		df1.to_parquet(outputFile)
		return
	for i in range(0,len(inputFiles)-1):
		df2=loadTSV(inputFiles[i+1])
		df1 = pd.merge(df1,df2,how='inner', on='sampleID')
	df1.to_parquet(outputFile)


