import UseParquet #import *
from FileTypeEnum import FileTypeEnum
from OperatorEnum import OperatorEnum
import argparse

def determineFileType(fileType):
	if fileType== "CSV":
		return FileTypeEnum.CSV
	elif fileType== "JSON":
		return FileTypeEnum.JSON
	elif fileType== "Excel":
		return FileTypeEnum.Excel
	elif fileType== "HDF5":
		return FileTypeEnum.HDF5
	elif fileType=="Feather":
		return FileTypeEnum.Feather
	elif fileType =="Parquet":
		return FileTypeEnum.Parquet
	elif fileType =="MsgPack":
		return FileTypeEnum.MsgPack
	elif fileType == "Stata":
		return FileTypeEnum.Stata
	elif fileType =="Pickle":
		return FileTypeEnum.Pickle
	elif fileType =="HTML":
		return FileTypeEnum.Pickle


parser = argparse.ArgumentParser(description = "Import, query on, and transform data into a format of your choice!")

parser.add_argument("input_file", help = "Data file to be read, queried on, and/or transformed")
parser.add_argument("output_file", help = "File path to which results are exported")
parser.add_argument("output_file_type", help = "Type of file to which results are exported", choices = ["CSV","JSON","Excel","HDF5","Feather","Parquet","MsgPack","Stata","Pickle","HTML"])

parser.add_argument("-t","--transpose", help="Transpose index and columns", action= "store_true")
parser.add_argument("-c","--columns", nargs='+', help ="List of column names to examine in the given dataset") 
parser.add_argument("--discrete_queries", nargs = '*', help = "List of discrete queries in the form of A B where A is the column name and B is the corresponding value")
args = parser.parse_args()
fileType= determineFileType(args.output_file_type)

isTransposed = False
if args.transpose:
	isTransposed=True
colList=[]
discreteQueryList=[]
continuousQueryList=[]
if args.columns:
	colList=args.columns

UseParquet.exportQueryResults(args.input_file, args.output_file, fileType,columnList=colList,transpose=isTransposed)
