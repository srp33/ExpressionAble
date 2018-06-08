from UseParquet import exportQueryResults #import *
from UseParquet import exportFilterResults
from FileTypeEnum import FileTypeEnum
from OperatorEnum import OperatorEnum
from ContinuousQuery import ContinuousQuery
from DiscreteQuery import DiscreteQuery
from ColumnNotFoundError import ColumnNotFoundError
import pandas as pd
import argparse
import pyarrow
import sys

def determineFileType(fileType):
	if fileType== "CSV":
		return FileTypeEnum.CSV
	elif fileType=="TSV":
		return FileTypeEnum.TSV
	elif fileType == "JSON":
		return FileTypeEnum.JSON
	elif fileType== "Excel":
		return FileTypeEnum.Excel
	elif fileType== "HDF5":
		return FileTypeEnum.HDF5
	#elif fileType=="Feather":
	#	return FileTypeEnum.Feather
	elif fileType =="Parquet":
		return FileTypeEnum.Parquet
	elif fileType =="MsgPack":
		return FileTypeEnum.MsgPack
	elif fileType == "Stata":
		return FileTypeEnum.Stata
	elif fileType =="Pickle":
		return FileTypeEnum.Pickle
	elif fileType =="HTML":
		return FileTypeEnum.HTML
	elif fileType =="SQL":
		return FileTypeEnum.SQL
	elif fileType == "ARFF":
		return FileTypeEnum.ARFF

def determineExtension(fileName):
	extensions= fileName.rstrip("\n").split(".")
	if len(extensions)>1:
		extension=extensions[len(extensions)-1]
		if extension =='gz':
			extension=extensions[len(extensions)-2]
	if extension== "tsv" or extension =="txt":
                return FileTypeEnum.TSV
	elif extension == "csv":
		return FileTypeEnum.CSV
	elif extension== "json":
		return FileTypeEnum.JSON
	elif extension== "xlsx":
		return FileTypeEnum.Excel
	elif extension== "hdf" or extension=="h5":
		return FileTypeEnum.HDF5
	#elif extension=="feather":
	#	return FileTypeEnum.Feather
	elif extension =="pq":
		return FileTypeEnum.Parquet
	elif extension =="mp":
		return FileTypeEnum.MsgPack
	elif extension == "dta":
		return FileTypeEnum.Stata
	elif extension =="pkl":
		return FileTypeEnum.Pickle
	elif extension =="html":
		return FileTypeEnum.HTML
	elif extension=="db":
		return FileTypeEnum.SQL
	elif extension == "arff":
		return FileTypeEnum.ARFF
	else:
		print("Error: Extension on " + fileName+ " not recognized. Please use appropriate file extensions or explicitly specify file type using the -i or -o flags")
		sys.exit()

def isGzipped(fileName):
	extensions=fileName.rstrip("\n").split(".")
	if extensions[len(extensions)-1]=='gz':
		return True
	return False

def determineOperator(operator):
	if operator=="==" or operator=="=":
		return OperatorEnum.Equals
	elif operator== "<":
		return OperatorEnum.LessThan
	elif operator =="<=":
		return OperatorEnum.LessThanOrEqualTo
	elif operator ==">":
		return OperatorEnum.GreaterThan
	elif operator ==">=":
		return OperatorEnum.GreaterThanOrEqualTo
	elif operator == "!=":
		return OperatorEnum.NotEquals

def isOperator(operator):
	if operator=="==" or operator=="=" or operator=="<" or operator ==">" or operator==">=" or operator=="<=" or operator=="!=":
		return True
	print("\'"+operator+"\' is not a valid operator")
	return False

def isContinuous(query):
	query = query.rstrip("\n").split(" ")
	if len(query)<3:
		print("invalid query: " + str(query) + ": wrong number of arguments")
		return False
	try:
		float(query[2])
		if isOperator(query[1]):
			return True
	except:
		return False
	return False 

def isDiscrete(query):
	query = query.rstrip("\n").split(" ")
	if len(query)<3:
		print("invalid query: " + str(query) + ": wrong number of arguments")
		return False
	try:
		float(query[2])
		return False
	except:
		if query[1]=='==' or query[1]=='=':
			return True
	return False 
def buildContinuousQuery(query):
	query = query.rstrip("\n").split(" ")
	col = query[0]
	operator = determineOperator(query[1])
	value = float(query[2])
	return ContinuousQuery(col, operator, value)
	
def buildDiscreteQuery(query):
	query=query.rstrip("\n").split(" ")
	col = query[0]
	values=[]
	for i in range(2, len(query)):
		if query[i] == "True":
			values.append(True)
		elif query[i] == "False":
			values.append(False)
		else:
			values.append(query[i])
	return DiscreteQuery(col, values)

def parseColumns(columns):
	colList = columns.rstrip("\n").split(",")
	return colList

def buildAllQueries(queryList, discreteQueryList, continuousQueryList):
	
	for query in queryList:
		if isDiscrete(query):
			discreteQueryList.append(buildDiscreteQuery(query))
		elif isContinuous(query):
			continuousQueryList.append(buildContinuousQuery(query))
		else:
			print(query+": query is malformed")	

parser = argparse.ArgumentParser(description = "Import, filter, and transform data into a format of your choice!")

parser.add_argument("input_file", help = "Data file to be imported, filtered, and/or transformed")
parser.add_argument("output_file", help = "File path to which results are exported")

supportedFiles=["CSV", "TSV", "JSON","Excel","HDF5","Parquet","MsgPack","Stata","Pickle","HTML", "SQL"]

parser.add_argument("-i","--input_file_type", help = "Type of file to be imported. If not specified, file type will be determined by the file extension given. Available choices are: "+", ".join(supportedFiles), choices = supportedFiles, metavar= 'File_Type')
parser.add_argument("-o","--output_file_type", help = "Type of file to which results are exported. If not specified, file type will be determined by the file extension given. Available choices are: "+", ".join(supportedFiles), choices = supportedFiles, metavar='File_Type')
parser.add_argument("-t","--transpose", help="Transpose index and columns in the output file", action= "store_true")
parser.add_argument("-f", "--filter", help = "Filter data using python logical syntax. Your filter must be surrounded by quotes.\n\nFor example: -f \"ColumnName1 > 12.5 and (ColumnName2 == 'x' or ColumnName2 =='y')\"", metavar="\"FILTER\"", action='append')
parser.add_argument("-c","--columns", action='append', help ="List of additional column names to include in the output file. Column names must be seperated by commas and without spaces. For example: -c ColumnName1,ColumnName2,ColumnName3") 
parser.add_argument("-a", "--all_columns", help = "Includes all columns in the output file. Overrides the \"--columns\" flag", action="store_true")
parser.add_argument("-g","--gzip", help = "Gzips the output file", action="store_true")
args = parser.parse_args()

inFileType = determineExtension(args.input_file)
if args.input_file_type:
	inFileType = determineFileType(args.input_file_type)
isInFileGzipped = isGzipped(args.input_file)	

outFileType= determineExtension(args.output_file)
if args.output_file_type:
	outFileType = determineFileType(args.output_file_type)
isTransposed = False
if args.transpose:
	isTransposed=True
	if outFileType == FileTypeEnum.Feather or outFileType == FileTypeEnum.Parquet or outFileType == FileTypeEnum.Stata:
		print("Error: Parquet and Stata file types do not support transposing. Either choose a different output file type or remove the --transpose flag")
		sys.exit()
colList=[]
query=None
discreteQueryList=[]
continuousQueryList=[]
if args.filter and len(args.filter)>1:
	parser.error("--filter appears multiple times")
if args.columns and len(args.columns)>1:
	parser.error("--columns appears multiple times")

if args.filter:
	#buildAllQueries(args.filter, discreteQueryList, continuousQueryList)
	query=args.filter[0]
	if not("==" in query or "!=" in query or "<" in query or ">" in query or "<=" in query or ">=" in query):
		print("Error: Filter must be an expression involving an operator such as '==' or '<'. If you simply want to include certain columns in the output, try using the --columns flag")
		sys.exit()
if args.columns:
	colList=parseColumns(args.columns[0])
	#todo: find a way to remove duplicates in the list without reordering the list
	#colSet=set(args.columns)
	#colList=list(colSet)
allCols=False
if args.all_columns:
	allCols=True

gzip=False
if (not isGzipped(args.output_file) and args.gzip):
	print("NOTE: Because you requested the output be gzipped, it will be saved to "+args.output_file+".gz")
if isGzipped(args.output_file) or args.gzip:
	gzip=True
#exportQueryResults(args.input_file, args.output_file, outFileType, columnList=colList, continuousQueries=continuousQueryList, discreteQueries=discreteQueryList, transpose=isTransposed, includeAllColumns=allCols)
try:
	exportFilterResults(args.input_file, args.output_file, outFileType, gzippedInput=isInFileGzipped, query=query, columnList=colList,transpose=isTransposed, includeAllColumns=allCols, gzipResults=gzip)
except pyarrow.lib.ArrowIOError as e:
	print("Error: " + str(e))
except pd.core.computation.ops.UndefinedVariableError as e:
	print("Error: Variable not found: " + str(e))
	print("Hint: If the variable not found is a column name, make sure it is spelled correctly. If the variable is a value, make sure it is surrounded by quotes")
except SyntaxError as error:
	try:
		print("Error: \'" +error.text.rstrip() +"\'\n" +" "*(error.offset+6)+"^")
	except AttributeError:
		pass
	finally:
		print("Syntax is invalid. Valid python syntax must be used")
except ValueError as e:
	print("Error: "+str(e))
#except TypeError as e:
#	print("Error: Type error. Maybe you left the filter blank?")
except KeyError as e:
	print("Error: " + str(e))
except NotImplementedError as e:
	print("Error: " + str(e))
except RecursionError as e:
	print("Error: " + str(e))
except ColumnNotFoundError as e:
	print("Warning: the following columns requested were not found and therefore not included in the output: " +str(e))
