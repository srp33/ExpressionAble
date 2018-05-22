from UseParquet import exportQueryResults #import *
from UseParquet import exportFilterResults
from FileTypeEnum import FileTypeEnum
from OperatorEnum import OperatorEnum
from ContinuousQuery import ContinuousQuery
from DiscreteQuery import DiscreteQuery
import argparse

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
		return FileTypeEnum.HTML

def determineExtension(fileName):
	extension= fileName.rstrip("\n").split(".")
	if len(extension)>1:
		extension=extension[len(extension)-1]
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
	elif extension=="feather":
		return FileTypeEnum.Feather
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

supportedFiles=["CSV", "TSV", "JSON","Excel","HDF5","Feather","Parquet","MsgPack","Stata","Pickle","HTML"]

parser.add_argument("-i","--input_file_type", help = "Type of file to be imported. If not specified, file type will be determined by the file extension given. Available choices are: "+", ".join(supportedFiles), choices = supportedFiles, metavar= 'File_Type')
parser.add_argument("-o","--output_file_type", help = "Type of file to which results are exported. If not specified, file type will be determined by the file extension given. Available choices are: "+", ".join(supportedFiles), choices = supportedFiles, metavar='File_Type')
parser.add_argument("-t","--transpose", help="Transpose index and columns in the output file", action= "store_true")
parser.add_argument("-f", "--filter", help = "Filter data using python logical syntax. Your filter must be surrounded by quotes.\n\nFor example: -f \"ColumnName1 > 12.5 and (ColumnName2 == 'x' or ColumnName2 =='y')\"", metavar="\"FILTER\"")
parser.add_argument("-c","--columns",  help ="List of additional column names to include in the output file. Column names must be seperated by commas and without spaces. For example: -c ColumnName1,ColumnName2,ColumnName3") 
parser.add_argument("-a", "--all_columns", help = "Includes all columns in the output file. Overrides the \"--columns\" flag", action="store_true")
args = parser.parse_args()

inFileType = determineExtension(args.input_file)
if args.input_file_type:
	inFileType = determineFileType(args.input_file_type)
	

outFileType= determineExtension(args.output_file)
if args.output_file_type:
	outFileType = determineFileType(args.output_file_type)
isTransposed = False
if args.transpose:
	isTransposed=True
colList=[]
query=None
discreteQueryList=[]
continuousQueryList=[]
if args.filter:
	#buildAllQueries(args.filter, discreteQueryList, continuousQueryList)
	query=args.filter

if args.columns:
	colList=parseColumns(args.columns)
	#todo: find a way to remove duplicates in the list without reordering the list
	#colSet=set(args.columns)
	#colList=list(colSet)
allCols=False
if args.all_columns:
	allCols=True

#exportQueryResults(args.input_file, args.output_file, outFileType, columnList=colList, continuousQueries=continuousQueryList, discreteQueries=discreteQueryList, transpose=isTransposed, includeAllColumns=allCols)
exportFilterResults(args.input_file, args.output_file, outFileType, query=query, columnList=colList,transpose=isTransposed, includeAllColumns=allCols)
