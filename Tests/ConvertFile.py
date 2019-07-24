import argparse
import sys
import pandas as pd
import pyarrow
import warnings
from expressionable import ExpressionAble
from expressionable.errors import ColumnNotFoundError
from expressionable.utils import ContinuousQuery
from expressionable.utils import DiscreteQuery
from expressionable.utils import OperatorEnum

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
            print(query + ": query is malformed")

warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
parser = argparse.ArgumentParser(description = "Import, filter, and transform data into a format of your choice!")

parser.add_argument("input_file", help = "Data file to be imported, filtered, and/or transformed")
parser.add_argument("output_file", help = "File path to which results are exported")

supportedFiles=["CSV", "TSV", "JSON", "Excel", "HDF5", "Parquet", "MsgPack", "Stata", "Pickle", "HTML", "SQLite","ARFF", "GCT", "Kallisto"]

parser.add_argument("-i","--input_file_type", help = "Type of file to be imported. If not specified, file type will be determined by the file extension given. Available choices are: "+", ".join(supportedFiles),  metavar= 'File_Type') #choices = supportedFiles,
parser.add_argument("-o","--output_file_type", help = "Type of file to which results are exported. If not specified, file type will be determined by the file extension given. Available choices are: "+", ".join(supportedFiles), choices = supportedFiles, metavar='File_Type')
parser.add_argument("-t","--transpose", help="Transpose index and columns in the output file", action= "store_true")
parser.add_argument("-f", "--filter", help = "Filter data using python logical syntax. Your filter must be surrounded by quotes.\n\nFor example: -f \"ColumnName1 > 12.5 and (ColumnName2 == 'x' or ColumnName2 =='y')\"", metavar="\"FILTER\"", action='append')
parser.add_argument("-c","--columns", action='append', help ="List of additional column names to include in the output file. Column names must be seperated by commas and without spaces. For example: -c ColumnName1,ColumnName2,ColumnName3")
parser.add_argument("-a", "--all_columns", help = "Includes all columns in the output file. Overrides the \"--columns\" flag", action="store_true")
parser.add_argument("-g","--gzip", help = "Gzips the output file", action="store_true")
parser.add_argument("-s","--set_index", help="Sets the given column to become the index column, where appropriate. If not set, the default index will be 'Sample'",nargs=1)

args = parser.parse_args()

inFileType = None
if args.input_file_type:
    inFileType = args.input_file_type.lower()
isInFileGzipped = isGzipped(args.input_file)

outFileType = None
if args.output_file_type:
    outFileType = args.output_file_type.lower()

isTransposed = False
if args.transpose:
    isTransposed=True
    if outFileType == 'feather' or outFileType == 'parquet' or outFileType == 'stata':
        print("Error: Parquet and Stata file types do not support transposing. Either choose a different output file type or remove the --transpose flag")
        sys.exit(1)

colList=[]
query=None
if args.filter and len(args.filter)>1:
    parser.error("--filter appears multiple times")

if args.columns and len(args.columns)>1:
    parser.error("--columns appears multiple times")

if args.filter:
    query=args.filter[0]
    if not("==" in query or "!=" in query or "<" in query or ">" in query or "<=" in query or ">=" in query):
        print("Error: Filter must be an expression involving an operator such as '==' or '<'. If you simply want to include specific columns in the output, try using the --columns flag")
        sys.exit(1)

if args.columns:
    colList=parseColumns(args.columns[0])

allCols=False
if args.all_columns:
    allCols=True

gzip=False
if (not isGzipped(args.output_file) and args.gzip):
    print("NOTE: Because you requested the output be gzipped, it will be saved to " + args.output_file + ".gz")
if isGzipped(args.output_file) or args.gzip:
    gzip=True

indexCol = "Sample"
if args.set_index:
    indexCol = args.set_index[0]

ss = ExpressionAble(args.input_file, inFileType)
ss.export_filter_results(args.output_file, outFileType, filters=query, columns=colList, transpose=isTransposed,
                         include_all_columns=allCols, gzip_results=gzip, index=indexCol)
