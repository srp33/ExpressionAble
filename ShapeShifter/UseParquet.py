import pyarrow.parquet as pq
import re
#import pyarrow as pa
import pandas as pd
#from sqlalchemy import create_engine
from ColumnInfo import ColumnInfo
from ContinuousQuery import ContinuousQuery
from DiscreteQuery import DiscreteQuery
from OperatorEnum import OperatorEnum
from FileTypeEnum import FileTypeEnum
import time
#import sys

def peek(parquetFilePath, numRows=10, numCols=10)->pd.DataFrame:
	"""
	Takes a look at the first few rows and columns of a parquet file and returns a pandas dataframe corresponding to the number of requested rows and columns
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be examined

	:type numRows: int
	:param numRows: the number of rows the returned Pandas dataframe will contain

	:type numCols: int
	:param numCols: the number of columns the returned Pandas dataframe will contain
	
	:return: The first numRows and numCols in the given parquet file
	:rtype: Pandas dataframe
	"""	
	allCols = getColumnNames(parquetFilePath)
	if(numCols>len(allCols)):
		numCols=len(allCols)
	selectedCols = []
	selectedCols.append("Sample")
	for i in range(0, numCols):
		selectedCols.append(allCols[i])
	df = pd.read_parquet(parquetFilePath, columns=selectedCols)
	df.set_index("Sample", drop=True, inplace=True)
	df=df.iloc[0:numRows, 0:numCols]
	return df

def peekByColumnNames(parquetFilePath, listOfColumnNames,numRows=10)->pd.DataFrame:
	"""
	Peeks into a parquet file by looking at a specific set of columns
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be examined

	:type numRows: int
	:param numRows: the number of rows the returned Pandas dataframe will contain

	:return: The first numRows of all the listed columns in the given parquet file
	:rtype: Pandas dataframe

	"""
	listOfColumnNames.insert(0,"Sample")
	df = pd.read_parquet(parquetFilePath, columns=listOfColumnNames)
	df.set_index("Sample", drop=True, inplace=True)
	df=df[0:numRows]
	return df

def getColumnNames(parquetFilePath)->list:
	"""
	Retrieves all column names from a dataset stored in a parquet file
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be examined

	:return: All column names
	:rtype: list
	
	"""
	
	p = pq.ParquetFile(parquetFilePath)
	columnNames = p.schema.names
	#delete 'Sample' from schema
	del columnNames[0]

	#delete extraneous other schema that the parquet file tacks on at the end
	if '__index_level_' in columnNames[len(columnNames)-1]:
		del columnNames[len(columnNames)-1]
	if 'Unnamed:' in columnNames[len(columnNames)-1]:
		del columnNames[len(columnNames)-1]
	return columnNames

def getColumnInfo(parquetFilePath, columnName:str, sizeLimit:int=None)->ColumnInfo:
	"""
	Retrieves a specified column's name, data type, and all its unique values from a parquet file
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be examined

	:type columnName: string
	:param columnName: the name of the column about which information is being obtained

	:type sizeLimit: int
	:param sizeLimit: limits the number of unique values returned to be no more than this number

	:return: Name, data type (continuous/discrete), and unique values from specified column
	:rtype: ColumnInfo object
	"""
	columnList = [columnName]
	df = pd.read_parquet(parquetFilePath, columns=columnList)

#	uniqueValues = set()
#	for index, row in df.iterrows():
#		try:
#			uniqueValues.add(row[columnName])
#		except (TypeError, KeyError) as e:
#			return None
#		if sizeLimit != None:
#			if len(uniqueValues)>=sizeLimit:
#				break
#	uniqueValues = list(uniqueValues)

	uniqueValues = df[columnName].unique().tolist()
	#uniqueValues.remove(None)
	#Todo: There is probably a better way to do this...
	i=0
	while uniqueValues[i] == None:
		i+=1
	if isinstance(uniqueValues[i],str) or isinstance(uniqueValues[i],bool):
		return ColumnInfo(columnName,"discrete", uniqueValues)
	else:
		return ColumnInfo(columnName, "continuous", uniqueValues)

def getAllColumnsInfo(parquetFilePath):
	"""
	Retrieves the column name, data type, and all unique values from every column in a file

	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be examined

	:return: Name, data type (continuous/discrete), and unique values from every column
	:rtype: dictionary where key: column name and value:ColumnInfo object containing the column name, data type (continuous/discrete), and unique values from all columns
	"""
	#columnNames = getColumnNames(parquetFilePath)
	df=pd.read_parquet(parquetFilePath)
	columnDict={}
	for col in df:
		uniqueValues=df[col].unique().tolist()
		i=0
		while uniqueValues[i] == None:
			i+=1
		if isinstance(uniqueValues[i],str) or isinstance(uniqueValues[i],bool):
	                columnDict[col] = ColumnInfo(col,"discrete", uniqueValues)
		else:
			columnDict[col] = ColumnInfo(col, "continuous", uniqueValues)
	return columnDict

def query(parquetFilePath, columnList: list=[], continuousQueries: list=[], discreteQueries: list=[], includeAllColumns = False)->pd.DataFrame:
	"""
	Performs mulitple queries on a parquet dataset. If no queries or columns are passed, it returns the entire dataset as a pandas dataframe. Otherwise, returns only the queried data over the requested columns as a Pandas dataframe

	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be queried on

	:type columnList: list of strings
	:param columnList: list of column names that will be included in the data resulting from the queries

	:type continuousQueries: list of ContinuousQuery objects
	:param continuousQueries: list of objects representing queries on a column of continuous data
	
	:type discreteQueries: list of DiscreteQuery objects
	:param discreteQueries: list of objects representing queries on a column of discrete data

	:type includeAllColumns: bool
	:param includeAllColumns: if true, will include all columns in results. Overrides columnList if True

	:return: Requested columns with results of all queries 
	:rtype: Pandas dataframe
	"""
	if len(columnList)==0 and len(continuousQueries)==0 and len(discreteQueries)==0:
		df = pd.read_parquet(parquetFilePath)
		#df.set_index("Sample", drop=True, inplace=True)
		return df
	
	if includeAllColumns:
		columnList = getColumnNames(parquetFilePath)
	else:	
		#extract all necessary columns in order to read them into pandas
		for query in continuousQueries:
			if query.columnName not in columnList:
				columnList.append(query.columnName)
		for query in discreteQueries:
			if query.columnName not in columnList:
				columnList.append(query.columnName)
	if "Sample" not in columnList:
		columnList.insert(0,"Sample")
	else:
		columnList.insert(0, columnList.pop(columnList.index("Sample")))
	
	df = pd.read_parquet(parquetFilePath, columns = columnList)
	#df.set_index("Sample", drop=True, inplace=True)
	#del columnList[0]

	#perform continuous queries, adjusting for which operator is to be used
	for query in continuousQueries:
		if query.operator == OperatorEnum.Equals:
			df = df.loc[df[query.columnName]==query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.GreaterThan:
			df = df.loc[df[query.columnName]>query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.GreaterThanOrEqualTo:
			df = df.loc[df[query.columnName]>=query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.LessThan:
			df = df.loc[df[query.columnName]<query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.LessThanOrEqualTo:
			df = df.loc[df[query.columnName]<=query.value, [ col for col in columnList]]
		elif query.operator== OperatorEnum.NotEquals:
			df = df.loc[df[query.columnName]!=query.value, [col for col in columnList]]
	#perform discrete queries
	for query in discreteQueries:
		df = df.loc[df[query.columnName].isin(query.values), [col for col in columnList]]
	
	return df

def parseColumnNamesFromQuery(query):
	args = re.split('==|<|>|!=|<=|>=|and|or|\&|\|', query)
	colList=[]
	for arg in args:
		#first remove all whitespace and parentheses and brackets
		arg=arg.strip()
		arg=arg.replace("(","")
		arg=arg.replace(")","")
		arg=arg.replace("[","")
		arg=arg.replace("]","")
		#if it is a number, it isn't a column name
		try:
			float(arg)
		except:
			#check if the string is surrounded by quotes. If so, it is not a column name
			if arg[0]!="'" and arg[0]!='"':
				#check for duplicates
				if arg not in colList:
					colList.append(arg)
	return colList


def newQuery(parquetFilePath, columnList, query=None, includeAllColumns = False):
	
	if len(columnList)==0 and query == None:
		df = pd.read_parquet(parquetFilePath)
		#df.set_index("Sample", drop=True, inplace=True)
		return df
	if includeAllColumns:
		columnList = getColumnNames(parquetFilePath)
	else:
		queryColumns= parseColumnNamesFromQuery(query)
		columnList = queryColumns+columnList 
	if "Sample" not in columnList:
		columnList.insert(0,"Sample")
	else:
		columnList.insert(0, columnList.pop(columnList.index("Sample")))

	df = pd.read_parquet(parquetFilePath, columns=columnList)
	df=df.query(query)
	return df
	

def exportFilterResults(parquetFilePath, outFilePath, outFileType:FileTypeEnum, columnList: list=[], query=None, transpose= False, includeAllColumns = False):
	"""
	Performs mulitple queries on a parquet dataset and exports results to a file of specified type. If no queries or columns are passed, it exports the entire dataset as a pandas dataframe. Otherwise, exports the queried data over the requested columns 

	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be queried on

	:type outFilePath: string
	:param outFilePath: name of the file that query results will written to

	:type outFileType: FileTypeEnum
	:param outFileType: an enumerated object specifying what sort of file to which results will be exported	

	:type columnList: list of strings
	:param columnList: list of column names that will be included in the data resulting from the queries

	:type continuousQueries: list of ContinuousQuery objects
	:param continuousQueries: list of objects representing queries on a column of continuous data
	
	:type discreteQueries: list of DiscreteQuery objects
	:param discreteQueries: list of objects representing queries on a column of discrete data

	:type transpose: bool
	:param transpose: if True, index and columns will be transposed

	:type includeAllColumns: bool
        :param includeAllColumns: if true, will include all columns in results. Overrides columnList if True 

	"""
	df = newQuery(parquetFilePath, columnList, query, includeAllColumns = includeAllColumns)
	null= 'NA'
	#df.reset_index(inplace=True)
	if transpose:
		df=df.transpose()

	if outFileType== FileTypeEnum.TSV:
		df.to_csv(path_or_buf=outFilePath, sep='\t',na_rep=null, index=False)
	elif outFileType == FileTypeEnum.CSV:
		df.to_csv(path_or_buf=outFilePath, na_rep=null, index=False)
	elif outFileType == FileTypeEnum.JSON:
		df.to_json(path_or_buf=outFilePath, index = False)
	elif outFileType == FileTypeEnum.Excel:
		import xlsxwriter
		writer = pd.ExcelWriter(outFilePath, engine='xlsxwriter')
		df.to_excel(writer, sheet_name='Sheet1', na_rep=null, index=False) 
		writer.save()
	elif outFileType == FileTypeEnum.Feather:
		df=df.reset_index()
		df.to_feather(outFilePath)
	elif outFileType ==FileTypeEnum.HDF5:
		df.to_hdf(outFilePath, "group", mode= 'w')
	elif outFileType ==FileTypeEnum.MsgPack:
		df.to_msgpack(outFilePath)
	elif outFileType ==FileTypeEnum.Parquet:
		df.to_parquet(outFilePath)
	elif outFileType == FileTypeEnum.Stata:
		df.to_stata(outFilePath, write_index=False)
	elif outFileType == FileTypeEnum.Pickle:
		df.to_pickle(outFilePath)
	elif outFileType == FileTypeEnum.HTML:
		html = df.to_html(na_rep=null,index=False)
		outFile = open(outFilePath, "w")
		outFile.write(html)
		outFile.close()
def operatorEnumConverter(operator: OperatorEnum):
	if operator == OperatorEnum.Equals:
		return "=="
	elif operator == OperatorEnum.GreaterThan:
		return ">"
	elif operator == OperatorEnum.GreaterThanOrEqualTo:
		return ">="
	elif operator == OperatorEnum.LessThan:
		return "<"
	elif operator == OperatorEnum.LessThanOrEqualTo:
		return "<="
	elif operator == OperatorEnum.NotEquals:
		return "!="

def convertQueriesToString(continuousQueries: list=[], discreteQueries: list=[]):
	if len(continuousQueries) ==0 and len(discreteQueries)==0:
		return None
	
	completeQuery=""
	for i in range(0, len(continuousQueries)):
		completeQuery+= continuousQueries[i].columnName + operatorEnumConverter(continuousQueries[i].operator) + str(continuousQueries[i].value)
		if i < len(continuousQueries)-1 or len(discreteQueries)>0:
			completeQuery+=" and "

	for i in range(0, len(discreteQueries)):
		completeQuery+="("
		for j in range(0, len(discreteQueries[i].values)):
			completeQuery+= discreteQueries[i].columnName + "==" + "'"+discreteQueries[i].values[j]+"'"
			if j<len(discreteQueries[i].values)-1:
				completeQuery+=" or "
		completeQuery+=")"
		if i<len(discreteQueries)-1:
			completeQuery+=" and "
	print(completeQuery)
	return completeQuery
	

def exportQueryResults(parquetFilePath, outFilePath, outFileType:FileTypeEnum, columnList: list=[], continuousQueries: list=[], discreteQueries: list=[], transpose= False, includeAllColumns = False):
	"""
	Performs mulitple queries on a parquet dataset and exports results to a file of specified type. If no queries or columns are passed, it exports the entire dataset as a pandas dataframe. Otherwise, exports the queried data over the requested columns. This function differs from 'exportFilterResults' in that it takes ContinuousQuery and DiscreteQuery objects rather than a single string representing all queries 
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be queried on
	
	:type outFilePath: string
	:param outFilePath: name of the file that query results will written to
	
	:type outFileType: FileTypeEnum
	:param outFileType: an enumerated object specifying what sort of file to which results will be exported	
	
	:type columnList: list of strings
	:param columnList: list of column names that will be included in the data resulting from the queries
	
	:type continuousQueries: list of ContinuousQuery objects
	:param continuousQueries: list of objects representing queries on a column of continuous data
	
	:type discreteQueries: list of DiscreteQuery objects
	:param discreteQueries: list of objects representing queries on a column of discrete data
	
	:type transpose: bool
	:param transpose: if True, index and columns will be transposed
	
	:type includeAllColumns: bool
        :param includeAllColumns: if true, will include all columns in results. Overrides columnList if True 
	"""
	query = convertQueriesToString(continuousQueries, discreteQueries)
	exportFilterResults(parquetFilePath, outFilePath, outFileType, columnList=columnList, query=query, transpose = transpose, includeAllColumns = includeAllColumns)
