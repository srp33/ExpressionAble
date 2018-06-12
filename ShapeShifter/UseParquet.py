import pyarrow.parquet as pq
import re
#import pyarrow as pa
import pandas as pd
from ColumnInfo import ColumnInfo
from ContinuousQuery import ContinuousQuery
from DiscreteQuery import DiscreteQuery
from OperatorEnum import OperatorEnum
from FileTypeEnum import FileTypeEnum
from ColumnNotFoundError import ColumnNotFoundError
from ConvertARFF import toARFF
from ConvertARFF import arffToPandas
import gzip
import time
import sys
import os

def peek(parquetFilePath, numRows=10, numCols=10,indexCol="Sample")->pd.DataFrame:
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
	selectedCols.append(indexCol)
	for i in range(0, numCols):
		selectedCols.append(allCols[i])
	df = pd.read_parquet(parquetFilePath, columns=selectedCols)
	df.set_index(indexCol, drop=True, inplace=True)
	df=df.iloc[0:numRows, 0:numCols]
	return df

def peekByColumnNames(parquetFilePath, listOfColumnNames,numRows=10,indexCol="Sample")->pd.DataFrame:
	"""
	Peeks into a parquet file by looking at a specific set of columns
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be examined

	:type numRows: int
	:param numRows: the number of rows the returned Pandas dataframe will contain

	:return: The first numRows of all the listed columns in the given parquet file
	:rtype: Pandas dataframe

	"""
	listOfColumnNames.insert(0,indexCol)
	df = pd.read_parquet(parquetFilePath, columns=listOfColumnNames)
	df.set_index(indexCol, drop=True, inplace=True)
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

def query(parquetFilePath, columnList: list=[], continuousQueries: list=[], discreteQueries: list=[], includeAllColumns = False,indexCol="Sample")->pd.DataFrame:
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
	if indexCol not in columnList:
		columnList.insert(0, indexCol)
	else:
		columnList.insert(0, columnList.pop(columnList.index(indexCol)))
	
	df = pd.read_parquet(parquetFilePath, columns = columnList)

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

def translateNullQuery(query):
	"""
	For internal use only. Because pandas does not support querying for null values by "columnname == None", this function translates such queries into valid syntax
	"""
	regex1 = r"\S*\s*!=\s*None\s*"
	regex2 = r"\S*\s*==\s*None\s*"
	matchlist1=re.findall(regex1, query, flags=0)
	matchlist2=re.findall(regex2,query,flags=0)
	for match in matchlist1:
		col = match.split("!=")[0].rstrip()
		query=query.replace(match, col+"=="+col+" ")
	for match in matchlist2:
		col = match.split("==")[0].rstrip()
		query=query.replace(match, col+"!="+col+" ")
	return query	

def parseColumnNamesFromQuery(query):
	"""
	For internal use. Takes a query and determines what columns are being queried on
	"""
	query=re.sub(r'\band\b','&',query)
	query=re.sub(r'\bor\b','|',query)
	args = re.split('==|<=|>=|!=|<|>|\&|\|', query)
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
			if len(arg)>0 and arg[0]!="'" and arg[0]!='"':
				#check for duplicates
				if arg not in colList and arg !="True" and arg!="False":
					colList.append(arg)
	return colList


def checkIfColumnsExist(df, columnList):
	missingColumns=[]
	for column in columnList:
		if column not in df.columns:
			missingColumns.append(column)
	#if len(missingColumns)>0:
	#	raise ColumnNotFoundError(missingColumns)
	return missingColumns

def filterData(parquetFilePath, columnList=[],inputFileType=FileTypeEnum.Parquet, query=None, includeAllColumns = False, indexCol="Sample"):
	"""	
	Applies a filter to a parquet dataset. If no filter or columns are passed in, it returns the entire dataset as a pandas dataframe. Otherwise, returns only the filtered data over the requested columns as a Pandas dataframe

	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be filtered

	:type columnList: list of strings
	:param columnList: list of column names that will be included in the data resulting from the filter

	:type query: string
	:param query: filter to apply to the dataset, written using python logical syntax

	:type includeAllColumns: bool
	:param includeAllColumns: if True, will include all columns in the filtered dataset. Overrides columnList if True
	"""
	if len(columnList)==0 and query == None:
		#df = pd.read_parquet(parquetFilePath)
		df = readInputToPandas(parquetFilePath, inputFileType, columnList,indexCol)
		df.set_index(indexCol, drop=True, inplace=True)
		df.reset_index(inplace=True)
		return df
	elif len(columnList)>0 and query==None and not includeAllColumns:
		if indexCol not in columnList:
			columnList.insert(0,indexCol)
		else:
			columnList.insert(0, columnList.pop(columnList.index(indexCol)))
		#df=pd.read_parquet(parquetFilePath, columns=columnList)
		df=readInputToPandas(parquetFilePath, inputFileType, columnList,indexCol)
		return df
	if includeAllColumns:
		columnList = getColumnNames(parquetFilePath)
	else:
		queryColumns= parseColumnNamesFromQuery(query)
		columnList = queryColumns+columnList 
	if indexCol not in columnList:
		columnList.insert(0, indexCol)
	else:
		columnList.insert(0, columnList.pop(columnList.index(indexCol)))

	df = readInputToPandas(parquetFilePath, inputFileType, columnList, indexCol)
	missingColumns =  checkIfColumnsExist(df, columnList) 
	df=df.query(query)	
	if len(missingColumns)>0:
		print("Warning: the following columns were not found and therefore not included in output: " + ", ".join(missingColumns))
	return df
	
def appendGZ(outFilePath):
	if not (outFilePath[len(outFilePath) -3] == '.' and outFilePath[len(outFilePath)-2] =='g' and outFilePath[len(outFilePath)-1] =='z'):
		outFilePath += '.gz'
	return outFilePath


def compressResults(outFilePath):
	#file name in and out must be different, I can't compress a file without changing its filepath as well
	with open(outFilePath, 'rb') as f_in:
		with gzip.open(appendGZ(outFilePath), 'wb') as f_out:
			f_out.writelines(f_in)
	os.remove(outFilePath)


def readInputToPandas(inputFilePath, inputFileType, columnList, indexCol):
	if inputFileType==FileTypeEnum.Parquet:
		if len(columnList)==0:
			return pd.read_parquet(inputFilePath)
		return pd.read_parquet(inputFilePath, columns=columnList)
	elif inputFileType==FileTypeEnum.TSV:
		if len(columnList)==0:
			return pd.read_csv(inputFilePath, sep="\t")
		return pd.read_csv(inputFilePath, sep="\t", usecols=columnList)	
	elif inputFileType == FileTypeEnum.CSV:
		if len(columnList)==0:
			return pd.read_csv(inputFilePath)
		return pd.read_csv(inputFilePath, usecols=columnList)
	elif inputFileType == FileTypeEnum.JSON:
		df=pd.read_json(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			columnList[columnList.index(indexCol)]='index'
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.Excel:
		df= pd.read_excel(inputFilePath)
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.HDF5:
		df= pd.read_hdf(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.MsgPack:
		df= pd.read_msgpack(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.Stata:
		if len(columnList)>0:
			return pd.read_stata(inputFilePath, columns=columnList)
		return pd.read_stata(inputFilePath)
	elif inputFileType == FileTypeEnum.Pickle:
		df=pd.read_pickle(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.HTML:
		df=pd.read_html(inputFilePath)[0]
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.ARFF:
		df= arffToPandas(inputFilePath)
		print(df.columns)
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == FileTypeEnum.SQLite:
		from sqlalchemy import create_engine
		engine = create_engine('sqlite:///'+inputFilePath)
		table=inputFilePath.split('.')[0]
		tableList=table.split('/')
		table=tableList[len(tableList)-1]
		query="SELECT * FROM "+table
		if len(columnList)>0:
			query="SELECT " +", ".join(columnList) +" FROM " + table

		df=pd.read_sql(query, engine)
		return df
	
	

def exportFilterResults(parquetFilePath, outFilePath, outFileType:FileTypeEnum, inputFileType=FileTypeEnum.Parquet, gzippedInput=False, columnList: list=[], query=None, transpose= False, includeAllColumns = False, gzipResults=False, indexCol="Sample"):
	"""
	Performs mulitple queries on a parquet dataset and exports results to a file of specified type. If no queries or columns are passed, it exports the entire dataset as a pandas dataframe. Otherwise, exports the queried data over the requested columns 

	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be filtered

	:type outFilePath: string
	:param outFilePath: name of the file that query results will written to

	:type outFileType: FileTypeEnum
	:param outFileType: an enumerated object specifying what sort of file to which results will be exported	

	:type columnList: list of strings
	:param columnList: list of column names that will be included in the data resulting from the filter

	:type query: string
	:param query: filter to apply to the dataset, written using python logical syntax

	:type transpose: bool
	:param transpose: if True, index and columns will be transposed

	:type includeAllColumns: bool
        :param includeAllColumns: if True, will include all columns in the filtered dataset. Overrides columnList if True

	:type gzipResults: bool
	:param gzipResults: if True, the output file will be compressed with gzip 

	"""
	if query != None:
		query=translateNullQuery(query)
	if gzippedInput:
		df = filterData(gzip.open(parquetFilePath), columnList=columnList, query=query, inputFileType=inputFileType, includeAllColumns = includeAllColumns,indexCol=indexCol)
	else:
		df = filterData(parquetFilePath, columnList=columnList, query=query, inputFileType=inputFileType, includeAllColumns=includeAllColumns,indexCol=indexCol)
	null= 'NA'
	includeIndex=False
	if transpose and outFileType!=FileTypeEnum.SQLite:
		df=df.set_index(indexCol)
		df=df.transpose()
		includeIndex=True
	if outFileType== FileTypeEnum.TSV:
		if gzipResults:
			outFilePath= appendGZ(outFilePath)
			df.to_csv(path_or_buf=outFilePath, sep='\t',na_rep=null, index=includeIndex, compression = 'gzip')
		else:
			df.to_csv(path_or_buf=outFilePath, sep='\t',na_rep=null, index=includeIndex)
	elif outFileType == FileTypeEnum.CSV:
		if gzipResults:
			outFilePath=appendGZ(outFilePath)
			df.to_csv(path_or_buf=outFilePath, na_rep=null, index=includeIndex, compression = 'gzip')
		else:
			df.to_csv(path_or_buf=outFilePath, na_rep=null, index=includeIndex)
	elif outFileType == FileTypeEnum.JSON:
		if not transpose:
			df=df.set_index(indexCol,drop=True)
		if gzipResults:
			outFilePath = appendGZ(outFilePath)
			df.to_json(path_or_buf=outFilePath, compression='gzip')
		else:
			df.to_json(path_or_buf=outFilePath) 
	elif outFileType == FileTypeEnum.Excel:
		#NEED TO GZIP MANUALLY
		import xlsxwriter
		writer = pd.ExcelWriter(outFilePath, engine='xlsxwriter')
		df.to_excel(writer, sheet_name='Sheet1', na_rep=null, index=includeIndex) 
		writer.save()
		if gzipResults:
			compressResults(outFilePath)
#	elif outFileType == FileTypeEnum.Feather:
#		df=df.reset_index()
#		#MANUALLY GZIP
#		df.to_feather(outFilePath)
#		if gzipResults:
#			compressResults(outFilePath)
	elif outFileType ==FileTypeEnum.HDF5:
		#manually gzip
		if not transpose:
			df=df.set_index(indexCol)
		df.to_hdf(outFilePath, "group", mode= 'w')
		if gzipResults:
			compressResults(outFilePath)
	elif outFileType ==FileTypeEnum.MsgPack:
		#manually gzip
		if not transpose:
			df=df.set_index(indexCol)
		df.to_msgpack(outFilePath)
		if gzipResults:
			compressResults(outFilePath)

	elif outFileType ==FileTypeEnum.Parquet:
		if not transpose:
			df=df.set_index(indexCol)
		if gzipResults:
			df.to_parquet(appendGZ(outFilePath), compression='gzip')
		else:
			df.to_parquet(outFilePath)
	elif outFileType == FileTypeEnum.Stata:
		#manually gzip
		if not transpose:
			df=df.set_index(indexCol)
		df.to_stata(outFilePath, write_index=True)
		if gzipResults:
			compressResults(outFilePath)

	elif outFileType == FileTypeEnum.Pickle:
		if not transpose:
			df=df.set_index(indexCol)
		if gzipResults:
			df.to_pickle(appendGZ(outFilePath), compression='gzip')
		else:
			df.to_pickle(outFilePath)
	elif outFileType == FileTypeEnum.HTML:
		#if not transpose:
		#	df=df.set_index(indexCol)
		html = df.to_html(na_rep=null,index=False)
		if gzipResults:
			html= html.encode()
			with gzip.open(outFilePath,'wb') as f:
				f.write(html)
		else:
			outFile = open(outFilePath, "w")
			outFile.write(html)
			outFile.close()
	elif outFileType == FileTypeEnum.SQLite:
		from sqlalchemy import create_engine
		engine = create_engine('sqlite:///'+outFilePath)
		table = outFilePath.split('.')[0]
		tableList= table.split('/')
		table= tableList[len(tableList)-1]
		if not transpose:
			df=df.set_index(indexCol)
			df.to_sql(table,engine, index=True, if_exists="replace")
		else:
			df=df.set_index(indexCol)
			df=df.transpose()
			df.to_sql(table, engine, if_exists="replace", index=True, index_label=indexCol)
		if gzipResults:
			compressResults(outFilePath)
	elif outFileType == FileTypeEnum.ARFF:
		#if not transpose:
		#	df=df.set_index(indexCol)
		toARFF(df, outFilePath)
		if gzipResults:
			compressResults(outFilePath)
		
def operatorEnumConverter(operator: OperatorEnum):
	"""
	Function for internal use. Used to translate an OperatorEnum into a string representation of that operator
	"""

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
	"""
	Function for internal use. Given a list of ContinuousQuery objects and DiscreteQuery objects, returns a single string representing all given queries
	"""

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
	#print(completeQuery)
	return completeQuery
	

def exportQueryResults(parquetFilePath, outFilePath, outFileType:FileTypeEnum, columnList: list=[], continuousQueries: list=[], discreteQueries: list=[], transpose= False, includeAllColumns = False):
	"""
	Performs mulitple queries on a parquet dataset and exports results to a file of specified type. If no queries or columns are passed, it exports the entire dataset as a pandas dataframe. Otherwise, exports the queried data over the requested columns. This function differs from 'exportFilterResults' in that it takes ContinuousQuery and DiscreteQuery objects rather than a single string representing all queries 
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be queried on
	
	:type outFilePath: string
	:param outFilePath: name of the file that query results will be written to
	
	:type outFileType: FileTypeEnum
	:param outFileType: an enumerated object specifying what sort of file to which results will be exported	
	
	:type columnList: list of strings
	:param columnList: list of column names that will be included in the data resulting from the queries
	
	:type continuousQueries: list of ContinuousQuery objects
	:param continuousQueries: list of objects representing queries on a column of continuous data
	
	:type discreteQueries: list of DiscreteQuery objects
	:param discreteQueries: list of objects representing queries on a column of discrete data
	
	:type transpose: bool
	:param transpose: if True, index and columns will be transposed in the output file
	
	:type includeAllColumns: bool
        :param includeAllColumns: if True, will include all columns in resulting dataset. Overrides columnList if True 
	"""
	query = convertQueriesToString(continuousQueries, discreteQueries)
	exportFilterResults(parquetFilePath, outFilePath, outFileType, columnList=columnList, query=query, transpose = transpose, includeAllColumns = includeAllColumns)
	
