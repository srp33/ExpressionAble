import pyarrow.parquet as pq
import re
import pandas as pd
from shapeshifter import columninfo, operatorenum, filetypeenum
from shapeshifter.utils import to_arff, arff_to_pandas, to_gct, gct_to_pandas
import gzip
import time
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
	allCols = get_column_names(parquetFilePath)
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

def peek_by_column_names(parquetFilePath, listOfColumnNames, numRows=10, indexCol="Sample")->pd.DataFrame:
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

def get_column_names(parquetFilePath)->list:
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

def get_column_info(parquetFilePath, columnName:str, sizeLimit:int=None)->columninfo:
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

	# uniqueValues = set()
	# for index, row in df.iterrows():
	# 	try:
	# 		uniqueValues.add(row[columnName])
	# 	except (TypeError, KeyError) as e:
	# 		return None
	# 	if sizeLimit != None:
	# 		if len(uniqueValues)>=sizeLimit:
	# 			break
	# uniqueValues = list(uniqueValues)

	uniqueValues = df[columnName].unique().tolist()
	#uniqueValues.remove(None)
	#Todo: There is probably a better way to do this...
	i=0
	while uniqueValues[i] == None:
		i+=1
	if isinstance(uniqueValues[i],str) or isinstance(uniqueValues[i],bool):
		return columninfo(columnName, "discrete", uniqueValues)
	else:
		return columninfo(columnName, "continuous", uniqueValues)

def get_all_columns_info(parquetFilePath):
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
	                columnDict[col] = columninfo(col, "discrete", uniqueValues)
		else:
			columnDict[col] = columninfo(col, "continuous", uniqueValues)
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
		columnList = get_column_names(parquetFilePath)
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
		if query.operator == operatorenum.Equals:
			df = df.loc[df[query.columnName]==query.value, [ col for col in columnList]]
		elif query.operator == operatorenum.GreaterThan:
			df = df.loc[df[query.columnName]>query.value, [ col for col in columnList]]
		elif query.operator == operatorenum.GreaterThanOrEqualTo:
			df = df.loc[df[query.columnName]>=query.value, [ col for col in columnList]]
		elif query.operator == operatorenum.LessThan:
			df = df.loc[df[query.columnName]<query.value, [ col for col in columnList]]
		elif query.operator == operatorenum.LessThanOrEqualTo:
			df = df.loc[df[query.columnName]<=query.value, [ col for col in columnList]]
		elif query.operator== operatorenum.NotEquals:
			df = df.loc[df[query.columnName]!=query.value, [col for col in columnList]]
	#perform discrete queries
	for query in discreteQueries:
		df = df.loc[df[query.columnName].isin(query.values), [col for col in columnList]]
	
	return df

def translate_null_query(query):
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

def parse_column_names_from_query(query):
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


def check_if_columns_exist(df, columnList):
	missingColumns=[]
	for column in columnList:
		if column not in df.columns:
			missingColumns.append(column)
	#if len(missingColumns)>0:
	#	raise ColumnNotFoundError(missingColumns)
	return missingColumns

def filter_data(parquetFilePath, columnList=[], inputFileType=filetypeenum.Parquet, query=None, includeAllColumns = False, indexCol="Sample"):
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
	t1=time.time()
	if len(columnList)==0 and query == None:
		#df = pd.read_parquet(parquetFilePath)
		df = read_input_to_pandas(parquetFilePath, inputFileType, columnList, indexCol)
		t2=time.time()
		print("Time to read to pandas: " + str(t2-t1))
		if indexCol in df.columns:
			df.set_index(indexCol, drop=True, inplace=True)
			df.reset_index(inplace=True)
		return df
	elif len(columnList)>0 and query==None and not includeAllColumns:
		if indexCol not in columnList:
			columnList.insert(0,indexCol)
		else:
			columnList.insert(0, columnList.pop(columnList.index(indexCol)))
		#df=pd.read_parquet(parquetFilePath, columns=columnList)
		df=read_input_to_pandas(parquetFilePath, inputFileType, columnList, indexCol)
		t2=time.time()
		print("Time to read to pandas: " + str(t2-t1))
		return df
	if includeAllColumns:
		columnList = get_column_names(parquetFilePath)
	else:
		queryColumns= parse_column_names_from_query(query)
		columnList = queryColumns+columnList 
	if indexCol not in columnList:
		columnList.insert(0, indexCol)
	else:
		columnList.insert(0, columnList.pop(columnList.index(indexCol)))
	df = read_input_to_pandas(parquetFilePath, inputFileType, columnList, indexCol)
	t2=time.time()
	print("Time to read to pandas: " + str(t2-t1))
	missingColumns =  check_if_columns_exist(df, columnList)
	t3=time.time()
	df=df.query(query)	
	t4=time.time()
	print("Time to filter: " +str(t4-t3))
	if len(missingColumns)>0:
		print("Warning: the following columns were not found and therefore not included in output: " + ", ".join(missingColumns))
	return df
	
def append_gz(outFilePath):
	if not (outFilePath[len(outFilePath) -3] == '.' and outFilePath[len(outFilePath)-2] =='g' and outFilePath[len(outFilePath)-1] =='z'):
		outFilePath += '.gz'
	return outFilePath


def compress_results(outFilePath):
	#file name in and out must be different, I can't compress a file without changing its filepath as well
	with open(outFilePath, 'rb') as f_in:
		with gzip.open(append_gz(outFilePath), 'wb') as f_out:
			f_out.writelines(f_in)
	os.remove(outFilePath)


def read_input_to_pandas(inputFilePath, inputFileType, columnList, indexCol):
	if inputFileType==filetypeenum.Parquet:

		
		df=pd.read_parquet(inputFilePath, columns=columnList)
		if df.index.name == indexCol:
			df.reset_index(inplace=True)	
		return(df)
	elif inputFileType==filetypeenum.TSV:
		if len(columnList)==0:
			return pd.read_csv(inputFilePath, sep="\t")
		return pd.read_csv(inputFilePath, sep="\t", usecols=columnList)	
	elif inputFileType == filetypeenum.CSV:
		if len(columnList)==0:
			return pd.read_csv(inputFilePath)
		return pd.read_csv(inputFilePath, usecols=columnList)
	elif inputFileType == filetypeenum.JSON:
		df=pd.read_json(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			columnList[columnList.index(indexCol)]='index'
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.Excel:
		df= pd.read_excel(inputFilePath)
		if len(columnList)>0:
			print(columnList)
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.HDF5:
		df= pd.read_hdf(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.MsgPack:
		df= pd.read_msgpack(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.Stata:
		t1=time.time()
		if len(columnList)>0:
			return pd.read_stata(inputFilePath, columns=columnList)
		t2=time.time()
		print(str(t2-t1))
		return pd.read_stata(inputFilePath)
	elif inputFileType == filetypeenum.Pickle:
		df=pd.read_pickle(inputFilePath)
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.HTML:
		df=pd.read_html(inputFilePath)[0]
		df=df.reset_index()
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.ARFF:
		df= arff_to_pandas(inputFilePath)
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.GCT:
		df= gct_to_pandas(inputFilePath)
		if len(columnList)>0:
			df=df[columnList]
		return df
	elif inputFileType == filetypeenum.SQLite:
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
	
	

def export_filter_results(parquetFilePath, outFilePath, outFileType:filetypeenum, inputFileType=filetypeenum.Parquet, gzippedInput=False, columnList: list=[], query=None, transpose= False, includeAllColumns = False, gzipResults=False, indexCol="Sample"):
	"""
	Performs mulitple queries on a parquet dataset and exports results to a file of specified type. If no queries or columns are passed, it exports the entire dataset as a pandas dataframe. Otherwise, exports the queried data over the requested columns 

	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be filtered

	:type outFilePath: string
	:param outFilePath: name of the file that query results will written to

	:type outFileType: filetypeenum
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
		query=translate_null_query(query)
	if gzippedInput:
		df = filter_data(gzip.open(parquetFilePath), columnList=columnList, query=query, inputFileType=inputFileType, includeAllColumns = includeAllColumns, indexCol=indexCol)
	else:
		df = filter_data(parquetFilePath, columnList=columnList, query=query, inputFileType=inputFileType, includeAllColumns=includeAllColumns, indexCol=indexCol)
	null= 'NA'
	includeIndex=False
	t1=time.time()
	if transpose and outFileType!=filetypeenum.SQLite:
		df=df.set_index(indexCol) if indexCol in df.columns else df
		df=df.transpose()
		includeIndex=True
	if outFileType== filetypeenum.TSV:
		if gzipResults:
			outFilePath= append_gz(outFilePath)
			df.to_csv(path_or_buf=outFilePath, sep='\t',na_rep=null, index=includeIndex, compression = 'gzip')
		else:
			df.to_csv(path_or_buf=outFilePath, sep='\t',na_rep=null, index=includeIndex)
	elif outFileType == filetypeenum.CSV:
		if gzipResults:
			outFilePath=append_gz(outFilePath)
			df.to_csv(path_or_buf=outFilePath, na_rep=null, index=includeIndex, compression = 'gzip')
		else:
			df.to_csv(path_or_buf=outFilePath, na_rep=null, index=includeIndex)
	elif outFileType == filetypeenum.JSON:
		if not transpose:
			df=df.set_index(indexCol,drop=True) if indexCol in df.columns else df 
		if gzipResults:
			outFilePath = append_gz(outFilePath)
			df.to_json(path_or_buf=outFilePath, compression='gzip')
		else:
			df.to_json(path_or_buf=outFilePath)
	elif outFileType == filetypeenum.Excel:
		#NEED TO GZIP MANUALLY
		if len(df.columns)>16384 or len(df.index)>1048576:
			print("WARNING: Excel supports a maximum of 16,384 columns and 1,048,576 rows. The dimensions of your data are " + str(df.shape))
			print("Data beyond the size limit will be truncated")
		import xlsxwriter
		writer = pd.ExcelWriter(outFilePath, engine='xlsxwriter')
		df.to_excel(writer, sheet_name='Sheet1', na_rep=null, index=includeIndex) 
		writer.save()
		if gzipResults:
			compress_results(outFilePath)
#	elif outFileType == FileTypeEnum.Feather:
#		df=df.reset_index()
#		#MANUALLY GZIP
#		df.to_feather(outFilePath)
#		if gzipResults:
#			compressResults(outFilePath)
	elif outFileType ==filetypeenum.HDF5:
		#manually gzip
		if not transpose:
			df=df.set_index(indexCol) if indexCol in df.columns else df
		df.to_hdf(outFilePath, "group", mode= 'w')
		if gzipResults:
			compress_results(outFilePath)
	elif outFileType ==filetypeenum.MsgPack:
		#manually gzip
		if not transpose:
			df=df.set_index(indexCol) if indexCol in df.columns else df
		df.to_msgpack(outFilePath)
		if gzipResults:
			compress_results(outFilePath)

	elif outFileType ==filetypeenum.Parquet:
		if not transpose:
			df=df.set_index(indexCol) if indexCol in df.columns else df
		if gzipResults:
			df.to_parquet(append_gz(outFilePath), compression='gzip')
		else:
			df.to_parquet(outFilePath)
	elif outFileType == filetypeenum.Stata:
		#manually gzip
		if not transpose:
			df=df.set_index(indexCol) if indexCol in df.columns else df
		df.to_stata(outFilePath, write_index=True)
		if gzipResults:
			compress_results(outFilePath)

	elif outFileType == filetypeenum.Pickle:
		if not transpose:
			df=df.set_index(indexCol) if indexCol in df.columns else df
		if gzipResults:
			df.to_pickle(append_gz(outFilePath), compression='gzip')
		else:
			df.to_pickle(outFilePath)
	elif outFileType == filetypeenum.HTML:
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
	elif outFileType == filetypeenum.SQLite:
		if len(df.columns) > 2000:
			print("Error: SQLite supports a maximum of 2,000 columns. Your data has " + str(len(df.columns)) + " columns")
			return
		from sqlalchemy import create_engine
		engine = create_engine('sqlite:///'+outFilePath)
		table = outFilePath.split('.')[0]
		tableList= table.split('/')
		table= tableList[len(tableList)-1]
		if not transpose:
			df=df.set_index(indexCol) if indexCol in df.columns else df
			df.to_sql(table,engine, index=True, if_exists="replace")
		else:
			df=df.set_index(indexCol) if indexCol in df.columns else df
			df=df.transpose()
			df.to_sql(table, engine, if_exists="replace", index=True, index_label=indexCol)
		if gzipResults:
			compress_results(outFilePath)
	elif outFileType == filetypeenum.ARFF:
		#if not transpose:
		#	df=df.set_index(indexCol)
		to_arff(df, outFilePath)
		if gzipResults:
			compress_results(outFilePath)
	elif outFileType == filetypeenum.GCT:
		to_gct(df, outFilePath)
		if gzipResults:
			compress_results(outFilePath)
	t2=time.time()
	print("Time to write to file: " + str(t2-t1))
		
def operator_enum_converter(operator: operatorenum):
	"""
	Function for internal use. Used to translate an OperatorEnum into a string representation of that operator
	"""

	if operator == operatorenum.Equals:
		return "=="
	elif operator == operatorenum.GreaterThan:
		return ">"
	elif operator == operatorenum.GreaterThanOrEqualTo:
		return ">="
	elif operator == operatorenum.LessThan:
		return "<"
	elif operator == operatorenum.LessThanOrEqualTo:
		return "<="
	elif operator == operatorenum.NotEquals:
		return "!="

def convert_queries_to_string(continuousQueries: list=[], discreteQueries: list=[]):
	"""
	Function for internal use. Given a list of ContinuousQuery objects and DiscreteQuery objects, returns a single string representing all given queries
	"""

	if len(continuousQueries) ==0 and len(discreteQueries)==0:
		return None
	
	completeQuery=""
	for i in range(0, len(continuousQueries)):
		completeQuery+= continuousQueries[i].columnName + operator_enum_converter(continuousQueries[i].operator) + str(continuousQueries[i].value)
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
	

def export_query_results(parquetFilePath, outFilePath, outFileType:filetypeenum, columnList: list=[], continuousQueries: list=[], discreteQueries: list=[], transpose= False, includeAllColumns = False):
	"""
	Performs mulitple queries on a parquet dataset and exports results to a file of specified type. If no queries or columns are passed, it exports the entire dataset as a pandas dataframe. Otherwise, exports the queried data over the requested columns. This function differs from 'exportFilterResults' in that it takes ContinuousQuery and DiscreteQuery objects rather than a single string representing all queries 
	
	:type parquetFilePath: string
	:param parquetFilePath: filepath to a parquet file to be queried on
	
	:type outFilePath: string
	:param outFilePath: name of the file that query results will be written to
	
	:type outFileType: filetypeenum
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
	query = convert_queries_to_string(continuousQueries, discreteQueries)
	export_filter_results(parquetFilePath, outFilePath, outFileType, columnList=columnList, query=query, transpose = transpose, includeAllColumns = includeAllColumns)
	
