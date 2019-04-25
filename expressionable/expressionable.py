import pandas as pd

from expressionable.files import EAFile


class ExpressionAble:
    """
    Creates an ExpressionAble object, which represents a file to be transformed.

    :type file_path: str
    :param file_path: Name of a file path to read and perform operations on.

    :type file_type: str
    :param file_type: Name of the type of file that is being read.
    """
    def __init__(self, file_path, file_type=None):


        self.input_file = EAFile.factory(file_path, file_type)
        self.gzipped_input= self.__is_gzipped()
        self.output_file= None

    def export_filter_results(self, output_file_path, data_type=None, filters=None, columns=[],
                              transpose=False, include_all_columns=False, gzip_results=False, index=None):
        """
        Filters and then exports data to a file.

        :type output_file_path: str
        :param output_file_path: Name of the file that results will be saved to.

        :type data_type: str, default None
        :param data_type: Name of the file format results will be saved to. If None, the type will be inferred from the file path.

        :type filters: str, default None
        :param filters: Query or filter to apply to the data set written in Python logic.

        :type columns: list of str, default []
        :param columns: Names of columns to include in the output. If blank and no filter is specified, all columns will be included.

        :type transpose: bool, default False
        :param transpose: If True, index and columns will be transposed in the output file.

        :type include_all_columns: bool, default False
        :param include_all_columns: Indicates whether to include all columns in the output. If True, overrides columnList.

        :type gzip_results: bool, default False
        :param gzip_results: Indicates whether the resulting file will be gzipped.

        :type index: str, default None
        :param index: Name of the column to be set as index.

        :return: None
        """
        self.output_file = EAFile.factory(output_file_path, data_type)
        self.output_file.export_filter_results(self.input_file, column_list=columns, query=filters, transpose=transpose,
                                               include_all_columns=include_all_columns, gzip_results=gzip_results,
                                               index_col=index)

    def export_query_results(self, out_file_path, out_file_type=None, columns= [],
                             continuous_queries = [], discrete_queries = [], transpose=False,
                             include_all_columns=False, gzip_results = False):
        """
        Filters and exports data to a file. Similar to export_filter_results, but takes filters in the form of ContinuousQuery and DiscreteQuery objects,
        and has slightly less flexible functionality

        :type out_file_path: str
        :param out_file_path: Name of the file that results will be saved to.

        :type out_file_type: str, default None
        :param out_file_type: Name of the file format results will be saved to. If None, the type will be inferred from the file path.

        :type columns: list of str, default []
        :param columns: Names of columns to include in the output. If blank and no filter is specified, all columns will be included.

        :type continuous_queries: list of ContinuousQuery.
        :param continuous_queries: Objects representing queries on a column of continuous data.

        :type discrete_queries: list of DiscreteQuery
        :param discrete_queries: Objects representing queries on a column of discrete data.

        :type transpose: bool, default False
        :param transpose: If True, index and columns will be transposed in the output file.

        :type include_all_columns: bool, default False
        :param include_all_columns: Indicates whether to include all columns in the output. If True, overrides columnList.

        :type gzip_results: bool, default False
        :param gzip_results: Indicates whether the resulting file will be gzipped.

        :return: None
        """

        self.output_file = EAFile.factory(out_file_path, out_file_type)
        query = self.__convert_queries_to_string(continuous_queries, discrete_queries)
        self.output_file.export_filter_results(self.input_file, column_list=columns, query=query,
                                               transpose=transpose, include_all_columns=include_all_columns, gzip_results=gzip_results)

    def get_filtered_samples(self, continuous_queries, discrete_queries):
        query = self.__convert_queries_to_string(continuous_queries, discrete_queries)
        df = self.input_file._filter_data(query=query)
        return df.index.values

    def peek_by_column_names(self, listOfColumnNames, numRows=10, indexCol="Sample"):
        """
        Takes a look at a portion of the file by showing only the requested columns.

        :type listOfColumnNames: list of str
        :param listOfColumnNames: Names of columns that will be given in the output.

        :type numRows: int, default 10
        :param numRows: The number of rows that will be shown with the requested columns in the output.

        :type indexCol: str, default 'Sample'
        :param indexCol: Name of the column that will be the index column in the DataFrame.

        :return: Pandas DataFrame with only the requested columns and number of rows.
        """
        listOfColumnNames.insert(0, indexCol)
        df = self.input_file.read_input_to_pandas(columnList=listOfColumnNames, indexCol = indexCol)
        #df = pd.read_parquet(self.inputFile.filePath, columns=listOfColumnNames)
        df.set_index(indexCol, drop=True, inplace=True)
        df = df[0:numRows]
        return df

    def merge_files(self, files_to_merge, out_file_path, files_to_merge_types=[], out_file_type=None, gzip_results=False, on=None, how='inner'):
        """
        Merges multiple ExpressionAble-compatible files into a single file.

        :type files_to_merge: list of str
        :param files_to_merge: File paths representing files that will be merged with the file in this ExpressionAble object.

        :type out_file_path: str
        :param out_file_path: File path where the output of merging the files will be stored.

        :type files_to_merge_types: list of str
        :param files_to_merge_types: list of file types corresponding to files_to_merge. If the list is empty, types will be inferred from file extensions. If the list has one value, that will be the type of every file in files_to_merge. If the list has the same number of items as files_to_merge, the types will correspond to the files in files_to_merge.

        :type out_file_type: str, default None
        :param out_file_type: Name of the file format that results will be saved to. If None, the type will be inferred from the file path.

        :type gzip_results: bool, default False
        :param gzip_results: Indicates whether the resulting file will be gzipped.

        :type on: str, default None
        :param on: Column or index level names to join on. These must be found in all files. If on is None and not merging on indexes then this defaults to the intersection of the columns in all.

        :return: None
        """
        how=how.lower()
        if how not in ['left','right','outer','inner']:
            print("Error: \'How\' must one of the following options: left, right, outer, inner")
            return
        outFile = EAFile.factory(out_file_path, out_file_type)
        eaFileList=[]
        #create a file object for every file path passed in
        if len(files_to_merge_types) == 1:
            for file in files_to_merge:
                eaFileList.append(EAFile.factory(file, type=files_to_merge_types[0]))
        elif len(files_to_merge_types) > 1:
            if len(files_to_merge_types) != len(files_to_merge):
                print("Error: the number of input files and the number of input file types must be the same "
                      "unless only one input type is specified, in which case all input files to merge are assumed"
                      "to be of that type.")
                return
            else:
                for i in range(len(files_to_merge)):
                    eaFileList.append(EAFile.factory(files_to_merge[i], type=files_to_merge_types[i]))
        else:
            for file in files_to_merge:
                eaFileList.append(EAFile.factory(file))
        if len(eaFileList) < 1:
            print("Error: there must be at least one input file to merge with.")
            return

        eaFileList.insert(0, self.input_file)
        df1 = eaFileList[0].read_input_to_pandas()

        #we keep track of how often a column name appears and will change the names to avoid duplicates if necessary
        #the exception is whatever columns they want to merge on - we don't keep track of those
        columnDict={}
        self.__increment_columnname_counters(columnDict, df1, on)
        if len(eaFileList) == 1:
            outFile.write_to_file(df1, gzipResults=gzip_results)
            return
        for i in range(0, len(eaFileList) - 1):
            df2 = eaFileList[i + 1].read_input_to_pandas()
            if on !=None:
                self.__increment_columnname_counters(columnDict, df2, on)
                self.__rename_common_columns(columnDict, df1, df2, on)
            #perform merge
            if on==None:
                df1 = pd.merge(df1, df2, how=how)
            else:
                df1=pd.merge(df1,df2, how=how, on=on)
        indexCol = list(df1.columns.values)[0]
        outFile.write_to_file(df1, gzipResults=gzip_results, indexCol=indexCol)
        return

    def __increment_columnname_counters(self, columnDict, df, on):
        """
        Function for internal use. Adds column names to a dictionary or increments its counter if it is already there
        :param columnDict: dictionary with column names as keys and ints as counters representing how many files the column name appears in
        :param df: Pandas data frame whose column names will be examined
        :param on: column name or list of column names that data frames will be merged on
        """
        for colName in list(df.columns.values):
            if colName in columnDict:
                columnDict[colName] += 1
            elif colName not in columnDict and (colName != on or colName not in on):
                columnDict[colName] = 1

    def __rename_common_columns(self, columnDict, df1, df2, on):
        """
        Function for internal use. Renames common columns between two data frames to distinguish them, so long as the columns are not part of 'on'
        :param columnDict: dictionary with column names as keys and ints as counters representing how many files the column name appears in
        :param df1: first Pandas data frame whose columns may be renamed
        :param df2: second Pandas data frame whose columns may be renamed
        :param on: column name or list of column names that the data frames will be merged on
        """
        commonColumns={}
        for colName in list(df1.columns.values):
            if colName == on or (isinstance(on, list) and colName in on):
                pass
            elif colName in columnDict and columnDict[colName] > 1:
                newColumnName = colName + '_' + str(columnDict[colName] - 1)
                commonColumns[colName]=newColumnName
        if len(commonColumns)>0:
            df1.rename(columns=commonColumns, inplace=True)
        commonColumns = {}
        for colName in list(df2.columns.values):
            if colName == on or (isinstance(on, list) and colName in on):
                pass
            elif colName in columnDict and columnDict[colName] > 1:
                newColumnName = colName + '_' + str(columnDict[colName])
                commonColumns[colName]=newColumnName
        if len(commonColumns)>0:
            df2.rename(columns=commonColumns, inplace=True)

    def get_column_info(self, columnName: str, sizeLimit: int = None):
        """
        Retrieves a specified column's name, data type, and all its unique values from a file.

        :type columnName: str
        :param columnName: The name of the column about which information is being obtained.

        :type sizeLimit: int
        :param sizeLimit: limits the number of unique values returned to be no more than this number.

        :return: Name, data type (continuous/discrete), and unique values from specified column
        :rtype: ColumnInfo object
        """
        from expressionable.utils import columninfo
        columnList = [columnName]
        #df = pd.read_parquet(self.inputFile.filePath, columns=columnList)
        df = self.input_file.read_input_to_pandas(columnList=columnList)

        uniqueValues = df[columnName].unique().tolist()
        # uniqueValues.remove(None)
        # Todo: There is probably a better way to do this...
        i = 0
        while uniqueValues[i] == None:
            i += 1
        if isinstance(uniqueValues[i], str) or isinstance(uniqueValues[i], bool):
            return columninfo.ColumnInfo(columnName, "discrete", uniqueValues)
        else:
            return columninfo.ColumnInfo(columnName, "continuous", uniqueValues)

    def get_all_columns_info(self):
        """
        Retrieves the column name, data type, and all unique values from every column in a file.

        :return: Name, data type (continuous/discrete), and unique values from every column.
        :rtype: dictionary where key: column name and value:ColumnInfo object containing the column name, data type (continuous/discrete), and unique values from all columns
        """
        # columnNames = getColumnNames(parquetFilePath)
        from expressionable.utils import columninfo
        df = self.input_file.read_input_to_pandas()
        columnDict = {}
        for col in df:
            uniqueValues = df[col].unique().tolist()
            i = 0
            while uniqueValues[i] == None:
                i += 1
            if isinstance(uniqueValues[i], str) or isinstance(uniqueValues[i], bool):
                columnDict[col] = columninfo.ColumnInfo(col, "discrete", uniqueValues)
            else:
                columnDict[col] = columninfo.ColumnInfo(col, "continuous", uniqueValues)
        return columnDict

    def __is_gzipped(self):
        """
        Function for internal use. Checks if a file is gzipped based on its extension
        """
        extensions = self.input_file.filePath.rstrip("\n").split(".")
        if extensions[len(extensions) - 1] == 'gz':
            return True
        return False

    def __convert_queries_to_string(self, continuousQueries: list = [], discreteQueries: list = []):
        """
        Function for internal use. Given a list of ContinuousQuery objects and DiscreteQuery objects, returns a single string representing all given queries
        """

        if len(continuousQueries) == 0 and len(discreteQueries) == 0:
            return None

        completeQuery = ""
        for i in range(0, len(continuousQueries)):
            completeQuery += continuousQueries[i].columnName + self.__operator_enum_converter(
                continuousQueries[i].operator) + str(continuousQueries[i].value)
            if i < len(continuousQueries) - 1 or len(discreteQueries) > 0:
                completeQuery += " and "

        for i in range(0, len(discreteQueries)):
            completeQuery += "("
            for j in range(0, len(discreteQueries[i].values)):
                completeQuery += discreteQueries[i].columnName + "==" + "'" + discreteQueries[i].values[j] + "'"
                if j < len(discreteQueries[i].values) - 1:
                    completeQuery += " or "
            completeQuery += ")"
            if i < len(discreteQueries) - 1:
                completeQuery += " and "
        # print(completeQuery)
        return completeQuery

    def __operator_enum_converter(self, operator):
        """
        Function for internal use. Used to translate an OperatorEnum into a string representation of that operator
        """
        from expressionable.utils import operatorenum
        if operator == operatorenum.OperatorEnum.Equals:
            return "=="
        elif operator == operatorenum.OperatorEnum.GreaterThan:
            return ">"
        elif operator == operatorenum.OperatorEnum.GreaterThanOrEqualTo:
            return ">="
        elif operator == operatorenum.OperatorEnum.LessThan:
            return "<"
        elif operator == operatorenum.OperatorEnum.LessThanOrEqualTo:
            return "<="
        elif operator == operatorenum.OperatorEnum.NotEquals:
            return "!="



########################################################################################################################

    ####### All of the following functions are designed for parquet only right now
    #todo: clean up all the documentation here

    def get_column_names(self) -> list:
        """
        Retrieves all column names from a dataset stored in a parquet file
        :type parquetFilePath: string
        :param parquetFilePath: filepath to a parquet file to be examined

        :return: All column names
        :rtype: list

        """
        return self.input_file.get_column_names()


    def peek(self, numRows=10, numCols=10):
        """
        Takes a look at the first few rows and columns of a parquet file and returns a Pandas DataFrame corresponding to the number of requested rows and columns

        :type numRows: int, default 10
        :param numRows: the number of rows the returned Pandas DataFrame will contain.

        :type numCols: int, default 10
        :param numCols: the number of columns the returned Pandas DataFrame will contain.

        :return: The first numRows and numCols in the given parquet file
        :rtype: Pandas DataFrame
        """
        #TODO: Optimize peek for every file type by writing a get_column_names() function for every file type
        # allCols = self.get_column_names()

        # selectedCols = []
        # selectedCols.append(self.index)
        # for i in range(0, numCols):
        #      selectedCols.append(allCols[i])
        # df = self.inputFile.read_input_to_pandas(columnList=selectedCols)
        # df.set_index(self.index, drop=True, inplace=True)

        df=self.input_file.read_input_to_pandas()
        if (numCols > len(df.columns)):
             numCols = len(df.columns)
        df = df.iloc[0:numRows, 0:numCols]
        return df
