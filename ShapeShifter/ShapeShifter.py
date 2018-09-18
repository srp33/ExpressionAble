import SSFile

class ShapeShifter:

    def __init__(self, filePath, fileType=None):
        """
        Creates a ShapeShifter object
        :param filePath: string name of a file path to read and perform operations on
        :param fileType: string indicating the type of file that is being read

        """

        self.inputFile = SSFile.SSFile.factory(filePath, fileType)
        self.gzippedInput= self.__is_gzipped()
        self.outputFile= None

    def export_filter_results(self, outFilePath, outFileType=None, filters=None, columns=[],
                              transpose=False, includeAllColumns=False, gzipResults=False, index='Sample'):
        """
        Filters and then exports data to a file
        :param outFilePath: Name of the file that results will be saved to
        :param outFileType: string indicating what file format results will be saved to
        :param filters: string representing the query or filter to apply to the data set
        :param columns: list of columns to include in the output. If blank and no filter is specified, all columns will be included.
        :param transpose: boolean when, if True, index and columns will be transposed in the output file
        :param includeAllColumns: boolean indicating whether to include all columns in the output. If True, overrides columnList
        :param gzipResults: boolean indicating whether the resulting file will be gzipped
        :return:
        """
        self.outputFile = SSFile.SSFile.factory(outFilePath,outFileType)
        self.outputFile.export_filter_results(self.inputFile, columnList=columns, query=filters, transpose=transpose,
                                              includeAllColumns=includeAllColumns, gzipResults=gzipResults,
                                              indexCol=index)

    def export_query_results(self, outFilePath, outFileType=None, columns= [],
                             continuousQueries = [], discreteQueries = [], transpose=False,
                             includeAllColumns=False, gzipResults = False):
        """
        Filters and exports data to a file. Similar to export_filter_results, but takes filters in the form of ContinuousQuery and DiscreteQuery objects,
        and has slightly less flexible functionality
        :param outFilePath: Name of the file that results will be saved to
        :param outFileType: string indicating what file format results will be saved to
        :param columns: list of columns to include in the output. If blank, all columns will be included.
        :param continuousQueries: list of ContinuousQuery objects representing queries on a column of continuous data
        :param discreteQueries: list of DiscreteQuery objects representing queries on a column of discrete data
        :param transpose: boolean when, if True, index and columns will be transposed in the output file
        :param includeAllColumns: boolean indicating whether to include all columns in the output. If True, overrides columnList
        :param gzipResults: boolean indicating whether the resulting file will be gzipped
        :return:
        """

        self.outputFile = SSFile.SSFile.factory(outFilePath, outFileType)
        query = self.__convert_queries_to_string(continuousQueries, discreteQueries)
        self.outputFile.export_filter_results(self.inputFile, columns=columns, query=query,
                              transpose=transpose, includeAllColumns=includeAllColumns, gzipResults=gzipResults)

    def get_filtered_samples(self, continuous_queries, discrete_queries):
        query = self.__convert_queries_to_string(continuous_queries, discrete_queries)
        df = self.inputFile._filter_data(query=query)
        return df.index.values

    def peek_by_column_names(self, listOfColumnNames, numRows=10, indexCol="Sample"):
        """
        Takes a look at a portion of the file by showing only the requested columns
        :param listOfColumnNames: List of columns that will be given in the output
        :param numRows: The number of rows that will be shown with the requested columns in the output
        :param indexCol:
        :return:
        """
        listOfColumnNames.insert(0, indexCol)
        df = self.inputFile.read_input_to_pandas(columnList=listOfColumnNames, indexCol = indexCol)
        #df = pd.read_parquet(self.inputFile.filePath, columns=listOfColumnNames)
        df.set_index(indexCol, drop=True, inplace=True)
        df = df[0:numRows]
        return df

    def merge_files(self, fileList, outFilePath, outFileType=None, gzipResults=False, on= None, how='inner'):
        """
        Merges multiple ShapeShifter-compatible files into a single file

        :param fileList: List of file paths representing files that will be merged
        :param outFilePath: File path where merged files will be stored
        :param outFileType: string representing the type of file that the merged file will be stored as
        :param gzipResults: If True, merged file will be gzipped
        :param on: Column or index level names to join on. These must be found in all files.
                    If on is None and not merging on indexes then this defaults to the intersection of the columns in all.
        """
        how=how.lower()
        if how not in ['left','right','outer','inner']:
            print("Error: \'How\' must one of the following options: left, right, outer, inner")
            return
        outFile = SSFile.SSFile.factory(outFilePath, outFileType)
        SSFileList=[]
        #create a file object for every file path passed in
        for file in fileList:
            SSFileList.append(SSFile.SSFile.factory(file))

        if len(SSFileList) < 1:
            print("Error: there must be at least one input file to merge with.")
            return

        SSFileList.insert(0,self.inputFile)
        df1 = SSFileList[0].read_input_to_pandas()

        #we keep track of how often a column name appears and will change the names to avoid duplicates if necessary
        #the exception is whatever columns they want to merge on - we don't keep track of those
        columnDict={}
        self.__increment_columnname_counters(columnDict, df1, on)
        if len(SSFileList) == 1:
            outFile.write_to_file(df1, gzipResults=gzipResults)
            return
        for i in range(0, len(SSFileList) - 1):
            df2 = SSFileList[i + 1].read_input_to_pandas()
            if on !=None:
                self.__increment_columnname_counters(columnDict, df2, on)
                self.__rename_common_columns(columnDict, df1, df2, on)
            #perform merge
            if on==None:
                df1 = pd.merge(df1, df2, how=how)
            else:
                df1=pd.merge(df1,df2, how=how, on=on)
        indexCol = list(df1.columns.values)[0]
        outFile.write_to_file(df1, gzipResults=gzipResults, indexCol=indexCol)
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
        Retrieves a specified column's name, data type, and all its unique values from a  file

        :type columnName: string
        :param columnName: the name of the column about which information is being obtained

        :type sizeLimit: int
        :param sizeLimit: limits the number of unique values returned to be no more than this number

        :return: Name, data type (continuous/discrete), and unique values from specified column
        :rtype: ColumnInfo object
        """
        import ColumnInfo
        columnList = [columnName]
        #df = pd.read_parquet(self.inputFile.filePath, columns=columnList)
        df = self.inputFile.read_input_to_pandas(columnList=columnList)

        uniqueValues = df[columnName].unique().tolist()
        # uniqueValues.remove(None)
        # Todo: There is probably a better way to do this...
        i = 0
        while uniqueValues[i] == None:
            i += 1
        if isinstance(uniqueValues[i], str) or isinstance(uniqueValues[i], bool):
            return ColumnInfo.ColumnInfo(columnName, "discrete", uniqueValues)
        else:
            return ColumnInfo.ColumnInfo(columnName, "continuous", uniqueValues)

    def get_all_columns_info(self):
        """
        Retrieves the column name, data type, and all unique values from every column in a file

        :return: Name, data type (continuous/discrete), and unique values from every column
        :rtype: dictionary where key: column name and value:ColumnInfo object containing the column name, data type (continuous/discrete), and unique values from all columns
        """
        # columnNames = getColumnNames(parquetFilePath)
        import ColumnInfo
        df = self.inputFile.read_input_to_pandas()
        columnDict = {}
        for col in df:
            uniqueValues = df[col].unique().tolist()
            i = 0
            while uniqueValues[i] == None:
                i += 1
            if isinstance(uniqueValues[i], str) or isinstance(uniqueValues[i], bool):
                columnDict[col] = ColumnInfo.ColumnInfo(col, "discrete", uniqueValues)
            else:
                columnDict[col] = ColumnInfo.ColumnInfo(col, "continuous", uniqueValues)
        return columnDict

    def __is_gzipped(self):
        """
        Function for internal use. Checks if a file is gzipped based on its extension
        """
        extensions = self.inputFile.filePath.rstrip("\n").split(".")
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
        import OperatorEnum
        if operator == OperatorEnum.OperatorEnum.Equals:
            return "=="
        elif operator == OperatorEnum.OperatorEnum.GreaterThan:
            return ">"
        elif operator == OperatorEnum.OperatorEnum.GreaterThanOrEqualTo:
            return ">="
        elif operator == OperatorEnum.OperatorEnum.LessThan:
            return "<"
        elif operator == OperatorEnum.OperatorEnum.LessThanOrEqualTo:
            return "<="
        elif operator == OperatorEnum.OperatorEnum.NotEquals:
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
        return self.inputFile.get_column_names()


    def peek(self, numRows=10, numCols=10):
        """
        Takes a look at the first few rows and columns of a parquet file and returns a pandas dataframe corresponding to the number of requested rows and columns

        :type numRows: int
        :param numRows: the number of rows the returned Pandas dataframe will contain

        :type numCols: int
        :param numCols: the number of columns the returned Pandas dataframe will contain

        :return: The first numRows and numCols in the given parquet file
        :rtype: Pandas dataframe
        """
        #TODO: Optimize peek for every file type by writing a get_column_names() function for every file type
        # allCols = self.get_column_names()

        # selectedCols = []
        # selectedCols.append(self.index)
        # for i in range(0, numCols):
        #      selectedCols.append(allCols[i])
        # df = self.inputFile.read_input_to_pandas(columnList=selectedCols)
        # df.set_index(self.index, drop=True, inplace=True)

        df=self.inputFile.read_input_to_pandas()
        if (numCols > len(df.columns)):
             numCols = len(df.columns)
        df = df.iloc[0:numRows, 0:numCols]
        return df


import pandas as pd