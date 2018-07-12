import SSFile

class ShapeShifter:

    def __init__(self, filePath, fileType=None):
        """
        Creates a ShapeShifter object
        :param filePath: string name of a file path to read and perform operations on
        :param fileType: FileTypeEnum indicating the type of file that is being read
        :param index:
        """
        if fileType==None:
            self.inputFile=SSFile.SSFile.factory(filePath, self.__determine_extension(filePath))
        else:
            self.inputFile = SSFile.SSFile.factory(filePath, fileType)
        self.gzippedInput= self.__is_gzipped()
        self.outputFile= None

    def export_filter_results(self, outFilePath, outFileType, filters=None, columns=[],
                              transpose=False, includeAllColumns=False, gzipResults=False, index='Sample'):
        """
        Filters and then exports data to a file
        :param outFilePath: Name of the file that results will be saved to
        :param outFileType: string indicating what file format results will be saved to
        :param filters: string representing the query or filter to apply to the data set
        :param columns: list of columns to include in the output. If blank, all columns will be included.
        :param transpose: boolean when, if True, index and columns will be transposed in the output file
        :param includeAllColumns: boolean indicating whether to include all columns in the output. If True, overrides columnList
        :param gzipResults: boolean indicating whether the resulting file will be gzipped
        :return:
        """
        #todo: perhaps not require the outFileType to be specified; infer it from outFilePath?
        self.outputFile = SSFile.SSFile.factory(outFilePath,outFileType)
        self.outputFile.export_filter_results(self.inputFile, columnList=columns, query=filters, transpose=transpose,
                                              includeAllColumns=includeAllColumns, gzipResults=gzipResults,
                                              indexCol=index)

    def export_query_results(self, outFilePath, outFileType, columns: list = [],
                             continuousQueries: list = [], discreteQueries: list = [], transpose=False,
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

    def merge_files(self, fileList, outFilePath, outFileType, gzipResults=False, on= None):
        """
        Merges multiple ShapeShifter-compatible files into a single file

        :param fileList: List of file paths representing files that will be merged
        :param outFilePath: File path where merged files will be stored
        :param outFileType: string representing the type of file that the merged file will be stored as
        :param gzipResults: If True, merged file will be gzipped
        :param on: Column or index level names to join on. These must be found in all files.
                    If on is None and not merging on indexes then this defaults to the intersection of the columns in all.
        """
        outFile = SSFile.SSFile.factory(outFilePath, outFileType)

        SSFileList=[]


        #create a file object for every file path passed in
        for file in fileList:
            SSFileList.append(SSFile.SSFile.factory(file,self.__determine_extension(file)))
        #SSFileList.insert(0,self.inputFile) #This is if we include the base file

        if len(SSFileList) < 1:
            print("Error: there must be at least one input file to merge.")
            return

        df1 = SSFileList[0].read_input_to_pandas()

        if len(SSFileList) == 1:
            outFile.write_to_file(df1, gzipResults=gzipResults)
            return
        for i in range(0, len(SSFileList) - 1):
            df2 = SSFileList[i + 1].read_input_to_pandas()
            if on==None:
                df1 = pd.merge(df1, df2, how='inner')
            else:
                df1=pd.merge(df1,df2, how='inner', on=on)
        outFile.write_to_file(df1, gzipResults=gzipResults)


        return
    #merge_files=staticmethod(merge_files)


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

    def __determine_extension(self,fileName):
        extensions = fileName.rstrip("\n").split(".")
        if len(extensions) > 1:
            extension = extensions[len(extensions) - 1]
            if extension == 'gz':
                extension = extensions[len(extensions) - 2]
        else:
            extension = None
        if extension == "tsv" or extension == "txt":
            return 'tsv'
        elif extension == "csv":
            return 'csv'
        elif extension == "json":
            return 'json'
        elif extension == "xlsx":
            return 'excel'
        elif extension == "hdf" or extension == "h5":
            return 'hdf5'
        # elif extension=="feather":
        #	return FileTypeEnum.Feather
        elif extension == "pq":
            return 'parquet'
        elif extension == "mp":
            return 'msgpack'
        elif extension == "dta":
            return 'stata'
        elif extension == "pkl":
            return 'pickle'
        elif extension == "html":
            return 'html'
        elif extension == "db":
            return 'sqlite'
        elif extension == "arff":
            return 'arff'
        elif extension == "gct":
            return 'gct'
        else:
            #todo: this should throw an actual error
            print(
                "Error: Extension on " + fileName + " not recognized. Please use appropriate file extensions or explicitly specify file type using the -i or -o flags")



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