

import SSFile


class ShapeShifter:

    def __init__(self, filePath, fileType,  filters=None, columns=[], index='Sample'):
        self.inputFile = SSFile.SSFile.factory(filePath, fileType)
        self.gzippedInput= self.__is_gzipped()
        self.filters=filters
        self.columns = columns
        self.index = index
        self.outputFile= None

    def export_filter_results(self, outFilePath, outFileType,  transpose=False, includeAllColumns=False, gzipResults=False):
        self.outputFile = SSFile.SSFile.factory(outFilePath,outFileType)
        self.outputFile.export_filter_results(self.inputFile, gzippedInput=self.gzippedInput, columnList=self.columns, query=self.filters, transpose=transpose, includeAllColumns=includeAllColumns, gzipResults=gzipResults, indexCol=self.index)

    def __is_gzipped(self):
        extensions = self.inputFile.filePath.rstrip("\n").split(".")
        if extensions[len(extensions) - 1] == 'gz':
            return True
        return False



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
        import pyarrow.parquet as pq
        p = pq.ParquetFile(self.inputFile.filePath)
        columnNames = p.schema.names
        # delete 'Sample' from schema
        del columnNames[0]

        # delete extraneous other schema that the parquet file tacks on at the end
        if '__index_level_' in columnNames[len(columnNames) - 1]:
            del columnNames[len(columnNames) - 1]
        if 'Unnamed:' in columnNames[len(columnNames) - 1]:
            del columnNames[len(columnNames) - 1]
        return columnNames


    def peek(self, numRows=10, numCols=10):
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
        allCols = self.get_column_names()
        if (numCols > len(allCols)):
            numCols = len(allCols)
        selectedCols = []
        selectedCols.append(self.index)
        for i in range(0, numCols):
            selectedCols.append(allCols[i])
        df = pd.read_parquet(self.inputFile.filePath, columns=selectedCols)
        df.set_index(self.index, drop=True, inplace=True)
        df = df.iloc[0:numRows, 0:numCols]
        return df

    def peek_by_column_names(self, listOfColumnNames, numRows=10):
        """
        Peeks into a parquet file by looking at a specific set of columns

        :type parquetFilePath: string
        :param parquetFilePath: filepath to a parquet file to be examined

        :type numRows: int
        :param numRows: the number of rows the returned Pandas dataframe will contain

        :return: The first numRows of all the listed columns in the given parquet file
        :rtype: Pandas dataframe

        """
        listOfColumnNames.insert(0, self.index)
        df = pd.read_parquet(self.inputFile.filePath, columns=listOfColumnNames)
        df.set_index(self.index, drop=True, inplace=True)
        df = df[0:numRows]
        return df

    def get_column_info(self, columnName: str, sizeLimit: int = None):
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
        import ColumnInfo
        columnList = [columnName]
        df = pd.read_parquet(self.inputFile.filePath, columns=columnList)

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

        :type parquetFilePath: string
        :param parquetFilePath: filepath to a parquet file to be examined

        :return: Name, data type (continuous/discrete), and unique values from every column
        :rtype: dictionary where key: column name and value:ColumnInfo object containing the column name, data type (continuous/discrete), and unique values from all columns
        """
        # columnNames = getColumnNames(parquetFilePath)
        import ColumnInfo
        df = pd.read_parquet(self.inputFile.filePath)
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

import pandas as pd