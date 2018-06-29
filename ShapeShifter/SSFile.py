import pandas as pd




class SSFile:
    """
    Abstract base class for all the supported file types in ShapeShifter. Subclasses must implement reading a file to pandas and exporting a dataframe to the filetype
    """

    def __init__(self, filePath, fileType):
        self.filePath=filePath
        self.fileType=fileType


    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        """
        Reads from a file into a Pandas data frame. Must be implemented by subclasses
        :param columnList: List of string column names to be read in. If blank, all columns will be read in
        :param indexCol: String name of the column representing the index of the data set
        :return: Pandas data frame with the requested data
        """
        raise NotImplementedError("This method should have been implemented, but has not been")

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        """
        Filters and then exports data to a file
        :param inputSSFile: SSFile object representing the file to be read and filtered
        :param gzippedInput: boolean indicating if the inputSSFile is gzipped
        :param columnList: list of columns to include in the output. If blank, all columns will be included.
        :param query: string representing the query or filter to apply to the data set
        :param transpose: boolean indicating whether the results will be transposed
        :param includeAllColumns: boolean indicating whether to include all columns in the output. If True, overrides columnList
        :param gzipResults: boolean indicating whether the resulting file will be gzipped
        :param indexCol: string name of the index column of the data set
        """
        raise NotImplementedError("This method should have been implemented, but has not been")

    def _prep_for_export(self, inputSSFile, gzippedInput, columnList, query, transpose, includeAllColumns, df, includeIndex, indexCol):
        """
        Prepares a file to be exported by checking query syntax, unzipping the input, filtering, and transposing the data. This function is used
        in every file type's export_filter_results function with the exception of SQLiteFile

        :param inputSSFile: SSFile containing the data to be filtered
        :param gzippedInput: boolean indicating if inputSSFile is gzipped
        :param columnList: list of column names to be included in the output. If the list is empty, all columns will be included
        :param query: string representing the query or filter to be applied to the data set
        :param transpose: boolean indicating if the resulting data should be transposed
        :param includeAllColumns: boolean indicating to include all columns in the output. If True, it overrides columnList
        :param df: the Pandas data frame that will contain the results of the filters
        :param includeIndex: boolean that will store whether or not the output file will include the index column
        :param indexCol: string representing the name of the index column of the data set
        :return: updated query, inputSSFile, df, includeIndex. These updated values will be used by export_filter_results
        """

        #this function is used for every file's export_filter_results except SQLite
        if query != None:
            query = self._translate_null_query(query)
        if gzippedInput:
            inputSSFile.filePath=gzip.open(inputSSFile.filePath)
        df = inputSSFile._filter_data(columnList=columnList, query=query,
                                      includeAllColumns=includeAllColumns, indexCol=indexCol)
        if transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
            df = df.transpose()
            includeIndex = True
        return query, inputSSFile, df, includeIndex

    def factory(filePath, type):
        """
        Constructs the appropriate subclass object based on the type of file passed in
        :param filePath: string representing a file's path
        :param type: FileTypeEnum representing the type of file
        :return: SSFile subclass object
        """
        if type == FileTypeEnum.FileTypeEnum.Parquet: return ParquetFile.ParquetFile(filePath, type)
        elif type == FileTypeEnum.FileTypeEnum.TSV: return TSVFile.TSVFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.CSV: return CSVFile.CSVFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.JSON: return JSONFile.JSONFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.Excel: return ExcelFile.ExcelFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.HDF5: return HDF5File.HDF5File(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.Pickle: return PickleFile.PickleFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.MsgPack: return MsgPackFile.MsgPackFile(filePath, type)
        elif type == FileTypeEnum.FileTypeEnum.Stata: return StataFile.StataFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.SQLite: return SQLiteFile.SQLiteFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.HTML: return HTMLFile.HTMLFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.ARFF: return ARFFFile.ARFFFile(filePath,type)
        elif type == FileTypeEnum.FileTypeEnum.GCT: return GCTFile.GCTFile(filePath,type)
    factory=staticmethod(factory)

    def _filter_data(self, columnList=[], query=None,
                     includeAllColumns=False, indexCol="Sample"):
        """
        Filters a data set down according to queries and requested columns
        :param columnList: List of string column names to include in the results. If blank, all columns will be included
        :param query: String representing a query to be applied to the data set
        :param includeAllColumns: boolean indicating whether all columns should be included. If true, overrides columnList
        :param indexCol: string representing the name of the index column of the data set
        :return: filtered Pandas data frame
        """

        if includeAllColumns:
            columnList = []
            df = self.read_input_to_pandas(columnList, indexCol)
            self.__report_if_missing_columns(df, [indexCol])
            df = self.__replace_index(df, indexCol)
            if query != None:
                df = df.query(query)
            return df

        if len(columnList) == 0 and query == None:
            df = self.read_input_to_pandas(columnList, indexCol)
            self.__report_if_missing_columns(df, [indexCol])
            df = self.__replace_index(df, indexCol)
            return df

        if query != None:
            columnNamesFromQuery = self.__parse_column_names_from_query(query)
            columnList = columnNamesFromQuery + columnList
        if indexCol not in columnList:
            columnList.insert(0, indexCol)
        else:
            columnList.insert(0, columnList.pop(columnList.index(indexCol)))
        df = self.read_input_to_pandas(columnList, indexCol)
        self.__report_if_missing_columns(df, columnList)
        if query != None:
            df = df.query(query)

        return df

    def _translate_null_query(self, query):
        """
        For internal use only. Because pandas does not support querying for null values by "columnname == None", this function translates such queries into valid syntax
        """
        regex1 = r"\S*\s*!=\s*None\s*"
        regex2 = r"\S*\s*==\s*None\s*"
        matchlist1 = re.findall(regex1, query, flags=0)
        matchlist2 = re.findall(regex2, query, flags=0)
        for match in matchlist1:
            col = match.split("!=")[0].rstrip()
            query = query.replace(match, col + "==" + col + " ")
        for match in matchlist2:
            col = match.split("==")[0].rstrip()
            query = query.replace(match, col + "!=" + col + " ")
        return query

    def get_column_names(self) -> list:
        """
        Retrieves all column names from a data set stored in a parquet file

        :return: All column names
        :rtype: list
        """
        import pyarrow.parquet as pq
        p = pq.ParquetFile(self.filePath)
        columnNames = p.schema.names
        # delete 'Sample' from schema
        del columnNames[0]

        # delete extraneous other schema that the parquet file tacks on at the end
        if '__index_level_' in columnNames[len(columnNames) - 1]:
            del columnNames[len(columnNames) - 1]
        if 'Unnamed:' in columnNames[len(columnNames) - 1]:
            del columnNames[len(columnNames) - 1]
        return columnNames

    def __parse_column_names_from_query(self, query):
        """
        For internal use. Takes a query and determines what columns are being queried on
        """
        query = re.sub(r'\band\b', '&', query)
        query = re.sub(r'\bor\b', '|', query)
        args = re.split('==|<=|>=|!=|<|>|\&|\|', query)
        colList = []
        for arg in args:
            # first remove all whitespace and parentheses and brackets
            arg = arg.strip()
            arg = arg.replace("(", "")
            arg = arg.replace(")", "")
            arg = arg.replace("[", "")
            arg = arg.replace("]", "")
            # if it is a number, it isn't a column name
            try:
                float(arg)
            except:
                # check if the string is surrounded by quotes. If so, it is not a column name
                if len(arg) > 0 and arg[0] != "'" and arg[0] != '"':
                    # check for duplicates
                    if arg not in colList and arg != "True" and arg != "False":
                        colList.append(arg)
        return colList


    def __check_if_columns_exist(self, df, columnList):
        """
        For internal use. Checks to see if certain columns are found in a data frame
        :param df: Pandas data frame to be examined
        :param columnList: List of string column names to be checked
        :return: A list of string column names representing columns that were not found
        """
        missingColumns = []
        for column in columnList:
            if column not in df.columns:
                missingColumns.append(column)
        # if len(missingColumns)>0:
        #	raise ColumnNotFoundError(missingColumns)
        return missingColumns


    def _append_gz(self, outFilePath):
        """
        For internal use. If a file is to be gzipped, this function appends '.gz' to the filepath if necessary.
        """
        if not (outFilePath[len(outFilePath) - 3] == '.' and outFilePath[len(outFilePath) - 2] == 'g' and outFilePath[
            len(outFilePath) - 1] == 'z'):
            outFilePath += '.gz'
        return outFilePath

    def _compress_results(self, outFilePath):
        """
        For internal use. Manually gzips result files if Pandas does not inherently do so for the given file type.
        """
        with open(outFilePath, 'rb') as f_in:
            with gzip.open(self._append_gz(outFilePath), 'wb') as f_out:
                f_out.writelines(f_in)
        os.remove(outFilePath)

    def __replace_index(selfs, df, indexCol):
        """
        For internal use. If the user requests a certain column be the index, this function puts that column as the first in the data frame df
        """
        if indexCol in df.columns:
            df.set_index(indexCol, drop=True, inplace=True)
            df.reset_index(inplace=True)
        return df

    def __report_if_missing_columns(self,df, columnList):
        """
        Prints out a warning showing which of the columns in the given columnList are not found in the data frame df
        """
        missingColumns = self.__check_if_columns_exist(df, columnList)
        if len(missingColumns) > 0:
            print("Warning: the following columns were not found and therefore not included in output: " + ", ".join(
                missingColumns))


import ConvertARFF
import ConvertGCT
import gzip
import time
import os
import re
import ARFFFile
import CSVFile
import ExcelFile
import FileTypeEnum
import GCTFile
import HDF5File
import HTMLFile
import JSONFile
import MsgPackFile
import ParquetFile
import PickleFile
import SQLiteFile
import StataFile
import TSVFile