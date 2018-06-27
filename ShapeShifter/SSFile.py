import pandas as pd




class SSFile:
    """
    Abstract base class for all the supported file types in ShapeShifter. Subclasses must implement reading a file to pandas and exporting a dataframe to the filetype
    """

    def __init__(self, filePath, fileType):
        self.filePath=filePath
        self.fileType=fileType


    def read_input_to_pandas(self, columnList, indexCol):
        raise NotImplementedError("This method should have been implemented, but has not been")

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        raise NotImplementedError("This method should have been implemented, but has not been")

    def prep_for_export(self, inputSSFile, gzippedInput, columnList, query, transpose, includeAllColumns, df, includeIndex, indexCol):
        #this function is used for every file's export_filter_results except SQLite
        if query != None:
            query = self.__translate_null_query(query)
        if gzippedInput:
            inputSSFile.filePath=gzip.open(inputSSFile.filePath)
        df = inputSSFile.__filter_data(columnList=columnList, query=query,
                                       includeAllColumns=includeAllColumns, indexCol=indexCol)
        if transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
            df = df.transpose()
            includeIndex = True
        return query, inputSSFile, df, includeIndex

    def factory( filePath, type):
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

    def __translate_null_query(self, query):
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
        Retrieves all column names from a dataset stored in a parquet file
        :type parquetFilePath: string
        :param parquetFilePath: filepath to a parquet file to be examined

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
        missingColumns = []
        for column in columnList:
            if column not in df.columns:
                missingColumns.append(column)
        # if len(missingColumns)>0:
        #	raise ColumnNotFoundError(missingColumns)
        return missingColumns

    def __filter_data(self, columnList=[], query=None,
                      includeAllColumns=False, indexCol="Sample"):
        """
        Applies a filter to a parquet dataset. If no filter or columns are passed in, it returns the entire dataset as a pandas dataframe. Otherwise, returns only the filtered data over the requested columns as a Pandas dataframe

        :type inputFilePath: string
        :param inputFilePath: filepath to a parquet file to be filtered

        :type columnList: list of strings
        :param columnList: list of column names that will be included in the data resulting from the filter

        :type query: string
        :param query: filter to apply to the dataset, written using python logical syntax

        :type includeAllColumns: bool
        :param includeAllColumns: if True, will include all columns in the filtered dataset. Overrides columnList if True
        """
        t1 = time.time()
        if len(columnList) == 0 and query == None:
            df = self.read_input_to_pandas(columnList, indexCol)
            t2 = time.time()
            print("Time to read to pandas: " + str(t2 - t1))
            if indexCol in df.columns:
                df.set_index(indexCol, drop=True, inplace=True)
                df.reset_index(inplace=True)
            return df
        elif len(columnList) > 0 and query == None and not includeAllColumns:
            if indexCol not in columnList:
                columnList.insert(0, indexCol)
            else:
                columnList.insert(0, columnList.pop(columnList.index(indexCol)))
            df = self.read_input_to_pandas(columnList, indexCol)
            t2 = time.time()
            print("Time to read to pandas: " + str(t2 - t1))
            return df
        if includeAllColumns:
            columnList = self.get_column_names()
        else:
            queryColumns = self.__parse_column_names_from_query(query)
            columnList = queryColumns + columnList
        if indexCol not in columnList:
            columnList.insert(0, indexCol)
        else:
            columnList.insert(0, columnList.pop(columnList.index(indexCol)))
        df = self.read_input_to_pandas(columnList, indexCol)
        t2 = time.time()
        print("Time to read to pandas: " + str(t2 - t1))
        missingColumns = self.__check_if_columns_exist(df, columnList)
        t3 = time.time()
        df = df.query(query)
        t4 = time.time()
        print("Time to filter: " + str(t4 - t3))
        if len(missingColumns) > 0:
            print("Warning: the following columns were not found and therefore not included in output: " + ", ".join(
                missingColumns))
        return df

    def __append_gz(self, outFilePath):
        if not (outFilePath[len(outFilePath) - 3] == '.' and outFilePath[len(outFilePath) - 2] == 'g' and outFilePath[
            len(outFilePath) - 1] == 'z'):
            outFilePath += '.gz'
        return outFilePath

    def __compress_results(self, outFilePath):
        # file name in and out must be different, I can't compress a file without changing its filepath as well
        with open(outFilePath, 'rb') as f_in:
            with gzip.open(self.__append_gz(outFilePath), 'wb') as f_out:
                f_out.writelines(f_in)
        os.remove(outFilePath)


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