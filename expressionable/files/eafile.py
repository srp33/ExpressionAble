import gzip
import os
import re
import shutil
import tempfile

from ..errors import ColumnNotFoundError


class EAFile:
    """
    Abstract base class for all the supported file types in ExpressionAble. Subclasses must implement reading a file to pandas and exporting a dataframe to the data type.
    """


    def __init__(self, filePath, fileType):
        self.filePath=filePath
        self.dataType=fileType
        self.isGzipped= self.__is_gzipped()


    def read_input_to_pandas(self, columnList=[], indexCol=None):
        """
        Reads from a file into a Pandas DataFrame. File may be gzipped. Must be implemented by subclasses.

        :type columnList: list of str, default []
        :param columnList: Names of columns to be read in. If the list is empty, all columns will be read in.

        :type indexCol: str, default None
        :param indexCol: Name of the column representing the index of the data set.

        :return: Pandas data frame with the requested data.
        """
        raise NotImplementedError("Reading from this file type is not currently supported.")

    def export_filter_results(self, inputEAFile, column_list=[], query=None, transpose=False, include_all_columns=False,
                              gzip_results=False, index_col=None):
        """
        Filters and then exports data to a file.

        :type inputEAFile: EAFile
        :param inputEAFile: Object representing the file to be read and filtered.

        :type column_list: list of str, default []
        :param column_list: Names of columns to include in the output. If blank, all columns will be included.

        :type query: str, default None
        :param query: Query or filter to apply to the data set, written using Python logical syntax.

        :type transpose: bool, default False
        :param transpose:  If True, index and columns will be transposed in the output file.

        :type include_all_columns: bool, default False
        :param include_all_columns: Indicates whether to include all columns in the output. If True, overrides columnList.

        :type gzip_results: bool, default false
        :param gzip_results:  Indicates whether the resulting file will be gzipped.

        :type index_col: str, default None
        :param index_col: string name of the column that will be set as the index of the data. If None, it will not set an index.

        :return: None
        """
        df = None
        includeIndex = False
        null = 'NA'
        query, inputEAFile, df, includeIndex = self._prep_for_export(inputEAFile, column_list, query, transpose,
                                                                     include_all_columns, df, includeIndex, index_col)
        self.write_to_file(df, gzip_results, includeIndex, null)

    def _prep_for_export(self, inputEAFile, columnList, query, transpose, includeAllColumns, df, includeIndex,
                         indexCol):
        """
        Prepares a file to be exported by checking query syntax, unzipping the input, filtering, and transposing the data. This function is used
        in every file type's export_filter_results function with the exception of SQLiteFile

        :param inputEAFile: EAFile containing the data to be filtered
        :param columnList: list of column names to be included in the output. If the list is empty, all columns will be included
        :param query: string representing the query or filter to be applied to the data set
        :param transpose: boolean indicating if the resulting data should be transposed
        :param includeAllColumns: boolean indicating to include all columns in the output. If True, it overrides columnList
        :param df: the Pandas data frame that will contain the results of the filters
        :param includeIndex: boolean that will store whether or not the output file will include the index column
        :param indexCol: string representing the name of the index column of the data set
        :return: updated query, inputSSFile, df, includeIndex. These updated values will be used by export_filter_results
        """

        if query != None:
            query = self._translate_null_query(query)

        df = inputEAFile._filter_data(columnList=columnList, query=query,
                                      includeAllColumns=includeAllColumns, indexCol=indexCol)

        if transpose:
            #df = df.set_index(indexCol) if indexCol in df.columns else df
            df = df.set_index(indexCol) if (indexCol != None and indexCol in df.columns) else df.set_index(list(df)[0])
            df = df.transpose()
            includeIndex = True
        #TODO: remove returning inputEAFile for every file type, it is no longer needed since gzip is taken care of elsewhere
        return query, inputEAFile, df, includeIndex

    def factory(filePath, type=None):
        """
        Constructs the appropriate subclass object based on the type of file passed in.

        :type filePath: str
        :param filePath: Path to a file.

        :type type: str, default None
        :param type: Name of the type of file. If None, type will be inferred from filePath.

        :return: Appropriate EAFile subclass object.
        """
        from ..files import ARFFFile, CSVFile, ExcelFile, GCTFile, HDF5File, HTMLFile, JSONFile, MsgPackFile, ParquetFile
        from ..files import PickleFile, SQLiteFile, StataFile, TSVFile, JupyterNBFile, RMarkdownFile, KallistoTPMFile
        from ..files import KallistoEstCountsFile, SalmonTPMFile, SalmonNumReadsFile, StarReadsFile, GEOFile, GCTXFile, PDFFile

        if type==None:
            type = EAFile.__determine_extension(filePath)

        if type.lower() == 'parquet': return ParquetFile(filePath, type)
        elif type.lower() == 'tsv': return TSVFile(filePath, type)
        elif type.lower() == 'csv': return CSVFile(filePath, type)
        elif type.lower() == 'json': return JSONFile(filePath, type)
        elif type.lower() == 'excel': return ExcelFile(filePath, type)
        elif type.lower() == 'hdf5': return HDF5File(filePath, type)
        elif type.lower() == 'pickle': return PickleFile(filePath, type)
        elif type.lower() == 'msgpack': return MsgPackFile(filePath, type)
        elif type.lower() == 'stata': return StataFile(filePath, type)
        elif type.lower() == 'sqlite': return SQLiteFile(filePath, type)
        elif type.lower() == 'html': return HTMLFile(filePath, type)
        elif type.lower() == 'arff': return ARFFFile(filePath, type)
        elif type.lower() == 'gct': return GCTFile(filePath, type)
        elif type.lower() == 'jupyternotebook': return JupyterNBFile(filePath, type)
        elif type.lower() == 'rmarkdown': return RMarkdownFile(filePath, type)
        elif type.lower() == 'kallistotpm': return KallistoTPMFile(filePath, type)
        elif type.lower() == 'kallisto_est_counts': return KallistoEstCountsFile(filePath, type)
        elif type.lower() == 'salmontpm': return SalmonTPMFile(filePath, type)
        elif type.lower() == 'salmonnumreads': return SalmonNumReadsFile(filePath, type)
        elif type.lower() == 'geo': return GEOFile(filePath,type)
        elif type.lower() == 'pdf': return  PDFFile(filePath, type)
        elif type.lower() == 'starreads': return StarReadsFile(filePath, type)
        elif type.lower() == 'gctx': return GCTXFile(filePath, type)
        else:
            raise Exception("File type not recognized. Supported file types include: TSV, CSV, Parquet, JSON, Excel, "
                            "HDF5, Pickle, MsgPack, Stata, SQLite, HTML, ARFF, GCT, GCTX, PDF JupyterNotebook, RMarkdown,"
                            " KallistoTPM, Kallisto_est_counts, SalmonTPM, StarReads and SalmonNumReads")
    factory=staticmethod(factory)

    def __determine_extension(fileName):
        """
        Determines the file type of a given file based off its extension
        :param fileName: Name of a file whose extension will be examined
        :return: string representing the file type indicated by the file's extension
        """
        extensions = fileName.rstrip("\n").split(".")
        if len(extensions) > 1:
            extension = extensions[len(extensions) - 1]
            if extension == 'gz':
                extension = extensions[len(extensions) - 2]
        else:
            extension = None
        if extension == "tsv":
            return 'tsv'
        elif extension == "csv":
            return 'csv'
        elif extension == "json":
            return 'json'
        elif extension == "xlsx":
            return 'excel'
        elif extension == "hdf" or extension == "h5":
            return 'hdf5'
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
        elif extension == "pdf":
            return 'pdf'
        elif extension == "gctx":
            return 'gctx'
        elif extension == "ipynb":
            return 'jupyternotebook'
        elif extension == "rmd":
            return 'rmarkdown'
        elif EAFile.__is_geo_filepath(fileName):
            return 'geo'
        else:
            raise Exception("Error: Extension on " + fileName + " not recognized. Please use appropriate file extensions or explicitly specify file type.")

    __determine_extension = staticmethod(__determine_extension)

    def __is_geo_filepath(file_name):
        #Case 1: using a GEO id that starts with GSE and ends with a number
        if file_name.startswith("GSE") and file_name[3:].isdigit():
            return True
        if "ftp.ncbi.nlm.nih.gov/geo/series/" in file_name and file_name.endswith(".txt.gz"):
            return True
        return False

    __is_geo_filepath = staticmethod(__is_geo_filepath)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        """
        Writes a Pandas DataFrame to a file.

        :type df: Pandas DataFrame
        :param df: Pandas DataFrame object that contains the data that will be written to a file.

        :type gzipResults: bool, default False
        :param gzipResults: Indicates whether the resulting file will be gzipped.

        :type includeIndex: bool, default False
        :param includeIndex: Indicates whether the index column should be written to the file.

        :type null: str, default 'NA'
        :param null: Indicates how null or None values should be represented in the output file.

        :type indexCol: str, default None
        :param indexCol: Name of the column that will be the index in the output file.

        :type transpose: bool, default False
        :param transpose: If True, index and columns will be transposed in the output file.
        """
        raise NotImplementedError("Writing to this file type is not currently supported.")

    def _update_index_col(self, df, indexCol="Sample"):
        """
        Function for internal use. If the given index column is not in the data frame, it will default to the first column name
        """
        if indexCol not in df.columns:
            return
    def __is_gzipped(self):
        """
        Function for internal use. Checks if a file is gzipped based on its extension
        """
        extensions = self.filePath.rstrip("\n").split(".")
        if extensions[len(extensions) - 1] == 'gz':
            return True
        return False

    def _filter_data(self, columnList=[], query=None,
                     includeAllColumns=False, indexCol=None):
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
           # self.__report_if_missing_columns(df, [indexCol])
            if indexCol != None:
                df = self.__replace_index(df, indexCol)
            if query != None:
                df = df.query(query)
            return df

        if len(columnList) == 0 and query == None:
            df = self.read_input_to_pandas(columnList, indexCol)
            #self.__report_if_missing_columns(df, [indexCol])
            if indexCol != None:
                df = self.__replace_index(df, indexCol)
            return df

        if query != None:
            columnNamesFromQuery = self.__parse_column_names_from_query(query)
            columnList = columnNamesFromQuery + columnList
        if indexCol != None and indexCol not in columnList:
            columnList.insert(0, indexCol)
        elif indexCol != None and indexCol in columnList:
            columnList.insert(0, columnList.pop(columnList.index(indexCol)))
        try:
            df = self.read_input_to_pandas(columnList, indexCol)
        except (ValueError, KeyError) as e:
            raise ColumnNotFoundError(str(e))
        self.__report_if_missing_columns(df, columnList)
        df = self.__replace_index(df, indexCol) if indexCol != None else df
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
        raise NotImplementedError("This method should have been implemented, but has not been")


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
        #check if any columns begin with a number
        for col in colList:
            if col[0].isdigit():
                raise Exception("Error: columns whose names begin with numbers cannot be queried on")
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
        For internal use. If a file is to be gzipped, this function appends '.gz' to the file path if necessary.
        """
        if not (outFilePath[len(outFilePath) - 3] == '.' and outFilePath[len(outFilePath) - 2] == 'g' and outFilePath[
            len(outFilePath) - 1] == 'z'):
            outFilePath += '.gz'
        return outFilePath

    def _remove_gz(self, outFilePath):
        """
        For internal use. If a file is to be gzipped, this function removes '.gz' to the file path if necessary.
        """
        if (outFilePath[len(outFilePath) - 3] == '.' and outFilePath[len(outFilePath) - 2] == 'g' and outFilePath[
            len(outFilePath) - 1] == 'z'):
            outFilePath=outFilePath[:-3]
        return outFilePath

    def _gzip_results(self, tempFilePath, outFilePath):
        """
        For internal use. Manually gzips result files if Pandas does not inherently do so for the given file type.
        """
        with open(tempFilePath, 'rb') as f_in:
            with gzip.open(self._append_gz(outFilePath), 'wb') as f_out:
                #f_out.writelines(f_in)
                shutil.copyfileobj(f_in,f_out)
        os.remove(tempFilePath)

    def _gunzip_to_temp_file(self):
        """
        Takes a gzipped file with extension 'gz' and unzips it to a temporary file location so it can be read into pandas
        """
        with gzip.open(self.filePath, 'rb') as f_in:
            # with open(self._remove_gz(self.filePath), 'wb') as f_out:
            #     shutil.copyfileobj(f_in, f_out)
            f_out=tempfile.NamedTemporaryFile(delete=False)
            shutil.copyfileobj(f_in, f_out)
            f_out.close()
        return f_out
    def __replace_index(selfs, df, indexCol):
        """
        For internal use. If the user requests a certain column be the index, this function puts that column as the first in the data frame df
        """
        if indexCol in df.columns:
            df.set_index(indexCol, drop=True, inplace=True)
            df.reset_index(inplace=True)
        else:
            print("Warning: the column " + "\"" +indexCol+"\" you requested as the index is not found and therefore not"
                                                          " included in the output.")
        return df

    def __report_if_missing_columns(self,df, columnList):
        """
        Prints out a warning showing which of the columns in the given columnList are not found in the data frame df
        """
        missingColumns = self.__check_if_columns_exist(df, columnList)
        if len(missingColumns) > 0:
            print("Warning: the following columns were not found and therefore not included in output: " + ", ".join(
                missingColumns))
