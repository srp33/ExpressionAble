import tempfile
from ..utils import to_gct, gct_to_pandas
from ..files import SSFile


class GCTFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        # probably don't need this section since GCT is read in pretty much the same way CSV is
        # if self.isGzipped:
        #     super()._gunzip()
        #     df= gctToPandas(super()._remove_gz(self.filePath))
        #     os.remove(super()._remove_gz(self.filePath))
        # else:
        if len(columnList) == 0:
            return gct_to_pandas(self.filePath)
        else:
            columnList.append("Description")
            return gct_to_pandas(self.filePath, columnList)
        # df = gctToPandas(self.filePath)
        # if len(columnList) > 0:
        #     df = df[columnList]
        # return df

    def export_filter_results(self, inputSSFile, column_list=[], query=None, transpose=False, include_all_columns=False,
                              gzip_results=False, index_col="Sample"):
        df = None
        includeIndex = False
        null = 'NA'

        if query != None:
            query = self._translate_null_query(query)
        if not include_all_columns and "NAME" not in column_list:
            column_list.append("NAME")
        df = inputSSFile._filter_data(columnList=column_list, query=query,
                                      includeAllColumns=include_all_columns, indexCol=index_col)

        if transpose:
            df = df.set_index(index_col) if index_col in df.columns else df
            df = df.transpose()
            includeIndex = True
        #TODO: remove returning inputSSFile for every file type, it is no longer needed since gzip is taken care of elsewhere

        self.write_to_file(df, gzip_results)


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        # if not transpose:
        #     df = df.set_index(indexCol) if indexCol in df.columns else df
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            to_gct(df, tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            to_gct(df, self._remove_gz(self.filePath))
