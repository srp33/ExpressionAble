import os
import tempfile
import pandas as pd

from files import SSFile


class HDF5File(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            df=pd.read_hdf(tempFile.name)
            os.remove(tempFile.name)
        else:
            df = pd.read_hdf(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def export_filter_results(self, inputSSFile, column_list=[], query=None, transpose=False, include_all_columns=False,
                              gzip_results=False, index_col="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, column_list, query, transpose,
                                                                        include_all_columns, df, includeIndex, index_col)

        self.write_to_file(df, gzip_results)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df.set_index(df.columns[0])
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            df.to_hdf(tempFile.name, "group", mode='w')
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            df.to_hdf(super()._remove_gz(self.filePath), "group", mode='w')
