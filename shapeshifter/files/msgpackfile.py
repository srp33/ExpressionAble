import gzip
import tempfile

import pandas as pd

from files import SSFile


class MsgPackFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            with gzip.open(self.filePath) as path:
                df=pd.read_msgpack(path)
        else:
            df = pd.read_msgpack(self.filePath)
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
            tempFile =tempfile.NamedTemporaryFile(delete=False)
            df.to_msgpack(tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            df.to_msgpack(super()._remove_gz(self.filePath))
