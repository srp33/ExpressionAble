import gzip

import pandas as pd

from SSFile import SSFile


class HDF5File(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            with gzip.open(self.filePath) as path:
                df=pd.read_hdf(path.read())
        else:
            df = pd.read_hdf(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def export_filter_results(self, inputSSFile, columnList=[], query=None, transpose=False, includeAllColumns=False,
                              gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, columnList, query, transpose,
                                                                        includeAllColumns, df, includeIndex, indexCol)
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
        self.write_to_file(df, gzipResults)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        df.to_hdf(super()._remove_gz(self.filePath), "group", mode='w')
        if gzipResults:
            super()._compress_results(self.filePath)