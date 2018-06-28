import pandas as pd

from SSFile import SSFile


class ParquetFile(SSFile):
    def __init__(self, filePath, fileType):
        super().__init__(filePath,fileType)

    def read_input_to_pandas(self, columnList, indexCol):
        if len(columnList) == 0:
            df = pd.read_parquet(self.filePath)
        else:
            df = pd.read_parquet(self.filePath, columns=columnList)
        if df.index.name == indexCol:
            df.reset_index(inplace=True)
        return df

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
        if gzipResults:
            df.to_parquet(super().__append_gz(self.filePath), compression='gzip')
        else:
            df.to_parquet(self.filePath)

import gzip