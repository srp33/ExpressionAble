import pandas as pd

from SSFile import SSFile


class StataFile(SSFile):

    def read_input_to_pandas(self, columnList, indexCol):
        if len(columnList) > 0:
            return pd.read_stata(self.filePath, columns=columnList)
        return pd.read_stata(self.filePath)

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
        df.to_stata(self.filePath, write_index=True)
        if gzipResults:
            super().__compress_results(self.filePath)