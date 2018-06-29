import pandas as pd

from SSFile import SSFile


class PickleFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = pd.read_pickle(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
        if gzipResults:
            df.to_pickle(super()._append_gz(self.filePath), compression='gzip')
        else:
            df.to_pickle(self.filePath)