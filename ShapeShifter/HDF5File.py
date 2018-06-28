import pandas as pd

from SSFile import SSFile


class HDF5File(SSFile):

    def read_input_to_pandas(self, columnList, indexCol):
        df = pd.read_hdf(self.filePath)
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
        df.to_hdf(self.filePath, "group", mode='w')
        if gzipResults:
            super().__compress_results(self.filePath)