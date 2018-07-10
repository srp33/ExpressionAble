import os

from ConvertARFF import arffToPandas
from ConvertARFF import toARFF
from SSFile import SSFile


class ARFFFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            super()._gunzip()
            df= arffToPandas(super()._remove_gz(self.filePath))
            os.remove(super()._remove_gz(self.filePath))
        else:
            df = arffToPandas(self.filePath)
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

        self.write_to_file(df, gzipResults, includeIndex, null)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        toARFF(df, super()._remove_gz(self.filePath))
        if gzipResults:
            super()._compress_results(self.filePath)


