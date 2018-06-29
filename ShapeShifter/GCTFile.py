from SSFile import SSFile
from ConvertGCT import toGCT
from ConvertGCT import gctToPandas

class GCTFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = gctToPandas(self.filePath)
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
        toGCT(df, self.filePath)
        if gzipResults:
            super()._compress_results(self.filePath)