
from ConvertARFF import toARFF
from ConvertARFF import arffToPandas
from SSFile import SSFile


class ARFFFile(SSFile):

    def read_input_to_pandas(self, columnList, indexCol):
        df = arffToPandas(self.filePath)
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super().prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                       transpose, includeAllColumns, df, includeIndex,
                                                                       indexCol)

        toARFF(df, self.filePath)
        if gzipResults:
            super().__compress_results(self.filePath)
import gzip