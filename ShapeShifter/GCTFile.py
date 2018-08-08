import tempfile

from ConvertGCT import gctToPandas
from ConvertGCT import toGCT
from SSFile import SSFile


class GCTFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        # probably don't need this section since GCT is read in pretty much the same way CSV is
        # if self.isGzipped:
        #     super()._gunzip()
        #     df= gctToPandas(super()._remove_gz(self.filePath))
        #     os.remove(super()._remove_gz(self.filePath))
        # else:
        df = gctToPandas(self.filePath)
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
        self.write_to_file(df, gzipResults)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            toGCT(df, tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            toGCT(df, self._remove_gz(self.filePath))
