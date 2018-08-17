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

        if query != None:
            query = self._translate_null_query(query)
        if not includeAllColumns and "NAME" not in columnList:
            columnList.append("NAME")
        df = inputSSFile._filter_data(columnList=columnList, query=query,
                                      includeAllColumns=includeAllColumns, indexCol=indexCol)

        if transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
            df = df.transpose()
            includeIndex = True
        #TODO: remove returning inputSSFile for every file type, it is no longer needed since gzip is taken care of elsewhere

        self.write_to_file(df, gzipResults)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        # if not transpose:
        #     df = df.set_index(indexCol) if indexCol in df.columns else df
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            toGCT(df, tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            toGCT(df, self._remove_gz(self.filePath))
