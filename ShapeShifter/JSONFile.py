import pandas as pd

from SSFile import SSFile


class JSONFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = pd.read_json(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            columnList[columnList.index(indexCol)] = 'index'
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
            df = df.set_index(indexCol, drop=True) if indexCol in df.columns else df
        self.write_to_file(df, gzipResults)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        if gzipResults:
            outFilePath = super()._append_gz(self.filePath)
            df.to_json(path_or_buf=outFilePath, compression='gzip')
        else:
            df.to_json(path_or_buf=self.filePath)