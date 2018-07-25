import pandas as pd

from SSFile import SSFile


class JSONFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = pd.read_json(self.filePath)
        df = df.reset_index()
        #todo: name the index column "Sample" instead of "index" and give a warning indicating that happened
        columns=columnList.copy()
        if len(columns) > 0:
            columns[columns.index(indexCol)] = 'index'
            df = df[columns]
        return df

    def export_filter_results(self, inputSSFile, columnList=[], query=None, transpose=False, includeAllColumns=False,
                              gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, columnList, query, transpose,
                                                                        includeAllColumns, df, includeIndex, indexCol)
        if not transpose:
            df = df.set_index(indexCol, drop=True) if indexCol in df.columns else df
        self.write_to_file(df, gzipResults)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        if gzipResults:
            outFilePath = super()._append_gz(self.filePath)
            df.to_json(path_or_buf=outFilePath, compression='gzip')
        else:
            df.to_json(path_or_buf=self.filePath)