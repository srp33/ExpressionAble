import pandas as pd

from ..files import EAFile


class JSONFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        df = pd.read_json(self.filePath)
        df = df.reset_index()
        #todo: name the index column "Sample" instead of "index" and give a warning indicating that happened
        columns=columnList.copy()

        if indexCol != None:
            if len(columns) > 0:
                columns[columns.index(indexCol)] = 'index'
                df = df[columns]
            df.rename(columns={'index':indexCol}, inplace=True)
        else:
            df.rename(columns={'index': 'Sample'}, inplace=True)

        if len(columnList) > 0:
            df = df[columnList]
        return df


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        if not transpose:
            df = df.set_index(indexCol, drop=True) if indexCol in df.columns else df.set_index(df.columns[0], drop=True)
        if gzipResults:
            outFilePath = super()._append_gz(self.filePath)
            df.to_json(path_or_buf=outFilePath, compression='gzip')
        else:
            df.to_json(path_or_buf=self.filePath)
