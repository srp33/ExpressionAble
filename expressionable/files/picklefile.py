import pandas as pd

from ..files import EAFile


class PickleFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        df = pd.read_pickle(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df.set_index(df.columns[0])
        if gzipResults:
            df.to_pickle(super()._append_gz(self.filePath), compression='gzip')
        else:
            df.to_pickle(self.filePath)
