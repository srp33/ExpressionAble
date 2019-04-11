import pandas as pd

from ..files import EAFile


class CSVFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if len(columnList) == 0:
            return pd.read_csv(self.filePath, low_memory=False)
        return pd.read_csv(self.filePath, usecols=columnList, low_memory=False) #low memory set to false ensures no mixed types?


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if gzipResults:
            self.filePath = super()._append_gz(self.filePath)
            df.to_csv(path_or_buf=self.filePath, na_rep=null, index=includeIndex, compression='gzip')
        else:
            df.to_csv(path_or_buf=self.filePath, na_rep=null, index=includeIndex)
