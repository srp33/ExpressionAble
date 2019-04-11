import gzip

import pandas as pd

from ..files import EAFile


class TSVFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if len(columnList) == 0:
            return pd.read_csv(self.filePath, sep="\t", low_memory=False)
        return pd.read_csv(self.filePath, sep="\t", usecols=columnList, low_memory=False)


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if gzipResults:
            self.filePath = super()._append_gz(self.filePath)
            df.to_csv(path_or_buf=self.filePath, sep='\t', na_rep=null, index=includeIndex, compression='gzip')
        else:
            df.to_csv(path_or_buf=self.filePath, sep='\t', na_rep=null, index=includeIndex)

    def get_column_names(self):
        if self.isGzipped:
            f=gzip.open(self.filePath,'r')
            line =f.readline().decode().rstrip('\n')

        else:
            f=open(self.filePath,'r')
            line=f.readline().rstrip('\n')
        f.close()
        columns = line.split("\t")
        return columns
