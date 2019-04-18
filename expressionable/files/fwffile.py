import pandas as pd

from ..files import EAFile


class FWFFile(EAFile):
    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if len(columnList) == 0:
            return pd.read_fwf(self.filePath, low_memory=False) #low_memory is false for not allowing mixed types?
        return pd.read_csv(self.filePath, usecols=columnList, low_memory=False)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        # Pandas to_fwf is not yet implemented
        pass