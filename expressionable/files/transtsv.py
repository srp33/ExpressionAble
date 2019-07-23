from ..files import eafile
import pandas as pd
import os



class TransTSV(eafile.EAFile):
    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            if len(columnList) == 0:
                df = pd.read_csv(tempFile, sep="\t", index_col=0)
            else:
                df = pd.read_csv(tempFile, usecols=columnList, index_col=0)
            os.remove(tempFile.name)
        else:
            if len(columnList) == 0:
                df = pd.read_csv(self.filePath, sep="\t", index_col=0)
            else:
                df = pd.read_csv(self.filePath, usecols=columnList, index_col=0)
        df = df.transpose()
        df.index.names = ["Sample"]
        df = df.reset_index()
        print(df)

        return df
