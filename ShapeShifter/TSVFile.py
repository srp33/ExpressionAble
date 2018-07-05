import pandas as pd

import SSFile


class TSVFile(SSFile.SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if len(columnList) == 0:
            return pd.read_csv(self.filePath, sep="\t")
        return pd.read_csv(self.filePath, sep="\t", usecols=columnList)

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df=None
        includeIndex=False
        null='NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query, transpose, includeAllColumns, df, includeIndex, indexCol)
        self.write_to_file(df, gzipResults, includeIndex, null)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        if gzipResults:
            self.filePath = super()._append_gz(self.filePath)
            df.to_csv(path_or_buf=self.filePath, sep='\t', na_rep=null, index=includeIndex, compression='gzip')
        else:
            df.to_csv(path_or_buf=self.filePath, sep='\t', na_rep=null, index=includeIndex)

    def get_column_names(self, gzippedInput):
        if gzippedInput:
            f=gzip.open(self.filePath,'r')
            line =f.readline().decode().rstrip('\n')

        else:
            f=open(self.filePath,'r')
            line=f.readline().rstrip('\n')
        f.close()
        columns = line.split("\t")
        return columns
import gzip