import pandas as pd
import gzip
from . import SSFile


class TSVFile(SSFile.SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if len(columnList) == 0:
            return pd.read_csv(self.filePath, sep="\t", low_memory=False)
        return pd.read_csv(self.filePath, sep="\t", usecols=columnList, low_memory=False)

    def export_filter_results(self, inputSSFile, column_list=[], query=None, transpose=False, include_all_columns=False,
                              gzip_results=False, index_col="Sample"):
        df=None
        includeIndex=False
        null='NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, column_list, query, transpose,
                                                                        include_all_columns, df, includeIndex, index_col)
        self.write_to_file(df, gzip_results, includeIndex, null)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
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
