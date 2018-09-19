import pandas as pd

from SSFile import SSFile


class HTMLFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        #NOTE: reading html files may be deprecated
        df = pd.read_html(self.filePath)[0]
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def export_filter_results(self, inputSSFile, column_list=[], query=None, transpose=False, include_all_columns=False,
                              gzip_results=False, index_col="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, column_list, query, transpose,
                                                                        include_all_columns, df, includeIndex, index_col)
        self.write_to_file(df, gzip_results, null=null)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        html = df.to_html(na_rep=null, index=False)
        if gzipResults:
            html = html.encode()
            with gzip.open(super()._append_gz(self.filePath), 'wb') as f:
                f.write(html)
        else:
            outFile = open(self.filePath, "w")
            outFile.write(html)
            outFile.close()


import gzip