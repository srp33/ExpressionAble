import pandas as pd
import gzip
from ..files import SSFile


class HTMLFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        #NOTE: reading html files may be deprecated
        df = pd.read_html(self.filePath)[0]
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df


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
