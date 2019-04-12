import gzip
import tempfile

import pandas as pd

from ..errors import SizeExceededError
from ..files import EAFile


class ExcelFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if self.isGzipped:
            with gzip.open(self.filePath) as path:
                df=pd.read_excel(path)

        else:
            df = pd.read_excel(self.filePath)
        if len(columnList) > 0:
            df = df[columnList]
        return df


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if len(df.columns) > 16384 or len(df.index) > 1048576:
            raise SizeExceededError("Excel supports a maximum of 1,048,576 rows and 16,384 columns. The dimensions of your data are " + str(df.shape)
                                                      +"\nPlease use a smaller data set or consider using a different file type")
            # print(
            #     "WARNING: Excel supports a maximum of 1,048,576 rows and 16,384 columns. The dimensions of your data are " + str(
            #         df.shape))
            # print("Data beyond the size limit will be truncated")
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            writer = pd.ExcelWriter(tempFile.name, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Sheet1', na_rep=null, index=includeIndex)
            writer.save()
            tempFile.close()
            super()._gzip_results(self.filePath)
        else:
            writer = pd.ExcelWriter(super()._remove_gz(self.filePath), engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Sheet1', na_rep=null, index=includeIndex)
            writer.save()
