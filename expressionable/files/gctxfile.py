from ..files import EAFile
from ..utils import gctx_to_pandas
import os

class GCTXFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            df= gctx_to_pandas(tempFile.name, columnList)
            os.remove(tempFile.name)
        else:
            df = gctx_to_pandas(self.filePath, columnList)
        return df
