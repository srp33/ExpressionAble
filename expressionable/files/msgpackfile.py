import gzip
import tempfile

import pandas as pd

from ..files import EAFile


class MsgPackFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if self.isGzipped:
            with gzip.open(self.filePath) as path:
                df=pd.read_msgpack(path)
        else:
            df = pd.read_msgpack(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df.set_index(df.columns[0])
        if gzipResults:
            tempFile =tempfile.NamedTemporaryFile(delete=False)
            df.to_msgpack(tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            df.to_msgpack(super()._remove_gz(self.filePath))
