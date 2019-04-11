import os
import tempfile

import pandas as pd

from ..files import EAFile


class HDF5File(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            df=pd.read_hdf(tempFile.name)
            os.remove(tempFile.name)
        else:
            df = pd.read_hdf(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df.set_index(df.columns[0])
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            df.to_hdf(tempFile.name, "group", mode='w')
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            df.to_hdf(super()._remove_gz(self.filePath), "group", mode='w')
