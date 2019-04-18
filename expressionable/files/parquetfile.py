import pandas as pd

from ..files import EAFile


class ParquetFile(EAFile):
    def __init__(self, filePath, fileType):
        super().__init__(filePath,fileType)

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        # if self.isGzipped:
        #     super()._gunzip()
        #     if len(columnList)==0:
        #         df=pd.read_parquet(super()._remove_gz(self.filePath))
        #     else:
        #         df = pd.read_parquet(super()._remove_gz(self.filePath), columns=columnList)
        #     #delete the unzipped file that was created by super()._gunzip()
        #     os.remove(super()._remove_gz(self.filePath))
        if True:
            if len(columnList) == 0:
                df = pd.read_parquet(self.filePath)
            else:
                df = pd.read_parquet(self.filePath, columns=columnList)
            if df.index.name == indexCol and indexCol != None:
                df.reset_index(inplace=True)
        return df


    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if gzipResults:
            df.to_parquet(super()._append_gz(self.filePath), compression='gzip')
        else:
            df.to_parquet(self.filePath)

    def get_column_names(self):
        import pyarrow.parquet as pq
        p = pq.ParquetFile(self.filePath)
        columnNames = p.schema.names
        # delete 'Sample' from schema
        del columnNames[0]

        # delete extraneous other schema that the parquet file tacks on at the end
        if '__index_level_' in columnNames[len(columnNames) - 1]:
            del columnNames[len(columnNames) - 1]
        if 'Unnamed:' in columnNames[len(columnNames) - 1]:
            del columnNames[len(columnNames) - 1]
        return columnNames
