import pandas as pd

from SSFile import SSFile


class ParquetFile(SSFile):
    def __init__(self, filePath, fileType):
        super().__init__(filePath,fileType)

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if len(columnList) == 0:
            df = pd.read_parquet(self.filePath)
        else:
            df = pd.read_parquet(self.filePath, columns=columnList)
        if df.index.name == indexCol:
            df.reset_index(inplace=True)
        return df

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
        self.write_to_file(df, gzipResults)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        if gzipResults:
            df.to_parquet(super()._append_gz(self.filePath), compression='gzip')
        else:
            df.to_parquet(self.filePath)

    def get_column_names(self, gzippedInput):
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
import gzip