import pandas as pd

from SSFile import SSFile


class SQLiteFile(SSFile):

    def read_input_to_pandas(self, columnList, indexCol):
        from sqlalchemy import create_engine
        engine = create_engine('sqlite:///' + self.filePath)
        table = self.filePath.split('.')[0]
        tableList = table.split('/')
        table = tableList[len(tableList) - 1]
        query = "SELECT * FROM " + table
        if len(columnList) > 0:
            query = "SELECT " + ", ".join(columnList) + " FROM " + table

        df = pd.read_sql(query, engine)
        return df

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):

        filePath=self.filePath #needs to be stored seperately as a string, can't be turned to a file object
        if query != None:
            query = super()._translate_null_query(query)
        if gzippedInput:
            inputSSFile.filePath=gzip.open(inputSSFile.filePath)
        df = inputSSFile.__filter_data(columnList=columnList, query=query,
                                       includeAllColumns=includeAllColumns, indexCol=indexCol)
        null = 'NA'
        includeIndex = False
        if len(df.columns) > 2000:
            print(
                "Error: SQLite supports a maximum of 2,000 columns. Your data has " + str(len(df.columns)) + " columns")
            return
        from sqlalchemy import create_engine
        engine = create_engine('sqlite:///' + filePath)
        table = filePath.split('.')[0]
        tableList = table.split('/')
        table = tableList[len(tableList) - 1]
        if not transpose:
            df = df.set_index(indexCol) if indexCol in df.columns else df
            df.to_sql(table, engine, index=True, if_exists="replace")
        else:
            df = df.set_index(indexCol) if indexCol in df.columns else df
            df = df.transpose()
            df.to_sql(table, engine, if_exists="replace", index=True, index_label=indexCol)
        if gzipResults:
            super().__compress_results(filePath)
import gzip