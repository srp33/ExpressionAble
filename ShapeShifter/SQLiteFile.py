import os
import tempfile

import pandas as pd

from SSFile import SSFile


class SQLiteFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        from sqlalchemy import create_engine
        filePath=self.filePath
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            filePath= tempFile.name
        engine = create_engine('sqlite:///' + filePath)
        table = filePath.split('.')[0]
        tableList = table.split('/')
        table = tableList[len(tableList) - 1]
        query = "SELECT * FROM " + table
        if len(columnList) > 0:
            query = "SELECT " + ", ".join(columnList) + " FROM " + table

        df = pd.read_sql(query, engine)
        if self.isGzipped:
            os.remove(filePath)
        return df

    def export_filter_results(self, inputSSFile, columnList=[], query=None, transpose=False, includeAllColumns=False,
                              gzipResults=False, indexCol="Sample"):

        filePath=self.filePath #needs to be stored separately as a string, can't be turned to a file object
        if query != None:
            query = super()._translate_null_query(query)
        if inputSSFile.isGzipped:
            inputSSFile.filePath=gzip.open(inputSSFile.filePath)
        df = inputSSFile._filter_data(columnList=columnList, query=query,
                                      includeAllColumns=includeAllColumns, indexCol=indexCol)
        null = 'NA'
        includeIndex = False
        if len(df.columns) > 2000:
            #print("Error: SQLite supports a maximum of 2,000 columns. Your data has " + str(len(df.columns)) + " columns.")
            #return
            #TODO: implement code below that doesn't raise sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) too many SQL variables
            print("Warning: SQLite supports a maximum of 2,000 columns. Your data has " + str(
                len(df.columns)) + " columns. Extra data has been truncated.")
            df=df.iloc[0:200,0:10]
        from sqlalchemy import create_engine
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            engine = create_engine('sqlite:///' + tempFile.name)
            table = filePath.split('.')[0]
            tableList = table.split('/')
            table = tableList[len(tableList) - 1]
            if not transpose:
                df = df.set_index(indexCol) if indexCol in df.columns else df
                df.to_sql(table, engine, index=True, if_exists="replace")
            else:
                df = df.set_index(indexCol) if indexCol in df.columns else df
                df = df.transpose()
                df.to_sql(table, engine, if_exists="replace", index=True, index_label=indexCol, chunksize=100000)
            tempFile.close()
            super()._gzip_results(tempFile.name, filePath)

        else:
            engine = create_engine('sqlite:///' + super()._remove_gz(filePath))
            table = filePath.split('.')[0]
            tableList = table.split('/')
            table = tableList[len(tableList) - 1]
            if not transpose:
                df = df.set_index(indexCol) if indexCol in df.columns else df
                df.to_sql(table, engine, index=True, if_exists="replace")
            else:
                df = df.set_index(indexCol) if indexCol in df.columns else df
                df = df.transpose()
                df.to_sql(table, engine, if_exists="replace", index=True, index_label=indexCol, chunksize=100000)

    def write_to_file(self,df, gzipResults=False, includeIndex=False, null='NA'):
        filePath = self.filePath
        if len(df.columns) > 2000:
            #print("Error: SQLite supports a maximum of 2,000 columns. Your data has " + str(len(df.columns)) + " columns.")
            #return
            #TODO: implement code below that doesn't raise sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) too many SQL variables
            print("Warning: SQLite supports a maximum of 2,000 columns. Your data has " + str(
                len(df.columns)) + " columns. Extra data has been truncated.")
            df=df.iloc[:,0:700]
        from sqlalchemy import create_engine
        if gzipResults:
            tempFile= tempfile.NamedTemporaryFile(delete=False)
            engine = create_engine('sqlite:///' + tempFile.name)
            table = filePath.split('.')[0]
            tableList = table.split('/')
            table = tableList[len(tableList) - 1]
            df.to_sql(table,engine, index=True, if_exists="replace")
            tempFile.close()
            super()._gzip_results(tempFile.name, filePath)
        else:
            engine = create_engine('sqlite:///' + super()._remove_gz(filePath))
            table = filePath.split('.')[0]
            tableList = table.split('/')
            table = tableList[len(tableList) - 1]
            df.to_sql(table, engine, index=True, if_exists="replace")

import gzip