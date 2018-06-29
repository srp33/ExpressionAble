import pandas as pd

from SSFile import SSFile


class ExcelFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = pd.read_excel(self.filePath)
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
        df = None
        includeIndex = False
        null = 'NA'
        query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
        if len(df.columns) > 16384 or len(df.index) > 1048576:
            print(
                "WARNING: Excel supports a maximum of 16,384 columns and 1,048,576 rows. The dimensions of your data are " + str(
                    df.shape))
            print("Data beyond the size limit will be truncated")
        import xlsxwriter
        writer = pd.ExcelWriter(self.filePath, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', na_rep=null, index=includeIndex)
        writer.save()
        if gzipResults:
            super()._compress_results(self.filePath)