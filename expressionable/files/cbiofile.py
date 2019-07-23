from ..files import eafile
import pandas as pd
import os


class CbioFile(eafile.EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        # Default Header if one isn't specified
        header = 0

        # If nothing follows the colon, raise an error
        try:

            if self.filePath[-1] == ":":

                raise KeyError

        except KeyError:

            error = "':' in File Path must be followed by 1 or 0. Got nothing."
            raise Exception(error)

        # Set the header equal to the number that follows the colon, if a char is detected, or if the number isn't 1 or 0, raise an error
        # Then remove the colon and number in filePath to allow Pandas to open it
        if self.filePath[-2] == ":" or self.filePath[-1] == ":":

            try:

                header = int(self.filePath[-1])
                if header != 1 and header != 0:

                    raise ValueError

            except ValueError:

                error = "Expected '0' or '1' after ':' in File Path. Got '{}'".format(header)
                raise Exception(error)

            self.filePath = self.filePath[:-2]

        if self.isGzipped:

            tempFile = super()._gunzip_to_temp_file()
            if len(columnList) == 0:

                df = pd.read_csv(tempFile, sep="\t", index_col=0)

            else:

                df = pd.read_csv(tempFile, usecols=columnList, index_col=0)

            os.remove(tempFile.name)
        else:

            if len(columnList) == 0:

                df = pd.read_csv(self.filePath, sep="\t", index_col=0)

            else:

                df = pd.read_csv(self.filePath, usecols=columnList, index_col=0)


        # Reset the index/header (To allow DataFrame.drop() to grab the first column), then drop the specified column
        df = df.reset_index()
        if header == 0:

            df = df.drop(df.columns[1], axis = "columns")

        else:

            df = df.drop(df.columns[0], axis = "columns")

        # Transpose the DataFrame, then grab the header from the first line
        df = df.transpose()
        df_header = df.iloc[0]

        # Remove the first line(the header grabbed from above) from the DataFrame, then create a dataFrame with the new header
        df = df[1:]
        df = df.rename(columns=df_header)

        # Reset the index (ExpressionAble will remove the default index later)
        df.index.names = ["Sample"]
        df = df.reset_index()

        return df
