import tempfile

from ..errors import SizeExceededError
from ..files import EAFile


class RMarkdownFile(EAFile):

    # def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        # Does this function need to be written ?

    def to_Rmd(self, df, filename, includeIndex):

        # Truncate Data to 500 rows X 20 columns as a max size if dataframe exceeds 10,000 data points.
        #todo don't truncate, just exit. Same for sqlite, excel. custon exception?
        max_size = 10000
        if df.size > max_size:
            #df = df.iloc[0:500, 0:20]
            # Print message to warn user that data will be truncated.
            raise SizeExceededError(" R Markdown support is only available for up to 10,000 data points."
                  "\nPlease use a smaller data set or consider using a different file type")

        # Convert the dataframe to a tab-separated string, either including or excluding index.
        if includeIndex:
            stringDF = df.to_csv(sep="\t", na_rep="NA", index_label="index", index=True)
        else:
            stringDF = df.to_csv(sep="\t", na_rep="NA", index=False)

        # Create and write data to the Rmd file.
        myFile = open(filename, "w")

        # Header
        myFile.write("---\noutput: html_document\n---\n\n")

        # Code block #1
        myFile.write("### Load data into pandas DataFrame\n\n")
        myFile.write("```{r}\ndfString <- \"" + stringDF + "\"\n")
        myFile.write("df <- read.table(text=dfString, header=TRUE)\n```\n")

        # Code block #2
        myFile.write("\n### Analyze data\n\n")
        myFile.write("```{r}\nprint(summary(df))\n```")

        # Close the Rmd file.
        myFile.close()

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            self.to_Rmd(df, tempFile.name, includeIndex)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            self.to_Rmd(df, super()._remove_gz(self.filePath), includeIndex)
