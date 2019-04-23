import tempfile

from ..errors import SizeExceededError
from ..files import EAFile


class JupyterNBFile(EAFile):

    # def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        # Does this function need to be written ?

    def to_JupyterNB(self, df, filename, includeIndex):

        import nbformat as nbf

        # Truncate Data to 500 rows X 20 columns as a max size if dataframe exceeds 10,000 data points.
        max_size = 10000
        if df.size > max_size:
            raise SizeExceededError(
                "Jupyter NoteBook support is only available for up to 10,000 data points."
                "\nPlease use a smaller data set or consider using a different file type")

        # Convert the dataframe to a tab-separated string, including or excluding index.
        if includeIndex:
            stringDF = df.to_csv(sep="\t", index_label="index", index=True)
        else:
            stringDF = df.to_csv(sep="\t", index=False)

        # Create the Notebook
        nb = nbf.v4.new_notebook()

        text1 = "## Load data into pandas DataFrame"

        df_code = """\
        import sys
        import pandas as pd
        if sys.version_info[0] < 3:
            from StringIO import StringIO
        else:
            from io import StringIO

        dfString = \"\"\"""" + stringDF + """\"\"\"

        df = pd.read_table(StringIO(dfString), sep='\t', index_col='index')
        print(df)"""

        text2 = "## Analyze data"

        summary_code = "df.info(max_cols = 10)"
        # Change max_cols in the future possibly.

        # Create the notebook cells, and put the above variables in them. These will show up in the .ipynb file.
        nb['cells'] = [nbf.v4.new_markdown_cell(text1),
                    nbf.v4.new_code_cell(df_code),
                    nbf.v4.new_markdown_cell(text2),
                    nbf.v4.new_code_cell(summary_code)]

        # Write the .ipynb to the desired file path.
        nbf.write(nb, filename)

    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol=None, transpose=False):

        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            self.to_JupyterNB(df, tempFile.name, includeIndex)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            self.to_JupyterNB(df, super()._remove_gz(self.filePath), includeIndex)
