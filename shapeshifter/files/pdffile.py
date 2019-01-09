# before this program can be used make sure to pip install tabula-py

from ..files import SSFile
from tabula import read_pdf
from tabula import convert_into
import re
import sys


class pdfFile(SSFile):
    """Coverts pdf tables into a pandas data frame"""

    def read_input_to_pandas(self, columnList = []):
        df = read_pdf(self)
        if len(columnList) > 0:
            df = df[columnList]
        return df

    def write_to_file(self, df, gizResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        file_format = re.search('\.\w+', self).group()
        convert_into(sys.argv[1], sys.argv[2], output_format=file_format)
        return

