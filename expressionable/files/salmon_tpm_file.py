from ..files import EAFile
from ..utils import salmon_to_pandas


class SalmonTPMFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        df = salmon_to_pandas(self.filePath, colName="TPM")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        return df