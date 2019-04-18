from ..files import EAFile
from ..utils import kallisto_to_pandas


class KallistoTPMFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        df = kallisto_to_pandas(self.filePath, colName="tpm")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        df=df.reset_index()
        return df