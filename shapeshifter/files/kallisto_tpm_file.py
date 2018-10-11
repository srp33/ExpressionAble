from ..files import SSFile
from ..utils import kallisto_to_pandas


class KallistoTPMFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = kallisto_to_pandas(self.filePath, colName="tpm")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        df=df.reset_index()
        return df