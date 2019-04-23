from ..files import EAFile
from ..utils import kallisto_to_pandas


class KallistoEstCountsFile(EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol=None):
        df = kallisto_to_pandas(self.filePath, colName="est_counts")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        df=df.reset_index()
        return df