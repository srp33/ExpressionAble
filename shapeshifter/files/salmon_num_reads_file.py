from ..files import SSFile
from ..utils import salmon_to_pandas


class SalmonNumReadsFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = salmon_to_pandas(self.filePath, colName="NumReads")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        return df