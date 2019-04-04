from ..files import SSFile
from ..utils import star_to_pandas

class StarReadsFile(SSFile):
    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = star_to_pandas(self.filePath, colnumber=3)
        # can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        df = df.reset_index()
        return df