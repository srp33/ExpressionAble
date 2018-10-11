from .SSFile import SSFile
from .salmon import salmonToPandas


class SalmonNumReadsFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = salmonToPandas(self.filePath, colName="NumReads")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        return df