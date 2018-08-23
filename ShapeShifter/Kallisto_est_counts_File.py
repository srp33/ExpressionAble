from SSFile import SSFile
from kallisto import kallistoToPandas


class Kallisto_est_counts_File(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = kallistoToPandas(self.filePath, colName="est_counts")
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        df=df.reset_index()
        return df