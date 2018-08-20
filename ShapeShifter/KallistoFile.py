from SSFile import SSFile
from kallisto import kallistoToPandas


class KallistoFile(SSFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        df = kallistoToPandas(self.filePath)
        #can I read in only certain columns?
        if len(columnList) > 0:
            df = df[columnList]
        df=df.reset_index()
        return df