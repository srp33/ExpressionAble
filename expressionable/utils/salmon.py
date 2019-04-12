import os
import tempfile
import zipfile

import pandas as pd


def salmon_to_pandas(fileName, colName="TPM"):
    df = pd.DataFrame()
    i = 0
    z = zipfile.ZipFile(fileName)
    #extract the zipfile and put the contents in a temp directory
    with tempfile.TemporaryDirectory() as temp:
        z.extractall(temp)
        #parse through the directory
        for dirpath, dirs, files in sorted(os.walk(temp)):
            for d in dirs:
                quantFilePath = os.path.join(dirpath, d, "quant.sf")

                if not os.path.exists(quantFilePath):
                    continue
                #read the first file's contents into a dataframe
                if i == 0:
                    df = pd.read_csv(filepath_or_buffer=quantFilePath, sep="\t", index_col=0, usecols=["Name", colName])
                    df = df.rename(columns={colName: d})
                #join other files onto the first dataframe
                else:
                    tempdf = pd.read_csv(filepath_or_buffer=quantFilePath, sep = "\t", index_col=0, usecols=["Name", colName])
                    tempdf = tempdf.rename(columns={colName: d})
                    df = df.join(tempdf, how='inner')
                i += 1
    # Make sure we found at least one file and throw an exception if we didn't.
    if df.empty:
        raise Exception("No quant.sf files were found in {}.".format(fileName))

    #transpose the dataframe
    df = df.T
    #rename the dataframe to "Sample"
    df.index = df.index.rename("Sample")
    df=df.sort_index()
    df.columns.name = None
    df = df.reset_index()
    return df
#Two different parsers for Salmon; two for kallisto.
#Kallisto_tpm and kallisto_est_counts
#Salmon_tpm and salmon_numreads
#Make capitalization consistent with that in the file (TPM)
#Change "abundance" to "quant."
#Add argument to function to specify column names? TPM or NumReads