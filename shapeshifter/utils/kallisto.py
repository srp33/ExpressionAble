import os
import tempfile
import zipfile

import pandas as pd


def kallisto_to_pandas(fileName, colName="tpm"):
    df = pd.DataFrame()
    i = 0
    z = zipfile.ZipFile(fileName)
    #extract the zipfile and put the contents in a temp directory
    with tempfile.TemporaryDirectory() as temp:
        z.extractall(temp)
        #parse through the directory
        for dirpath, dirs, files in sorted(os.walk(temp)):
            for d in sorted(dirs):
                abundanceFilePath = os.path.join(dirpath,d, "abundance.tsv")

                if not os.path.exists(abundanceFilePath):
                    continue
                #read the first file's contents into a dataframe
                if i == 0:
                    df = pd.read_csv(filepath_or_buffer=abundanceFilePath, sep="\t", index_col=0, usecols=["target_id", colName])
                    df = df.rename(columns={colName : d})
                #join other files onto the first dataframe
                else:
                    tempdf = pd.read_csv(filepath_or_buffer=abundanceFilePath, sep = "\t", index_col=0, usecols=["target_id", colName])
                    tempdf = tempdf.rename(columns={colName: d})
                    df = df.join(tempdf, how='inner')
                i += 1
    # Make sure we found at least one file and throw an exception if we didn't.
    if df.empty:
        raise Exception("No abundance.tsv files were found in {}.".format(fileName))

    #transpose the dataframe
    df = df.T
    #rename the dataframe to "Sample"
    #todo if it still is out of order, just sort the data by the values in the sample column
    df.index = df.index.rename("Sample")
    df.columns.name = None

    return df
