import pandas as pd

#Takes a pandas dataframe and converts it to a file
#The file will be named after the second parameter
def to_gct(df, fileName):
    #df = df.fillna("")
#Write #1.2 (the version string)
    writeFile = open(fileName, 'w')
    writeFile.write("#1.2\n")
#Write the number of rows and the number of columns
    writeFile.write(str(len(df.index)))
    writeFile.write("\t" + str(len(df.columns[1:])) + "\n")
    writeFile.close()
#Write the dataframe
    if "NAME" in df.columns:
        #add a description column
        df.insert(loc=1, column="Description", value=pd.Series(df["NAME"]))
    else:
        #throw a descriptive error.
        raise Exception("No column titled 'NAME' was found in the dataframe.")
    df.to_csv(path_or_buf=fileName, sep="\t", na_rep="", index=False, mode='a')

#Takes a GCT file and reads it into a pandas dataframe
def gct_to_pandas(fileName, columnList=None):
#We're trying to keep it from having the default index
    df = pd.read_csv(filepath_or_buffer=fileName, sep='\t', skiprows=2, usecols=columnList, low_memory=False)
    #todo: transpose here!
#Also remove the description column.
    df = df.drop(labels="Description", axis=1)
    return df
