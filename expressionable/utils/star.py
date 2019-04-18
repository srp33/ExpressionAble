import os
import tempfile
import zipfile


import pandas as pd

def insert_line_at_beginning_of_file(new_information, filename):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(new_information.rstrip('\r\n') + '\n' + content)

def star_to_pandas(fileName, colnumber):
    df = pd.DataFrame()
    i = 0
    z = zipfile.ZipFile(fileName)
    #extract the zipfile and put the contents in a temp directory
    with tempfile.TemporaryDirectory() as temp:
        z.extractall(temp)
        #parse through the directory
        for dirpath, dirs, files in sorted(os.walk(temp)):
            for d in sorted(dirs):
                ReadsPerGene_path = os.path.join(dirpath,d, "ReadsPerGene.out.tab")
                if not os.path.exists(ReadsPerGene_path):
                    continue
                # write a header_line to the file
                header_line = "Index\t1\t2\t3\n"
                insert_line_at_beginning_of_file(header_line, ReadsPerGene_path)
                # grab the appropriate column name
                list = header_line.strip().split('\t')
                colName = list[colnumber]
                #read the first file's contents into a dataframe
                if i == 0:
                    df = pd.read_csv(filepath_or_buffer=ReadsPerGene_path, sep="\t", index_col=0, usecols=["Index", colName])
                    df = df.rename(columns={colName : d})
                #join other files onto the first dataframe
                else:
                    tempdf = pd.read_csv(filepath_or_buffer=ReadsPerGene_path, sep = "\t", index_col=0, usecols=["Index", colName])
                    tempdf = tempdf.rename(columns={colName: d})
                    df = df.join(tempdf, how='inner')
                i += 1
    # Make sure we found at least one file and throw an exception if we didn't.
    if df.empty:
        raise Exception("No ReadsPerGene.out.tab files were found in {}.".format(fileName))

    #transpose the dataframe
    df = df.T
    #rename the dataframe to "Sample"
    #todo if it still is out of order, just sort the data by the values in the sample column
    df.index = df.index.rename("Sample")
    df.columns.name = None
    df = df.drop(['N_unmapped','N_multimapping', 'N_noFeature', 'N_ambiguous'], axis=1)
    return df