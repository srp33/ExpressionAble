import os
import tempfile
import zipfile


import pandas as pd

def insert_line_at_beginning_of_file(new_information, filename):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(new_information.rstrip('\r\n') + '\n' + content)

def htseq_to_pandas(fileName, colnumber):
    df = pd.DataFrame()
    i = 0
    z = zipfile.ZipFile(fileName)
    file_names = []
    #extract the zipfile and put the contents in a temp directory
    with tempfile.TemporaryDirectory() as temp:
        z.extractall(temp)
        #parse through the directory
        for dirpath, dirs, files in sorted(os.walk(temp)):
            for file in files:
                if file[0] != "." and file[-4:] == ".tsv":
                    filePath = os.path.join(dirpath,file)
                    file_path_list = filePath.split("/")
                    folder_name = file_path_list[-2]
                    file_names.append(folder_name)

                    if i == 0:
                        df = pd.read_csv(filepath_or_buffer=filePath, sep="\t", index_col=0, header = None )
                        df = df.drop(['__no_feature', '__ambiguous', '__too_low_aQual', '__not_aligned', '__alignment_not_unique'])

                    #join other files onto the first dataframe
                    else:
                       tempdf = pd.read_csv(filepath_or_buffer=filePath, sep = "\t", index_col=0, header = None)
                       tempdf = tempdf.drop(['__no_feature', '__ambiguous', '__too_low_aQual', '__not_aligned', '__alignment_not_unique'])
                       df = pd.concat([df, tempdf], axis=1)

                    i += 1
    # Make sure we found at least one file and throw an exception if we didn't.
    if df.empty:
        raise Exception("No .tsv files were found in the unzipped {} folder.".format(fileName))

    df = df.drop(labels = ['15S_rRNA', '21S_rRNA'], axis =0)
    df.columns = file_names
    df = df.transpose()
    df.index.names = ["Sample"]

    return df