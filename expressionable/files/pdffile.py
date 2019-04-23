# before this program can be used make sure to pip install tabula-py

from ..files import eafile
import tabula
import os
import subprocess
import sys
from tabula import errors
import gzip
import tempfile
import shutil

def gunzip_to_temp_file(path):
    """
    Takes a gzipped file with extension 'gz' and unzips it to a temporary file location so it can be read into pandas
    """
    with gzip.open(path, 'rb') as f_in:
        # with open(self._remove_gz(self.filePath), 'wb') as f_out:
        #     shutil.copyfileobj(f_in, f_out)
        f_out = tempfile.NamedTemporaryFile(delete=False)
        shutil.copyfileobj(f_in, f_out)
        f_out.close()
    return f_out

def build_df(file, input_list):
    data_frame = []
    pgs = 'all'
    valid_param_input = True
    parameter_input = input_list[-1].replace(",", "").replace("-", "").replace("p=", "").replace("&", "").replace("t=",
                                                                                                 "")
    t = False
    p = False
    tables = 0
    for char in parameter_input:
        if not char.isdigit():
            valid_param_input = False
    if valid_param_input == True or 'all' in input_list:
        input_list = input_list[-1].split("&")
        # Now taking our last segment and checking pg number and table number
        for item in input_list:
            print(item)
            if item.startswith("p="):
                pgs = item.replace("p=", "")
                p = True
            elif item.startswith("t="):
                tables = item.replace("t=", "")
                tables = int(tables)
                tables -= 1
                t = True
        if not p:
            print("No valid page input. Default page is \'all\'")
    else:
        print("No valid page input nor table input. Default is page \'all\' and default table is 1")
    try:
        try:
            try:
                if not t:
                    data_frame = tabula.read_pdf(file, pages=pgs)
                else:
                    print("multiple tables mode")
                    my_list = tabula.read_pdf(file, pages=pgs, multiple_tables=True)
                    data_frame = my_list[tables]
                    transposed_df = my_list[tables].transpose()
                    data_frame.columns = transposed_df[0]
                    data_frame = data_frame.drop([0])
            except tabula.errors.CSVParseError:
                print("multiple tables mode")
                if not t:
                    print("No valid table input. Default table is 1")
                my_list = tabula.read_pdf(file, pages=pgs, multiple_tables=True)
                data_frame = my_list[tables]
                transposed_df = my_list[tables].transpose()
                data_frame.columns = transposed_df[0]
                data_frame = data_frame.drop([0])
        except OSError:
            print("Error: need to download Java 7 or higher")
    except subprocess.CalledProcessError:
        print("Page number does not exist")
    return data_frame

#raw_input vs item one depending on whether the user adds parameters
def check_file_path(raw_input, index_1):
    os.getcwd()
    if os._exists(raw_input):
        return raw_input
    elif os.path.isfile(index_1):
        return index_1
    else:
        print("invalid path")
        sys.exit()

class PDFFile(eafile.EAFile):
    """Coverts pdf tables into a pandas data frame"""

    def read_input_to_pandas(self, columnList = [], indexCol="Sample" ):
        os.getcwd()
        user_input = self.filePath.split('?')
        print(user_input)
        path = self.filePath
        path = check_file_path(path, user_input[0])
        #No specified Parameters for pg number and table number

        if self.isGzipped or path.endswith(".gz"):
            tempFile = gunzip_to_temp_file(path)
            # read the unzipped tempfile into a dataframe using YOUR function
            df = build_df(tempFile.name, user_input)
            # delete the tempfile
            os.remove(tempFile.name)
        else:
            df = build_df(path, user_input)

        if len(columnList) > 0:
            if "Sample" in columnList:
                columnList.remove("Sample")
            df = df[columnList]  # ask Brandon
        if df is None:
            print("DataFrame is empty!")
            sys.exit()
        return df



