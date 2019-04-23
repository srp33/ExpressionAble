

from ..files import eafile
import pandas as pd
from io import StringIO
import csv
import ftplib
from ftplib import FTP
import sys
import gzip
import os
import shutil
import tempfile

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

def check_file_path(file_name):
    print(os.getcwd())
    if os._exists(file_name):
        print('Running parser on local file')
        return False
    elif os.path.isfile(file_name):
        print('Running parser on local file')
        return False
    else:
        print('Downloading File')
        return True

def build_df(filename, columnList = []):
    geo_file = open(filename, 'r')
    output = StringIO()
    csv_writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

    for line in geo_file:

        if line.startswith('"ID_REF"'):
            line = line.rstrip('\n')
            patients = line.split('\t')
            patients = [x.strip("\"") for x in patients]
            csv_writer.writerow(patients)
            csv_writer.writerow(patients)  # One is for the index column
            break

    geo_file.seek(0)
    for line in geo_file:
        line = line.strip()
        data = line.split('\t')
        data = [x.strip("\"") for x in data]
        if data[0].startswith("!Sample_"):
            data[0] = data[0].replace("!Sample_", "")
            if len(columnList) == 0:
                csv_writer.writerow(data)
            else:
                if data[0] in columnList:
                    csv_writer.writerow(data)
                else:
                    continue
        if data[0] == "!series_matrix_table_begin":
            break

    for line in geo_file:
        if line.startswith('"ID_REF"'):
            continue
        line = line.strip()
        data = line.split('\t')
        if data[0] == "!series_matrix_table_end":
            break
        data[0] = data[0].replace("\"", "")
        if len(columnList) == 0:
            csv_writer.writerow(data)
        else:
            if data[0] in columnList:
                csv_writer.writerow(data)
            else:
                continue

    output.seek(0)
    df = pd.read_csv(output, low_memory=False)
    df = df.set_index('ID_REF').T
    geo_file.close()
    # df = df.convert_objects(convert_numeric=True)
    df = df.apply(pd.to_numeric, errors='ignore')
    # print(df.dtypes)
    return df


def ftp_download(geo_id):
    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login(user="anonymous", passwd="")

    if geo_id.startswith("GSE"):
        if len(geo_id) > 6:
            sub_name = geo_id[:-3]
        else:
            sub_name = "GSE"
        parent_directory = '/geo/series/' + sub_name + "nnn/"
        full_path = parent_directory + geo_id + "/matrix/"
        # print(full_path)
        try:
            ftp.cwd(full_path)
        except ftplib.all_errors:
            print(Exception("Invalid GEO ID"))
            print(ftplib.Error)
            sys.exit()

        listing = []
        ftp.retrlines('LIST', listing.append)
        words = listing[0].split()
        filename = words[-1].lstrip()

        if len(listing) > 1:
            print("\nWarning! Multiple data files are included in this GEO series.\n"
                  "We have downloaded the first of these "
                  "files (" + filename + ").\n"
                                         "If you would like to download a different data"
                                         " file from this GEO series,\n"
                                         "please enter the full URL \n")
            print("You can see the available data files for this GEO series here:")
            print("ftp://ftp.ncbi.nlm.nih.gov" + full_path + "\n")
            print("Here are the URLS:")
            for words in listing:
                words = words.split()
                file = words[-1].lstrip()
                print(file + ": " + "ftp://ftp.ncbi.nlm.nih.gov" + full_path + file)

    else:
        if geo_id.startswith("ftp://"):
            geo_id = geo_id[6::]

        geo_id = geo_id.replace('ftp.ncbi.nlm.nih.gov', "")
        directories = geo_id.split('/')
        full_path = ""
        for name in directories:
            full_path += name + "/"
            if name == "matrix":
                break
        try:
            ftp.cwd(full_path)
        except ftplib.all_errors:
            print(Exception("Invalid URL"))
            sys.exit()

        filename = geo_id.replace(full_path, "")

    fd = tempfile.NamedTemporaryFile(delete=False)


    ftp.retrbinary('RETR ' + filename, fd.write, 1024)

    ftp.quit()
    fd.close()
    return fd

class GEOFile(eafile.EAFile):
    """handles all geo files"""


    def read_input_to_pandas(self, columnList = [], indexCol="ID_REF"):
        if "Sample" in columnList:
            columnList.remove("Sample")
        if check_file_path(self.filePath) == True:
            f_in = ftp_download(self.filePath)
            tempfile = gunzip_to_temp_file(f_in.name)
            df = build_df(tempfile.name, columnList)
            os.remove(f_in.name)
            os.remove(tempfile.name)
        else:
            f_in = gunzip_to_temp_file(self.filePath)
            df = build_df(f_in.name, columnList)
            os.remove(f_in.name)

        return df
