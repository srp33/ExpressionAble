

from ..files import SSFile
import pandas as pd
from io import StringIO
import csv
import ftplib
from ftplib import FTP
import sys
import gzip
import os
import shutil


class GEOFile(SSFile):
    """handles all geo files"""

    def read_input_to_pandas(self, columnList = [], indexCol="Sample"):

        ftp = FTP('ftp.ncbi.nlm.nih.gov')
        ftp.login(user="anonymous", passwd="")

        geo_id = self.filePath

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

        local_file = open("new_file.txt", 'w+b')
        ftp.retrbinary('RETR ' + filename, local_file.write, 1024)
        local_file.close()
        ftp.quit()

        with gzip.open("new_file.txt", 'rb') as local_file:
            with open("new_file_2.txt", 'wb') as f_out:
                shutil.copyfileobj(local_file, f_out)
        f_out.close()

        # This converts bytes to strings

        geo_file = open("new_file_2.txt", 'r')
        output = StringIO()
        csv_writer = csv.writer(output)

        for line in geo_file:

            if line.startswith('"ID_REF"'):
                line = line.rstrip('\n')
                patients = line.split('\t')
                patients = [x.strip("\"") for x in patients]
                csv_writer.writerow(patients)
                break

        geo_file.seek(0)
        for line in geo_file:
            if line.startswith('"ID_REF"'):
                continue
            line = line.strip()
            data = line.split('\t')
            data = [x.strip("\"") for x in data]
            if data[0].startswith("!Sample_") or data[0].endswith("_at"):
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
            line = line.strip()
            data = line.split('\t')
            if data[0] == "!series_matrix_table_end":
                break
            data[0] = data[0].replace("\"", "")
            csv_writer.writerow(data)

        output.seek(0)
        df = pd.read_csv(output)
        df = df.transpose()
        geo_file.close()
        local_file.close()
        os.remove("new_file.txt")
        os.remove("new_file_2.txt")
        return df



