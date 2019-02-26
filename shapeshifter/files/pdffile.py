# before this program can be used make sure to pip install tabula-py

from ..files import SSFile
import tabula
import os
import subprocess
import sys
from tabula import errors
import re



class PDFFile(SSFile):
    """Coverts pdf tables into a pandas data frame"""

    def read_input_to_pandas(self, columnList = [], indexCol="Sample" ):
        os.getcwd()
        user_input = self.filePath.split('?')
        print(user_input)
        if len(user_input) > 2:
            path = user_input[0:-1]
        else:
            path = user_input[0:2]
        path = '?'.join(path)

        def build_df(file, input_list):
            pgs = 'all'
            valid_pg_input = True
            pg_input = input_list[-1].replace(",", "").replace("-", "").replace("p=","").replace("&", "").replace("t=",
            "")
            t = False
            p = False
            tables = 0
            for char in pg_input:
                if not char.isdigit():
                    valid_pg_input = False
            if valid_pg_input == True or 'all' in input_list:
                input_list = input_list[-1].split("&")
                for item in input_list:
                    print(item)
                    if item.startswith("p="):
                        pgs = item.replace("p=","")
                        p = True
                    elif item.startswith("t="):
                        tables = item.replace("t=","")
                        tables = int(tables)
                        tables -= 1
                        t = True
                if not p:
                    print("No valid page input. Defaut page is \'all\'")
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

        if os.path.exists(path):
            df = build_df(path, user_input)
            if len(columnList) > 0:
                if "Sample" in columnList:
                    columnList.remove("Sample")
                df = df[columnList]  # ask Brandon
            if df is None:
                print("Dataframe is empty!")
                sys.exit()
            return df
            # print(df)

        elif os.path.isfile(user_input[0]):
            my_file = user_input[0]
            df = build_df(my_file, user_input)
            if len(columnList) > 0:
                if "Sample" in columnList:
                    columnList.remove("Sample")
                df = df[columnList]  # ask Brandon
            if df is None:
                print("Dataframe is empty!")
                sys.exit()
            return df

        else:
            print("Invalid Path or File not Found")
        sys.exit()



