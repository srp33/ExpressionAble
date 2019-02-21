# before this program can be used make sure to pip install tabula-py

from ..files import SSFile
import tabula
import os
import subprocess
import sys
from tabula import errors
import pandas as pd
import numpy as np



class PDFFile(SSFile):
    """Coverts pdf tables into a pandas data frame"""

    def read_input_to_pandas(self, columnList = [], indexCol="Sample" ):
        os.getcwd()
        user_input = self.filePath.split(':')
        if len(user_input) > 2:
            path = user_input[0:-1]
        else:
            path = user_input[0:2]
        path = ':'.join(path)

        def build_df(file, input_list):
            pgs = 'all'
            valid_pg_input = True
            pg_input = input_list[-1].replace(",", "").replace("-", "")
            for char in pg_input:
                if not char.isdigit():
                    valid_pg_input = False
            if valid_pg_input == True or input_list[-1] == 'all':
                pgs = input_list[-1]
                print(pgs)
            else:
                print("No valid page input. Default is page \'all\'")
            try:
                try:
                    try:
                        data_frame = tabula.read_pdf(file, pages=pgs)

                    except tabula.errors.CSVParseError:
                        print("multiple tables mode")
                        my_list = tabula.read_pdf(file, pages=pgs, multiple_tables=True)
                        data_frame = pd.DataFrame(my_list)
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



