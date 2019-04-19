from ..files import eafile
from ..utils import star_to_pandas

# Special Case Format: Used when a user wants to grab columns 1 or 2 instead of 3
def check_file_name(filePath):
    input_list = filePath.split('?')
    if len(input_list) == 1:
        return filePath
    else:
        return input_list[0]

def column_finder(filePath):
    input_list = filePath.split('?')
    if len(input_list) == 1:
        print("No column parameter entered. Default is \'3\'")
        return 3
    else:
        cols = input_list[-1].replace("c=", "")
        for char in cols:
            if not char.isdigit():
                print("Invalid column input. Default column is \'3\'")
                return 3
        if int(cols) > 3 or int(cols) < 1:
            print("Column input out of range (1-3). Default column is \'3\'")
            return 3
        return int(cols)

class StarReadsFile(eafile.EAFile):

    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        file_name = check_file_name(self.filePath)
        column_number = column_finder(self.filePath)
        df = star_to_pandas(file_name, colnumber=column_number)
        if len(columnList) > 0:
            df = df[columnList]
        df = df.reset_index()
        return df