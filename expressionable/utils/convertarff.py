import re

import numpy as np
import pandas as pd


#Return true if the values in the column are numeric
def is_numeric(column):
    for item in column:
        if type(item) != float and type(item) != int and item != "?":
            return False   
    return True

#Return true if the values in the column are dates
def is_date(column):
    for item in column: 
        try:
            pd.to_datetime(item)
        except:
            if item != "?": 
                return False
    return True

#Pass in a dataframe, which will be changed to an ARFF file
#The file name is the second parameter you'll pass in.  
def to_arff(df, fileName):
    df = df.fillna("?")

    writeFile = open(fileName, 'w')
    pattern = r"\..*"
    relation = re.sub(pattern, "", fileName)
    writeFile.write("@RELATION\t" + relation + "\n")
    # Check the type for each column and write them in as attributes
    for colName in list(df):
        writeFile.write("@ATTRIBUTE\t" + colName)
        options = set([])
        if is_numeric(df[colName]) == True:
            writeFile.write("\tNUMERIC\n")
        elif is_date(df[colName]) == True:
            writeFile.write("\tDATE\tyyyy-MM-dd\n")
            for value in df[colName]:
                fullDate = pd.to_datetime(value, errors='ignore')
                date = str(fullDate).split(" ")[0]
                df = df.replace(value, date)
        else:
            for value in df[colName]:
                if value != "?":
                    options.add(value)
            if len(options) == len(df[colName]):
                writeFile.write("\tstring\n")
            else:
                writeFile.write("\t{")
                writeFile.write(str(options.pop()))
                for o in options:
                    writeFile.write("," + str(o))
                writeFile.write("}\n")
    # Write out the data
    # If there's a space in the value, surround it with quotes
    writeFile.write("@DATA\n")
    for line in df.values:
        writeFile.write(str(line[0]))
        for i in range(1, len(line)):
            if " " in str(line[i]):
                writeFile.write(",'" + str(line[i]) + "'")
                continue
            writeFile.write("," + str(line[i]))
        writeFile.write("\n")

    writeFile.close()

#Converts an ARFF file (the parameter) into a pandas dataframe
def arff_to_pandas(fileName):
    i = 0
    columnNames = []
    dataList = []
    pattern = r"@[Aa][Tt][Tt][Rr][Ii][Bb][Uu][Tt][Ee]\s+([^\s]*)\s+.*"
    with open(fileName) as dataFile:
        for line in dataFile:
            i += 1
            line = line.rstrip("\n")
            # Takes the column names in the attribute and put them in a list
            if line.startswith("@ATTRIBUTE") or line.startswith("@attribute"):
                columnNames.append(re.sub(pattern, r"\1", line))
                continue
            elif line.startswith("@DATA") or line.startswith("@data"):
                break
            elif line.startswith("@") or line.startswith("%") or line == "":
                continue

    # Turns the lines into lists and put them into a list

    data = pd.read_csv(filepath_or_buffer=fileName, sep=',', names=columnNames, skiprows=i)

    data = data.replace("?", np.nan)

    return data
