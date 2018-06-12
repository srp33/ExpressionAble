import pandas as pd
import re
import datetime as dt
import numpy as np

#Return true if the values in the column are numeric
def isNumeric(column): 
    for item in column:
        if type(item) != float and type(item) != int and item != "?":
            return False   
    return True

#Return true if the values in the column are dates
def isDate(column):
    for item in column: 
        try:
            pd.to_datetime(item)
        except:
            if item != "?": 
                return False
    return True

#Pass in a dataframe, which will be changed to an ARFF file
#The file name is the second parameter you'll pass in.  
def toARFF(df, fileName):
   
    df = df.fillna("?")

    writeFile = open(fileName, 'w')
    pattern = r"\..*"
    relation = re.sub(pattern, "", fileName)
    writeFile.write("@RELATION\t" + relation + "\n")
#Check the type for each column and write them in as attributes
#The info in this part of the file is tab delimited
    for colName in list(df):		
        writeFile.write("@ATTRIBUTE\t" + colName)
        options = set([])
        if isNumeric(df[colName]) == True:
            writeFile.write("\tNUMERIC\n")
        elif isDate(df[colName]) == True:
            writeFile.write("\tDATE\tyyyy-MM-dd\n") 
            for value in df[colName]:
                fullDate = pd.to_datetime(value, errors = 'ignore')
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
#Write out the data
#If there's a space in the value, surround it with quotes
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
def arffToPandas(fileName):
    columnNames = []
    dataList = []
    pattern = r"@[Aa][Tt][Tt][Rr][Ii][Bb][Uu][Tt][Ee]\s+([^\s]*)\s+.*"
    with open(fileName) as dataFile:
        for line in dataFile: 
            line = line.rstrip("\n")
#Takes the column names in the attribute and put them in a list 
            if line.startswith("@ATTRIBUTE") or line.startswith("@attribute"):  
                columnNames.append(re.sub(pattern, r"\1", line)) 
                continue
            elif line.startswith("@") or line.startswith("%") or line == "":
                continue
#Turns the lines into lists and put them into a list
            line = line.replace("?", "") 
            dataList.append(line.split(","))      
    data = pd.DataFrame(dataList, columns = columnNames)
    data = data.replace("?", np.nan)
    for column in data:
        data[column] = pd.to_numeric(data[column], errors='ignore')
        i = True
        for cell in data[column]:
            if cell != "True" and cell != "False" and not pd.isnull(cell):
                i = False
                break
        if i == True:
            data[column] = data[column].astype('bool') 
    return data 
