import pandas as pd
import re
import datetime as dt

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

def toARFF(df, fileName):
   
    df = df.fillna("?")

    writeFile = open(fileName, 'w')
    pattern = r"\..*"
    relation = re.sub(pattern, "", fileName)
    writeFile.write("@RELATION\t" + relation + "\n")
#Check the type for each column and write them in as attributes
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
