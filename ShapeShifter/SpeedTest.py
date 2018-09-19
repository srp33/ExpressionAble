import time

from SSFile import SSFile

#add tests for reading the whole file and a tall data set (10 cols, 1 million rows)
#later, support open science framework for storing large files
#seperate filter logic for timing
#currently excluding ARFF and HTML. We do not test Excel and Stata on METABRIC
supported_file_type_dict = {"Small": ["Parquet"], "medium": ["Parquet"]} #etc
#todo change all these to maps wherever possible
#todo: rename medium to small, get rid of small
#todo: change METABRIC to be "Wide" and randomly generated
filetypes = ['Parquet', 'CSV','Excel','HDF5','JSON','MsgPack','Pickle','SQLite', 'Stata','TSV']
extensions=['pq','csv','xlsx','hdf','json','mp','pkl','db', 'dta','tsv']
filterDescriptions=['read_whole_file','1_categorical','1_numeric','1_of_each','2_categorical','2_numeric','2_of_each']
smallFilters=[None,"discrete1=='hot'","float1>2","discrete1=='hot' and float1>2", "discrete1=='hot' and discrete2=='blue'", "float1>2 and float2<10", "discrete1=='hot' and discrete2=='blue' and float1>2 and float2<10"]
smallColumns=[[],['Sample','discrete1'],['Sample','float1'],['Sample','discrete1','float1'],['Sample','discrete1','discrete2'],['Sample','float1','float2'],['Sample','discrete1','discrete2','float1','float2']]
mediumFilters=[None,"discrete10=='hot'", "int10>20", "discrete10=='hot' and int10>20", "discrete10=='hot' and discrete20=='cold'", "int10>20 and int20<70", "discrete10=='hot' and discrete20=='cold' and int10>20 and int20<70"]
mediumColumns=[[],['Sample','discrete10'],['Sample','int10'],['Sample','discrete10','int10'],['Sample','discrete10','discrete20'],['Sample','int10','int20'],['Sample','discrete10','discrete20','int10','int20']]
metabricColumns = [[],['Sample','OS_STATUS'], ['Sample','A1BG'], ['Sample','OS_STATUS','A1BG'], ['Sample','OS_STATUS', 'HORMONE_THERAPY'], ['Sample','A1BG', 'A1CF'], ['Sample','OS_STATUS', 'HORMONE_THERAPY','A1BG', 'A1CF'] ]
metabricFilters=[None,"OS_STATUS=='LIVING'", 'A1BG>5.8', "OS_STATUS=='LIVING' and A1BG>5.8", "OS_STATUS=='LIVING' and HORMONE_THERAPY=='YES'", 'A1BG>5.8 and A1CF>5.8', "OS_STATUS=='LIVING' and HORMONE_THERAPY=='YES' and A1BG>5.8 and A1CF>5.8"]
tallColumns = [[], ['Sample','discrete2'],['Sample','int2'], ['Sample','discrete2','int2'], ['Sample','discrete2','discrete4'], ['Sample','int2','int4'], ['Sample','discrete2','discrete4', 'int2','int4']]
tallFilters = [None,"discrete2=='hot'", "int2>20", "discrete2=='hot' and int2>20", "discrete2=='hot' and discrete4=='cold'", "int2>20 and int4<70", "discrete2=='hot' and discrete4=='cold' and int2>20 and int4<70"]
header='Operation\tFormat\tSize\tFilter\tSeconds\n'

outputLines=[]
filterLines=[]
#Read small file
for i in range(0,len(filetypes)):
    for j in range(0, len(filterDescriptions)):
        fileName = "Tests/Speed/Small/SmallTest." +extensions[i]
        smallFile = SSFile.factory(fileName)
        t1=time.time()
        df=smallFile.read_input_to_pandas(columnList=smallColumns[j])
        t2=time.time()
        seconds = str(t2-t1)
        line = "Read\t"+filetypes[i]+"\tSmall\t"+filterDescriptions[j]+"\t"+seconds+"\n"
        outputLines.append(line)
        # if smallFilters[j]==None:
        #     #print NA instead
        #     seconds='NA'
        # else:
        #     t1 = time.time()
        #     df = df.query(smallFilters[j])
        #     t2 = time.time()
        #     seconds = str(t2 - t1)
        # line = "Filter\t" + filetypes[i] + "\tSmall\t" + filterDescriptions[j] + "\t" + seconds + "\n"
        # filterLines.append(line)

#Write small file
for i in range(0,len(filetypes)):
    for j in range(0, len(filterDescriptions)):
        fileName = "Tests/Speed/Small/SmallTest.pq"
        inFile = SSFile.factory(fileName)
        outFile = SSFile.factory("Tests/Speed/Output/smallOutput"+str(j)+"." + extensions[i])
        df = inFile._filter_data(columnList=smallColumns[j], query=smallFilters[j])
        t1 = time.time()
        outFile.write_to_file(df)
        t2=time.time()
        seconds = str(t2-t1)
        line = "Write\t"+filetypes[i]+"\tSmall\t"+filterDescriptions[j]+"\t"+seconds+"\n"
        outputLines.append(line)

#Read medium file
for i in range(0,len(filetypes)):
    for j in range(0, len(filterDescriptions)):
        fileName = "Tests/Speed/Medium/MediumTest." +extensions[i]
        mediumFile = SSFile.factory(fileName)
        t1=time.time()
        df=mediumFile.read_input_to_pandas(columnList=mediumColumns[j])
        t2=time.time()
        seconds = str(t2-t1)
        line = "Read\t"+filetypes[i]+"\tMedium\t"+filterDescriptions[j]+"\t"+seconds+"\n"
        outputLines.append(line)
        # if mediumFilters[j] == None:
        #     seconds='NA'
        # else:
        #     t1 = time.time()
        #     df = df.query(mediumFilters[j])
        #     t2 = time.time()
        #     seconds = str(t2 - t1)
        # line = "Filter\t" + filetypes[i] + "\tMedium\t" + filterDescriptions[j] + "\t" + seconds + "\n"
        # filterLines.append(line)

#Write medium file
for i in range(0,len(filetypes)):
    for j in range(0, len(filterDescriptions)):
        fileName = "Tests/Speed/Medium/MediumTest.pq"
        inFile = SSFile.factory(fileName)
        outFile = SSFile.factory("Tests/Speed/Output/MediumOutput"+str(j)+"." + extensions[i])
        df = inFile._filter_data(columnList=mediumColumns[j], query=mediumFilters[j])
        t1 = time.time()
        outFile.write_to_file(df)
        t2=time.time()
        seconds = str(t2-t1)
        line = "Write\t"+filetypes[i]+"\tMedium\t"+filterDescriptions[j]+"\t"+seconds+"\n"
        outputLines.append(line)

# #Read METABRIC file
# for i in range(0,len(filetypes)):
#     if filetypes[i]!='Excel' and filetypes[i]!='JSON' and filetypes[i]!='Stata' and filetypes[i]!='SQLite':
#         for j in range(0, len(filterDescriptions)):
#             print("Reading METABRIC from " + filetypes[i] +": " +str(j))
#             fileName = "Tests/Speed/METABRIC/metabric." +extensions[i]
#             metabricFile = SSFile.factory(fileName)
#             t1=time.time()
#             df=metabricFile.read_input_to_pandas(columnList=metabricColumns[j])
#             t2=time.time()
#             seconds = str(t2-t1)
#             line = "Read\t"+filetypes[i]+"\tMETABRIC\t"+filterDescriptions[j]+"\t"+seconds+"\n"
#             outputLines.append(line)
#             # if metabricFilters[j] == None:
#             #     seconds = 'NA'
#             # else:
#             #     t1 = time.time()
#             #     df = df.query(metabricFilters[j])
#             #     t2 = time.time()
#             #     seconds = str(t2 - t1)
#             # line = "Filter\t" + filetypes[i] + "\tMETABRIC\t" + filterDescriptions[j] + "\t" + seconds + "\n"
#             # filterLines.append(line)
#     else:
#         for j in range(0, len(filterDescriptions)):
#             if filetypes[i]=='JSON':
#                 reason = 'NA_super_slow'
#             else:
#                 reason = 'NA_technical'
#             line = "Read\t" + filetypes[i] + "\tMETABRIC\t" + filterDescriptions[j] + "\t" + reason + "\n"
#             outputLines.append(line)
#             # line = "Filter\t" + filetypes[i] + "\tMETABRIC\t" + filterDescriptions[j] + "\t" + reason + "\n"
#             # filterLines.append(line)
#
# #Write METABRIC file
# for i in range(0,len(filetypes)):
#     #put in row for every case explaining why the results aren't there: either super_slow or technical
#     if filetypes[i]!='Excel' and filetypes[i]!='JSON' and filetypes[i]!='Stata' and filetypes[i]!='SQLite':
#         for j in range(0, len(filterDescriptions)):
#             print("Writing METABRIC to " + filetypes[i] + ": " +str(j))
#             fileName = "Tests/Speed/METABRIC/metabric.pq"
#             inFile = SSFile.factory(fileName)
#             outFile = SSFile.factory("Tests/Speed/Output/METABRICOutput"+str(j)+"." + extensions[i])
#             df = inFile._filter_data(columnList=metabricColumns[j], query=metabricFilters[j])
#             t1 = time.time()
#             outFile.write_to_file(df)
#             t2=time.time()
#             seconds = str(t2-t1)
#             line = "Write\t"+filetypes[i]+"\tMETABRIC\t"+filterDescriptions[j]+"\t"+seconds+"\n"
#             outputLines.append(line)
#     else:
#         for j in range(0, len(filterDescriptions)):
#             if filetypes[i]=='JSON':
#                 line = "Write\t"+filetypes[i]+"\tMETABRIC\t"+filterDescriptions[j]+"\t"+"NA_super_slow"+"\n"
#             else:
#                 line = "Write\t" + filetypes[i] + "\tMETABRIC\t" + filterDescriptions[j] + "\t" + "NA_technical" + "\n"
#             filterLines.append(line)
#Read Tall file
for i in range(0,len(filetypes)):
    for j in range(0, len(filterDescriptions)):
        fileName = "Tests/Speed/Tall/TallData." +extensions[i]
        tallFile = SSFile.factory(fileName)
        t1=time.time()
        df= tallFile.read_input_to_pandas(columnList=tallColumns[j])
        t2=time.time()
        seconds = str(t2-t1)
        line = "Read\t"+filetypes[i]+"\tTall\t"+filterDescriptions[j]+"\t"+seconds+"\n"
        outputLines.append(line)
        # if tallFilters[j] == None:
        #     seconds = 'NA'
        # else:
        #     t1 = time.time()
        #     df = df.query(tallFilters[j])
        #     t2 = time.time()
        #     seconds = str(t2 - t1)
        # line = "Filter\t" + filetypes[i] + "\tTall\t" + filterDescriptions[j] + "\t" + seconds + "\n"
        # filterLines.append(line)

#Write Tall file
for i in range(0,len(filetypes)):
    for j in range(0, len(filterDescriptions)):
        fileName = "Tests/Speed/Tall/TallData.pq"
        inFile = SSFile.factory(fileName)
        outFile = SSFile.factory("Tests/Speed/Output/TallOutput"+str(j)+"." + extensions[i])
        df = inFile._filter_data(columnList=tallColumns[j], query=tallFilters[j])
        t1 = time.time()
        outFile.write_to_file(df)
        t2=time.time()
        seconds = str(t2-t1)
        line = "Write\t"+filetypes[i]+"\tTall\t"+filterDescriptions[j]+"\t"+seconds+"\n"
        outputLines.append(line)

#Get data for filter times file
file_sizes=["Small", "Medium", "Tall"]
filter_files=["Tests/Speed/Small/SmallTest.pq", "Tests/Speed/Medium/MediumTest.pq", "Tests/Speed/Tall/TallData.pq"]
for i in range(1, len(filterDescriptions)):
    small_file_obj = SSFile.factory(filter_files[0])
    df = small_file_obj.read_input_to_pandas(columnList = smallColumns[i])
    t1 = time.time()
    df = df.query(smallFilters[i])
    t2 = time.time()
    seconds = str(t2-t1)
    line = "Small\t" + filterDescriptions[i] + "\t" + seconds + "\n"
    filterLines.append(line)

for i in range(1, len(filterDescriptions)):
    medium_file_obj = SSFile.factory(filter_files[1])
    df = medium_file_obj.read_input_to_pandas(columnList = mediumColumns[i])
    t1 = time.time()
    df = df.query(mediumFilters[i])
    t2 = time.time()
    seconds = str(t2-t1)
    line = "Medium\t" + filterDescriptions[i] + "\t" + seconds + "\n"
    filterLines.append(line)

for i in range(1, len(filterDescriptions)):
    tall_file_obj = SSFile.factory(filter_files[2])
    df = tall_file_obj.read_input_to_pandas(columnList=tallColumns[i])
    t1 = time.time()
    df = df.query(tallFilters[i])
    t2 = time.time()
    seconds = str(t2 - t1)
    line = "Tall\t" + filterDescriptions[i] + "\t" + seconds + "\n"
    filterLines.append(line)

with open('FilterSpeeds.tsv','w') as outFile:
    outFile.write("Size\tFilter\tSeconds\n")
    for line in filterLines:
        outFile.write(line)

#Write all data to results file
with open('SomeResults.tsv','w') as outFile:
    outFile.write(header)
    for line in outputLines:
        outFile.write(line)
    # for line in filterLines:
    #     outFile.write(line)


