import sys

import ShapeShifter


def compareDataframes(df1,df2):
    if len(df1.index) != len(df2.index) or len(df1.columns) != len(df2.columns):
        print("FAIL: dimensions differ")
        return

    for i in range(0,len(df1.index)):
        for j in range(0, len(df1.columns)):
            temp=df1.iloc[i,j]
            temp2=df2.iloc[i,j]
            if temp!=temp2:
                print("FAIL: Values differ at row " +str(df1.index[i]) + " and column \'"+str(df1.columns[j])+"\'")
                print("File 1 value: " + str(temp))
                print("File 2 value: " + str(temp2))
                return

f1 = sys.argv[1]
f2 = sys.argv[2]

try:
    ss1 = ShapeShifter.ShapeShifter(f1)
    ss2 = ShapeShifter.ShapeShifter(f2)

    df1 = ss1.inputFile.read_input_to_pandas()
    df2 = ss2.inputFile.read_input_to_pandas()

    if len(df1.index) != len(df2.index) or len(df1.columns) != len(df2.columns):
        print(f1 + " and " +f2+ ": FAIL: dimensions differ")
        sys.exit()

    for i in range(0,len(df1.index)):
        for j in range(0, len(df1.columns)):
            temp=df1.iloc[i,j]
            temp2=df2.iloc[i,j]
            if temp!=temp2:
                print(f1 + " and " + f2 + ": FAIL: Values differ at row " +str(df1.index[i]) + " and column \'"+str(df1.columns[j])+"\'")
                print(f1 + " value: " + str(temp))
                print(f2 + " value: " + str(temp2))
                sys.exit()


    # if df1.equals(df2):
    #     print(f1 + " and " +f2+ ": PASS")
    # else:
    #     merged = df1.merge(df2, indicator=True, how='outer')
    #     merged[merged['_merge'] == 'right_only']
    #     if 'right_only' in merged._merge.values or 'left_only' in merged._merge.values:
    #         print(f1 + " and " + f2 + ": FAIL")
    #         print(merged)
    #     else:
    #         print(f1 + " and "+ f2 +": PASS")
except Exception as e:
    print(f1 + " and " +f2+ ": FAIL")
    print("Error: " + str(e))

