import math
import sys
from expressionable.expressionable import ExpressionAble
from numbers import Number

def standardize_null_value(x):
    if x==None or x=='None' or x=='' or x=='nan' or x=='NaN' or (isinstance(x, Number) and math.isnan(x)):
        x=None

    if isinstance(x,Number) and x>0.3 and x<0.30001:
        x=0.3

    return x

masterFilePath = sys.argv[1]
testFilePath = sys.argv[2]
type=None
if len(sys.argv)>3:
    type=sys.argv[3]

try:
    ss1 = ExpressionAble(masterFilePath)
    ss2 = ExpressionAble(testFilePath, type)

    df1 = ss1.input_file.read_input_to_pandas()
    df2 = ss2.input_file.read_input_to_pandas()
    if len(df1.index) != len(df2.index) or len(df1.columns) != len(df2.columns):
        print(masterFilePath + " and " + testFilePath + ": FAIL: dimensions differ")
        sys.exit(0)

    for i in range(0,len(df1.index)):
        for j in range(0, len(df1.columns)):
            temp=df1.iloc[i,j]
            temp2=df2.iloc[i,j]
            temp=standardize_null_value(temp)
            temp2=standardize_null_value(temp2)
            isEqual = True
            if isinstance(temp,Number) and isinstance(temp2, Number):
                if abs(temp - temp2) >0.1:
                    isEqual = False
            else:
                if temp!=temp2:
                    isEqual = False
            if not isEqual:
                print(masterFilePath + " and " + testFilePath + ": FAIL: Values differ at row " +str(df1.index[i]) + " and column \'"+str(df1.columns[j])+"\'")
                print("\t" + masterFilePath + " value: " + str(temp) + "\t" + testFilePath + " value: " + str(temp2))
                sys.exit(0)

    print(masterFilePath + " and " + testFilePath + ": PASS")

except Exception as e:
    print(masterFilePath + " and " + testFilePath + ": FAIL")
    print("Error: " + str(e))
    sys.exit(1)
