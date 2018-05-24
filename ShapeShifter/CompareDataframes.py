import pandas as pd
import sys
#from scipy.io import arff

def readFileIntoPandas(fileName):
        extension= fileName.rstrip("\n").split(".")
        if len(extension)>1:
                extension=extension[len(extension)-1]
        if extension== "tsv" or extension =="txt":
                return pd.read_csv(fileName, sep="\t")
        elif extension == "csv":
                return pd.read_csv(fileName)
        elif extension== "json":
                return pd.read_json(fileName)
        elif extension== "xlsx":
                return pd.read_excel(fileName)
        elif extension== "hdf" or extension=="h5":
                return pd.read_hdf(fileName)
        elif extension=="feather":
                return pd.read_feather(fileName)
        elif extension =="pq":
                return pd.read_parquet(fileName)
        elif extension =="mp":
                return pd.read_msgpack(fileName)
        elif extension == "dta":
                return pd.read_stata(fileName)
        elif extension =="pkl":
                return pd.read_pickle(fileName)
        elif extension =="html":
                return read_html(fileName)
        #elif extension == "arff":
         #       data=arff.loadarff(fileName)
          #      return pd.DataFrame(data[0])


f1 = sys.argv[1]
f2 = sys.argv[2]

df1= readFileIntoPandas(f1)
df2= readFileIntoPandas(f2)
print(df1)
print(df2)
assert (df1.columns == df2.columns).all(), \
	"Dataframe column names are different"

if df1.equals(df2):
	print("Equals")
else:
	merged = df1.merge(df2, indicator=True, how='outer')
	merged[merged['_merge'] == 'right_only']
	if 'right_only' in merged._merge.values or 'left_only' in merged._merge.values:
		print("Dataframes from " +f1 + " and " + f2 + " differ: ")
		print(merged)
	else:
		print("Equals")
