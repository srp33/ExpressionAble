import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import dask.dataframe as dd
import sys
import time

filePath1 = sys.argv[1]
filePath2 = sys.argv[2]
time1 = time.time()
exportFilePath = sys.argv[3]
df = pd.read_parquet(filePath1)
df2 = pd.read_parquet(filePath2)
time2 = time.time()
print("Read Parquet: " + str(time2-time1))
mergedDF = pd.merge(df,df2, how='inner', on='Sample')
time3 = time.time()
print("Merge: " + str(time3-time2))
mergedDF.to_parquet(exportFilePath)
time4 = time.time()
print("Export: " + str(time4-time3))
