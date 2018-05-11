import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sys
inFile = sys.argv[1]
outPath = sys.argv[2]

df = pd.read_csv(inFile, sep="\t", engine = 'c')
df.to_parquet(outPath)
