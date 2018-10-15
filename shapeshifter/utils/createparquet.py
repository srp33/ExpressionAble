import pandas as pd
import sys

if __name__ == '__main__':
    inFile = sys.argv[1]
    outPath = sys.argv[2]

    df = pd.read_csv(inFile, sep="\t", engine = 'c')
    print(df)
    df.to_parquet(outPath)
