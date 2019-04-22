from cmapPy.pandasGEXpress.parse import parse

def gctx_to_pandas(filename, columnlist):
    gctToo = parse(filename, make_multiindex=True)
    df = gctToo.data_df
    df = df.T
    if len(columnlist) > 0:
        df = df[columnlist]
    return(df)