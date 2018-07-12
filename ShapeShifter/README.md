#Adding Support for Additional File Types in ShapeShifter

Currently, ShapeShifter supports working with files in the following formats: CSV, TSV, JSON, Excel, HDF5, Parquet, MsgPack, Stata, Pickle, HTML, SQLite, ARFF, and GCT. This file explains what steps must be taken to expand ShapeShifter to work with other types of files.


## Adding a file type to ShapeShifter

All ShapeShifter-supported file types are associated with a class that inherits from SSFile.SSFile. 
At a minimum, such a class must implement and override the following SSFile functions:

```python
def read_input_to_pandas(self, columnList=[], indexCol="Sample")

def write_to_file(self,df, gzipResults=False, includeIndex=False, null='NA')

def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
```
The two most significant functions are reading a file into a Pandas data frame and writing the contents of a Pandas data frame
to a file. "export_filter_results" uses the two previously-mentioned functions to allow ShapeShifter to filter or alter the data set.
```python
def read_input_to_pandas(self, columnList=[], indexCol="Sample")
```
This function must provide means for reading in your desired file type stored at the location self.filePath. The file may be gzipped, which can be checked 
using self.isGzipped. One way to read in a gzipped file is to temporarily unzip it using SSFile._gunzip_to_temp_file()
and then delete the temporary file. If I were reading from an HDF5 file, the code might look like this:

```python
    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            df=pd.read_hdf(tempFile.name)
            os.remove(tempFile.name)
        else:
            df = pd.read_hdf(self.filePath)
        df = df.reset_index()
        if len(columnList) > 0:
            df = df[columnList]
        return df
```

If passed a list of desired columns, this function should return a Pandas data frame containing the data on your file only for the selected columns. 
If the list of columns is empty, it should return the entire data set from the file in a Pandas data frame.
Note: the returned data frame should be un-indexed.

```python
def write_to_file(self,df, gzipResults=False, includeIndex=False, null='NA')
```
This function must provide means for writing data stored in a Pandas data frame to the location stored at self.filePath. 
If gzipResults is True, the file created should be gzipped. One way to do this is to export the file to a temporary file, and then gzip that file. 
To do this, you can create a tempfile.NamedTemporaryFile(delete=False), write your data frame to that file path, close the temporary file, and then
use SSFile._gzip_results() to gzip that temporary file to your desired file path. 
If I were writing to a MsgPack file, the code for this function might look like this:
```python
    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        if gzipResults:
            tempFile =tempfile.NamedTemporaryFile(delete=False)
            df.to_msgpack(tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            df.to_msgpack(super()._remove_gz(self.filePath))
```
'includeIndex' indicates whether or not the index column should be written to the file or not. Whether it should or not will depend on your own implementation and whether or not you
want Pandas' default index stored in your file. 'null' is an optional parameter which indicates how 'None' should be represented in your file.
```python
def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
```
This function uses 'read_input_to_pandas' to read 'inputSSFile' into a Pandas data frame, preps the data by filtering and transposing, then using 'write_to_file' to export the filtered data.
Most likely, all of the necessary work in this function can be performed by SSFile._prep_for_export, which can be called like this, and then exported:

```python
query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
        self.write_to_file(df, gzipResults)
```
##Connecting Your File Type to ShapeShifter
In addition to implementing the class for you file type, you must hook your file type's class into ShapeShifter so it can use it.
You need to add a clause to SSFile.factory
that will be used to construct an file object of your type. If I were adding support for a GCT file, my code would look like  this:
```python
def factory(filePath, type):
    if type.lower() ==......
    ...
    elif type.lower() == 'gct': return GCTFile.GCTFile(filePath,type)
```


## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```
