# Adding Support for Additional File Types in ShapeShifter

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
def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
    df=None
    includeIndex=False
    query, inputSSFile, df, includeIndex = super()._prep_for_export(inputSSFile, gzippedInput, columnList, query,
                                                                        transpose, includeAllColumns, df, includeIndex,
                                                                        indexCol)
    self.write_to_file(df, gzipResults)
```
## Connecting Your File Type to ShapeShifter
In addition to implementing the class for you file type, you must hook your file type's class into ShapeShifter so it can use it.
You need to add a clause to SSFile.factory
that will be used to construct an file object of your type. If I were adding support for a GCT file, my code would look like  this:
```python
def factory(filePath, type):
    if type.lower() ==......
    ...
    elif type.lower() == 'gct': return GCTFile.GCTFile(filePath,type)
```
Finally, a clause should be added in the SSFile.__determine_extension function that indicates what file extension or extensions correspond to your file type.
This method should return the name of your file type when a file name has the extension related to your file type. The purpose 
of this function is so that ShapeShifter can infer file types based on file extensions. If I were adding support for Parquet files, whose file extension is '.pq', my code would look like this:

```python
def __determine_extension(fileName):
        ...
        if extension == ...
        ...
        elif extension == 'pq':
            return 'parquet'
```

## Running the tests
In order to determine whether your file class properly works with ShapeShifter, it will need to pass a test that reads your data and performs
a filter on various data types before exporting the results. It will then need to pass a test that verifies that gzipped files of your type can be properly
read as well. To use the testing scripts to perform such tests, you will first need to create a file of your type that is equivalent to
[this TSV file](https://github.com/srp33/ShapeShifter/blob/master/ShapeShifter/Tests/InputData/UnitTest.tsv). This preferably should be done
by hand to ensure accuracy. You will also need to provide a gzipped version of this file.
Once you have your file ready, use ShapeShifter or the ParseArgs command-line tool to perform the following filter and produce an output file:
```python
#Example for using ShapeShifter to produce the output file, if I were testing ARFF format
your_shapeshifter = ShapeShifter('your_unit_test.arff')
your_shapeshifter.export_filter_results(outFilePath='results.arff', filters="Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True")

```
```bash
#Example for using the ParseArgs command-line tool to produce the output file, if I were testing ARFF format
python3 ParseArgs.py your_unit_test.arff results.arff -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
```

Point them to my scripts that run tests, explain how they can run them, and tell them what type of things need to be tested.
Explain how to ensure that my scripts perform tests on their file type. They will probably need to create a test file.
Explain how to commit their code to a branch and make a pull request. Use WishBuilder instructions as an example
Explain how to run the automated tests for this system

[Link to unit test TSV file](https://github.com/srp33/ShapeShifter/blob/master/ShapeShifter/Tests/InputData/UnitTest.tsv)

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
