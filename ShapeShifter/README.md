# Adding Support for Additional File Types in ShapeShifter

Currently, ShapeShifter supports working with files in the following formats: CSV, TSV, JSON, Excel, HDF5, Parquet, MsgPack, Stata, Pickle, HTML, SQLite, ARFF, and GCT. This file explains what steps must be taken to expand ShapeShifter to work with other types of files.

## Getting Started
First, you will need to clone the ShapeShifter git repository to have access to its files:
```bash
git clone https://github.com/srp33/ShapeShifter.git
cd ShapeShifter

```
Or, if you have previously cloned the ShapeShifter git repository, make sure it is up to date:
```bash
git pull origin master
```
Create a new branch on your copy of the git repository (see below). Replace `new-branch-name` with the name of the file
type you will be implementing:
```bash
git checkout -b new-branch-name
```
Now you are ready to use ShapeShifter code and begin work on your file type!

## Adding a file type to ShapeShifter

All ShapeShifter-supported file types are associated with a class that inherits from SSFile.SSFile.
Make sure the class you are building properly inherits from SSFile. For example, If I were writing a class to support the ARFF
file type, my class declaration would look like this:
```python
from SSFile import SSFile
class ARFFFile(SSFile):
    ...
``` 
At a minimum, such a class must implement and override the following three SSFile functions:

```python
def read_input_to_pandas(self, columnList=[], indexCol="Sample")

def write_to_file(self,df, gzipResults=False, includeIndex=False, null='NA')

def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
```
The two most significant functions are reading a file into a Pandas data frame and writing the contents of a Pandas data frame
to a file. `export_filter_results` uses the two previously-mentioned functions to allow ShapeShifter to filter or alter the data set.
```python
def read_input_to_pandas(self, columnList=[], indexCol="Sample")
```
This function must provide means for reading in your desired file type stored at the location `self.filePath`. The file may be gzipped, which can be checked 
using `self.isGzipped`. One way to read in a gzipped file is to temporarily unzip it using `SSFile._gunzip_to_temp_file()`
and then delete the temporary file. If I were reading from an ARFF file, and I had written a function `arffToPandas(filePath)` that takes an ARFF file
and puts it in a Pandas data frame, the code might look like the example below. In your code, you would replace `arffToPandas`
with a function or some code that reads your file into a Pandas data frame:

```python
    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            #read the unzipped tempfile into a dataframe
            df = arffToPandas(tempFile.name)
            #delete the tempfile
            os.remove(tempFile.name)
        else:
            df = arffToPandas(self.filePath)
        #reduce the dataframe to only the requested columns
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
This function must provide means for writing data stored in a Pandas data frame to the location stored at `self.filePath`. 
If gzipResults is True, the file created should be gzipped. One way to do this is to export the file to a temporary file, and then gzip that file. 
To do this, you can create a `tempfile.NamedTemporaryFile(delete=False)`, write your data frame to that file path, close the temporary file, and then
use `SSFile._gzip_results()` to gzip that temporary file to your desired file path. 
If I were writing to a MsgPack file, the code for this function might look like the example below. In your code, you would
replace `df.to_msgpack` with a function or code you wrote that writes your data frame to your file type:
```python
import tempfile
    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA'):
        if gzipResults:
            tempFile =tempfile.NamedTemporaryFile(delete=False)
            #write the dataframe to MsgPack at file location tempFile.name
            df.to_msgpack(path = tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFilePath = tempFile.name, outFilePath = self.filePath)
        else:
            df.to_msgpack(self.filePath)
```
`includeIndex` indicates whether or not the index column should be written to the file or not. Whether it should or not will depend on your own implementation and whether or not you
want Pandas' default index stored in your file. `null` is an optional parameter which indicates how 'None' should be represented in your file.
```python
def export_filter_results(self, inputSSFile, gzippedInput=False, columnList=[], query=None, transpose=False, includeAllColumns=False, gzipResults=False, indexCol="Sample"):
```
This function uses `read_input_to_pandas` to read `inputSSFile` into a Pandas data frame, preps the data by filtering and transposing, then using `write_to_file` to export the filtered data.

Most likely, all of the necessary work in this function can be performed by `SSFile._prep_for_export()`, which can be called like as shown below, and then exported. Unless your file type has
special behavior when transposing, your code may very well be exactly the following:

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
You need to add a clause to `SSFile.factory()`
that will be used to construct an file object of your type. The `type` parameter is a string that corresponds to
the name of your file type. If such a string is given, you should then return a file object of your type with the given `filePath` and `type`. If I were adding support for a GCT file, my code would look like  this:
```python
def factory(filePath, type):
    if type.lower() ==......
    ...
    elif type.lower() == 'gct': return GCTFile.GCTFile(filePath,type)
```
Finally, a clause should be added in the `SSFile.__determine_extension()` function that indicates what file extension or extensions correspond to your file type.
This method should return the name of your file type when a file name has the extension related to your file type. The purpose 
of this function is to enable ShapeShifter to infer file types based on file extensions. If I were adding support for Parquet files, whose file extension is '.pq', my code would look like this:

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
Now you can use the CompareDataframes.py script to check whether or not your result file is correct. To do so, I would execute the following program, putting your result file and the 
key file as the arguments on the command line:
```bash
python3 CompareDataframes.py results.arff Tests/OutputData/Parquet1ToOtherFormatsKey/MultiFilter.tsv
```
The script will output if the test passed, or why it failed.

The second test is performed in a similar manner, except it compares your gzipped file against against the full unit test file:

```bash
python3 CompareDataframes.py gzipped.arff.gz Tests/InputData/UnitTest.tsv
```
If these tests pass, congratulations! Now they must be added into the testing suite which will be run every time a commit is made.
First, you must add your gzipped file to the proper location in the `Tests` folder: `Tests/InputData/GzippedInput/`. In order for the test to work,
your file MUST be named `gzipped.arff.gz`, substituting 'arff' for your specific file extension.
Then you must make two small additions to the testing script `RunTests.sh`. Near the top of the file is a list named `extensions` which lists the file extensions being tested.
Add your file extension, in quotes, to the list. Finally, you must add a line to the section of code indicated by the comment `#exporting queries to other file types`. 
If the file type I were adding were ARFF, whose extension is .arff, I would add the following line of code to the script:
```bash
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.arff -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
``` 
Otherwise, replace the '.arff' extension with the extension of your file type.

Now that your file type is tested with all the others, you can verify that your code has been successfully integrated into ShapeShifter by running the test script:
```bash
bash RunTests.sh
```
If all the tests pass, you are ready to submit a pull request and officially integrate your code into ShapeShifter!

## Submitting a pull request
Add, commit, and push your changes to the branch that you created earlier. 
Replace `message` with a brief messages that describes the work you have done. 
Replace `new-branch-name` with the name of the branch you created previously:
```bash
git add --all
git commit -m "message"
git push origin new-branch-name
```
Go [here](https://github.com/srp33/ShapeShifter/compare?expand=1) to create a GitHub pull request. Put "master" as the base branch 
and your new branch as the compare branch. Click on "Create pull request". We will then check to make sure your code is working properly.
If it is, we will integrate your code into the ShapeShifter repository.

