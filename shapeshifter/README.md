# Adding Support for Additional File Types in ShapeShifter

Currently, ShapeShifter supports working with files in the following formats: CSV, TSV, JSON, Excel, HDF5, Parquet, MsgPack, Stata, Pickle, HTML, SQLite, ARFF, Salmon, Kallisto, Jupyter notebook, RMarkdown, and GCT. This file explains what steps must be taken to expand ShapeShifter to work with other types of files.

## Getting Started
If you are unfamiliar with object-oriented programming, classes, and inheritance in python, your time would be well spent
working through a few tutorials before you get going. I personally recommend [this tutorial](https://www.tutorialspoint.com/python/python_classes_objects.htm)
and [these interactive exercises](https://www.learnpython.org/en/Classes_and_Objects) to help you out.
Since you will need to implement your own classes that inherit from and use preexisting code, this README will make much more sense
if you are comfortable with those principles.

To get started, you will need to create your own fork of [the ShapeShifter repository](https://github.com/srp33/ShapeShifter).
Click on the "Fork" button as shown below:


![Fork button on Github](https://github-images.s3.amazonaws.com/help/bootcamp/Bootcamp-Fork.png)

Then, you will need to clone your fork of the ShapeShifter repository to have access to its files.
Press the "Clone or download" button as shown below to display the URL you will use to clone the repository.

![Clone button on Github](https://camo.githubusercontent.com/b50d9bb4d043c1b27dd6e4db5bb2d402697f4d4a/687474703a2f2f692e696d6775722e636f6d2f75624e627765302e706e67)

Copy the URL, then enter in the following commands at the command line, replacing `<URL>` with the URL you just copied from GitHub:
```bash
git clone <URL>
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

Begin by adding a new file to ShapeShifter/shapeshifter/files. The file name should be all lowercase and indicate the
file type you are supporting.
For example, if I were creating a class for supporting the ARFF format, I would name my file arfffile.py.
All ShapeShifter-supported file types are associated with a class that inherits from SSFile.
Make sure the class you are building properly inherits from SSFile. For example, If I were writing a class to support the ARFF
file type, my class declaration would look like this:
```python
from ..files import SSFile
class ARFFFile(SSFile):
    ...
``` 
If I were implementing support for reading in a file of my chosen type to ShapeShifter, I would at a minimum
override and implement this function:

```python
def read_input_to_pandas(self, columnList=[], indexCol="Sample")
```
If I were implementing support for writing/exporting data to a file of my chosen type from ShapeShifter, I would
at a minimum override and implement this function:
```python
def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False)
```

## Function Details
```python
def read_input_to_pandas(self, columnList=[], indexCol="Sample")
```
This function must provide means for reading your desired file type stored at the location `self.filePath` into a Pandas data frame. 
This function must return a Pandas data frame that contains the information stored in the file. If passed a list of desired columns `columnList`, this function should return a Pandas data frame containing the data on your file only for the selected columns.
If the list of columns is empty, it should return the entire data set from the file in a Pandas data frame.
Note: the returned data frame should not have an index (besides the default index). If necessary, reset the index using
`df.reset_index(inplace=True)`, and do not worry about the parameter `indexCol`. The file may be gzipped, which can be checked 
using `self.isGzipped`. One way to read in a gzipped file is to temporarily unzip it using `SSFile._gunzip_to_temp_file()`
and then delete the temporary file. If I were reading from an ARFF file, and I had written a function `arffToPandas(filePath)` that takes an ARFF file
and puts it in a Pandas data frame, the code might look like the example below. In your code, you would replace `arffToPandas`
with a function or some code that reads your file into a Pandas data frame:

```python
    def read_input_to_pandas(self, columnList=[], indexCol="Sample"):
        if self.isGzipped:
            tempFile = super()._gunzip_to_temp_file()
            #read the unzipped tempfile into a dataframe using YOUR function
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



```python
def write_to_file(self,df, gzipResults=False, includeIndex=False, null='NA')
```
This function must provide means for writing data stored in a Pandas data frame to the location stored at `self.filePath`. 
If gzipResults is True, the file created should be gzipped. One way to do this is to export the file to a temporary file, and then gzip that file. 
To do this, you can create a `tempfile.NamedTemporaryFile(delete=False)`, write your data frame to that file path, close the temporary file, and then
use `SSFile._gzip_results()` to gzip that temporary file to your desired file path. 
If I were writing to a GCT file, the code for this function might look like the example below. In your code, you would
replace `toGCT()` with a function or code you wrote that writes your data frame to your file type:
```python
import tempfile
    def write_to_file(self, df, gzipResults=False, includeIndex=False, null='NA', indexCol="Sample", transpose=False):
        if gzipResults:
            tempFile = tempfile.NamedTemporaryFile(delete=False)
            toGCT(df, tempFile.name)
            tempFile.close()
            super()._gzip_results(tempFile.name, self.filePath)
        else:
            toGCT(df, self._remove_gz(self.filePath))
```
`includeIndex` indicates whether or not the index column should be written to the file or not. Whether it should or not will depend on your own implementation and whether or not you
want Pandas' default index stored in your file. `null` is an optional parameter which indicates how 'None' should be represented in your file.
`indexCol` is the name of the column that should be the index in the data frame. `transpose` is a boolean indicating whether or not the 
data frame had been previously transposed from its original state.

It is worth noting that most Pandas data frames have a default index that numerically labels rows 1, 2, 3, and so on. Unless your specific format requires it,
we do not want the default index exported to your file because the default index was not originally part of the data set.
The general exception to this rule seems to be if the data has been transposed; then, the default index should be written.
When writing to some file types, it may help to insert the following code at the beginning of the `write_to_file` function:
```python
if not transpose:
    df = df.set_index(indexCol) if indexCol in df.columns else df
```
This code replaces the data frame's default index with the column indicated by `indexCol`, assuming the data was not transposed.
Doing this may make it easier to avoid writing the default index to your file. Whether or not you need to do this is dependent 
on your specific implementation, but it is worth considering.
## Connecting Your File Type to ShapeShifter
In addition to implementing the class for you file type, you must hook your file type's class into ShapeShifter so it can use it.
You need to add a clause to `SSFile.factory()`
that will be used to construct an file object of your type. The `type` parameter is a string that corresponds to
the name of your file type. If such a string is given, you should then return a file object of your type with the given `filePath` and `type`. 
In order for this to work, you will also need to add an import statement in the factory method of the file SSFile.py. If I were adding support for a GCT file, my code would look like the example below.
You should of course replace `'gct'` and `GCTFile.GCTFile()` with your extension and file constructor, respectively:
```python
def factory(filePath, type):
    from ..files import GCTFile

    if type.lower() ==......
    ...
    elif type.lower() == 'gct': return GCTFile(filePath,type)
```
A clause should be added in the `SSFile.__determine_extension()` function that indicates what file extension or extensions correspond to your file type.
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
Finally, a small addition must be made to ShapeShifter/shapeshifter/files/\_\_init__.py. At the top
of the file, add the name of the class you wrote to the list titled `__all__` as shown below:

```python
__all__ = ['SSFile', 'ARFFFile', 'CSVFile', 'ExcelFile', 'GCTFile', 'HDF5File', 'HTMLFile', 'JSONFile', 'JupyterNBFile',
           'KallistoEstCountsFile', 'KallistoTPMFile', 'MsgPackFile', 'ParquetFile', 'PickleFile', 'RMarkdownFile',
           'SalmonNumReadsFile', 'SalmonTPMFile', 'SQLiteFile', 'StataFile', 'TSVFile', 'FWFFile']
```
Then, add an import statement that imports the class you wrote. Below is an example of such a statement for importing an ExcelFile:
```python
from shapeshifter.files.excelfile import ExcelFile
```

## Adding Necessary Tests
In order to determine whether your file class properly works with ShapeShifter, it will need to pass tests
that check if the `read_input_to_pandas` and `write_to_file` work properly.
If you are only supporting reading your file type into ShapeShifter, only follow instructions for "Tests for reading files".
If you are only supporting exporting to your file type from ShapeShifter, only follow instructions for "Tests for writing to files". 
These tests will be run every time code is committed to GitHub to ensure that new code does not break previously-working code. 


 
### Tests for reading files
First, create a file of your type that is equivalent to [this TSV file](https://github.com/srp33/ShapeShifter/blob/master/ShapeShifter/Tests/InputData/UnitTest.tsv). This preferably should be done
by hand to ensure accuracy. This file must be named `input.tsv`, except you should replace the extension `tsv` with the appropriate extension for
your file type.

You will also need to provide a gzipped version of this same file. It must be named `gzipped.tsv.gz`, except you should replace the
middle extension `tsv` with the appropriate extension for your file type.

Now that you have the files created, they must be placed in the appropriate testing folder. Move your input file into the folder
`Tests/InputData/InputToRead`. Move your gzipped file to `Tests/InputData/GzippedInput`.

In order for the test script to check your files for accuracy, you must add a small item to the file `RunTests.sh`
Near the top of the file is a list declaration for `extensionsForReading` that lists all the file extensions for file types being 
tested for reading.
Add your file type's extension, in quotes, to the list. Now when the testing suite is run, it will include tests for reading
your file and its gzipped version.

### Tests for writing to files
If you have not yet done so,  create a file of your type that is equivalent to [this TSV file](https://github.com/srp33/ShapeShifter/blob/master/ShapeShifter/Tests/InputData/UnitTest.tsv). This preferably should be done
by hand to ensure accuracy. This file must be named `input.tsv`, except you should replace the extension `tsv` with the appropriate extension for
your file type.

Place this input file that you have created into the folder `Tests/OutputData/WriteToFileKey`.

In order for the test script to check your files for accuracy, 
you must add a small item to the file RunTests.sh Near the top of the file is a list declaration 
for `extensionsForWriting` that lists all the file extensions for file types being tested for writing. 
Add your file type's extension, in quotes, to the list. Now when the testing suite is run, it will include tests for 
writing to your file type.

### Running the automated tests
To run the suite of automated tests, enter the following command into the terminal:

```bash
bash RunTests2.sh
```
The script will alert you if tests fail and why they failed. Note that this testing suite is testing
operations across ALL file types, and not just yours.
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
Go [here](https://github.com/srp33/ShapeShifter/compare?expand=1) to create a GitHub pull request. Under "Comparing changes", click the blue text "compare across forks".
Ensure the base fork is "srp33/ShapeShifter/" and the base branch is "master".
For the head fork, select the fork you have been working on (it is easily identified by your GitHub username). 
For the compare branch, select the branch that you have been working on and that is up to date with your most recent changes. 
Leave a comment explaining the work you've done, then click on "Create pull request". We will then check to make sure your code is working properly.
If it is, we will integrate your code into the ShapeShifter repository.

