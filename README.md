# expressionable Python Module

This is the official repository for the ExpressionAble Python module, which allows for:

* Transforming tabular data sets from one format to another.
* Querying large data sets to extract useful data.
* Selecting columns/features to include in the resulting data set.
* Merging data sets of various formats into a single file.
* Gzipping resulting data sets, as well as the ability to read gzipped files.

**Most people will prefer to use the [ExpressionAble command-line tool](https://github.com/srp33/ShapeShifter-CLI), which combines the features of ExpressionAble with the ease of the command-line.**

Basic use of the Python module is described below. You can see full documentation on the functions in this module [here](https://shapeshifter.readthedocs.io/en/latest/).  

## Install
`pip install expressionable`

## Basic Use

After installing, import the ExpressionAble class with `from expressionable import ExpressionAble`. An ExpressionAble object 
represents the file to be transformed. It is then transformed using the `export_filter_results` method. Here is a simple
example of file called `input_file.tsv` being transformed into an HDF5 file called `output_file.h5`, while filtering 
the data on sex and age:

```python
from expressionable import ExpressionAble

my_expressionable = ExpressionAble("input_file.tsv")
my_expressionable.export_filter_results("output_file.h5", filters="Sex == 'M' and Age > 40")
```

Note that the type of file being read and exported to were not stated explicitly but inferred by ExpressionAble based on
the file extensions provided. If necessary, `input_file_type` and `output_file_type` can be named explicitly.


## Contributing

We welcome contributions that help expand ExpressionAble to be compatible with additional file formats. If you are 
interested in contributing, please follow the instructions [here](https://github.com/srp33/ExpressionAble/wiki/Adding-Support-for-Additional-File-Types-in-ExpressionAble).

## Currently Supported Formats

#### Input Formats:

* CSV
* TSV
* JSON
* Excel
* HDF5
* Parquet
* MsgPack
* Stata
* Pickle
* SQLite
* ARFF
* GCT
* GCTX
* PDF
* Gene Expression Omnibus
* Kallisto (RNA-Sequencing)
* Salmon (RNA-Sequencing)
* STAR (RNA-Sequencing)
* HT-Seq (RNA-Sequencing)
* CBio Portal (RNA expression format)

#### Output Formats:

* CSV 
* TSV
* JSON
* Excel
* HDF5
* Parquet
* MsgPack
* Stata 
* Pickle
* SQLite 
* ARFF 
* GCT 
* RMarkdown notebook
* Jupyter notebook
