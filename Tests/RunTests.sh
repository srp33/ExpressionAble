#!/bin/bash

#set -euo pipefail

inputFile1="Tests/InputData/InputParquet1.pq"
gctInput="Tests/InputData/GCTUnitTest.tsv"
outputDir1="Tests/OutputData/Parquet1ToTsv"
outputDir2="Tests/OutputData/Parquet1ToOtherFormats"
keyDir1="Tests/OutputData/Parquet1ToTsvKey"
keyDir2="Tests/OutputData/Parquet1ToOtherFormatsKey"
WriteToFileKey="Tests/OutputData/WriteToFileKey"
results=ssTestResults.txt

fileNames=("NoChange" "SimpleTranspose" "FloatFilter" "IntFilter" "DiscreteFilter" "DiscreteDoubleFilter" "BooleanFilter" "SampleFilter" "MultiFilter" "ColumnsOnly" "FilterWithColumn" "FilterWithManyColumns" "FilterWithAllColumns" "NullFilter1" "NullFilter2" "SetIndex")

filterList=("" "-t" "-f \"float1 > 9.1\"" "-f \"int2 <= 12\"" "-f \"discrete1 = hot\"" "-f \"discrete1 = hot medium\"" "-f \"bool1 = True\"" "-f \"Sample = A\"" "-f \"Sample = A\" \"float1 < 2\" \"int1 > 3\" \"discrete2 = blue\" \"bool1 = True\"" "-f \"float1 < 8\" -c int1" "-f \"float1 < 8\" -c int1 discrete1 bool1 float2" "-f \"float1 < 8\" -a")

#When testing new file types, add your file type's extension to the appropriate list(s) below!
extensionsForReading=("csv" "json" "xlsx" "hdf" "pq" "mp" "dta" "pkl" "db" "arff" "pdf")
extensionsForWriting=("rmd" "ipynb")

extensionsForFiltering=("csv" "json" "xlsx" "hdf" "pq" "mp" "dta" "pkl" "db" "arff")
mkdir $outputDir1
mkdir $outputDir2
rm $outputDir1/*
rm $outputDir2/*

#redirect all output to a file
#rm -f $results

#list of queries
echo Building output files...
python3 ParseArgs.py $inputFile1 $outputDir1/NoChange.tsv #| grep -v "RuntimeWarning" | tee -a $results
python3 ParseArgs.py $inputFile1 $outputDir1/SimpleTranspose.tsv -t
python3 ParseArgs.py $inputFile1 $outputDir1/FloatFilter.tsv -f "float1 > 9.1"
python3 ParseArgs.py $inputFile1 $outputDir1/IntFilter.tsv -f "int2 <= 12"
python3 ParseArgs.py $inputFile1 $outputDir1/DiscreteFilter.tsv -f "discrete1 == 'hot'"
python3 ParseArgs.py $inputFile1 $outputDir1/DiscreteDoubleFilter.tsv -f  "discrete1 == 'hot' or discrete1== 'medium'" 
python3 ParseArgs.py $inputFile1 $outputDir1/BooleanFilter.tsv -f "bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir1/SampleFilter.tsv -f "Sample == 'A'"
python3 ParseArgs.py $inputFile1 $outputDir1/MultiFilter.tsv -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir1/ColumnsOnly.tsv -c int1,int2,bool1,bool2
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithColumn.tsv -f  "float1 < 8" -c int1
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithManyColumns.tsv -f  "float1 < 8" -c int1,discrete1,bool1,float2
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithAllColumns.tsv -f  "float1 < 8" -a
python3 ParseArgs.py $inputFile1 $outputDir1/NullFilter1.tsv -f "int1>2 or null1 != None"
python3 ParseArgs.py $inputFile1 $outputDir1/NullFilter2.tsv -f "null1 ==None and int1>2"
python3 ParseArgs.py $inputFile1 $outputDir1/SetIndex.tsv -f "int1>5" -s bool1

#exporting queries to other file types

python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.csv -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.json -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.xlsx -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.hdf -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.db -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.pq -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.mp -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.dta -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.pkl -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.html -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.arff -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
#GCT unit test is unique
python3 ParseArgs.py $gctInput $outputDir2/MultiFilter.gct -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"

for i in "${extensionsForWriting[@]}"
do
    echo -n Writing $i:
    python3 ParseArgs.py $inputFile1 $outputDir2/output.$i
done

#compare with key
echo Testing filters on TSV files...
for i in "${fileNames[@]}"
do
	echo -n Filtering $i: 
	python3 CompareFiles.py $outputDir1/$i.tsv $keyDir1/$i.tsv
done

echo Testing exporting to base file types...
for i in "${extensionsForFiltering[@]}"
do
	echo -n Writing to .$i: 
	python3 CompareDataframes.py $keyDir2/MultiFilter.tsv $outputDir2/MultiFilter.$i
done
echo -n Writing to .gct:
python3 CompareDataframes.py $keyDir2/MultiFilter.gct $outputDir2/MultiFilter.gct

echo Testing exporting to additional file types...

for i in "${extensionsForWriting[@]}"
do
    echo -n Writing to .$i: 
    python3 CompareFiles.py $WriteToFileKey/input.$i $outputDir2/output.$i
done
test reading basic files here
echo Testing reading all file types to Pandas...
for i in "${extensionsForReading[@]}"
do
	echo -n Reading from .$i: 
	python3 CompareDataframes.py $inputFile1 Tests/InputData/InputToRead/input.$i
done
echo -n Reading from .gct:
python3 CompareDataframes.py $gctInput Tests/InputData/GCTUnitTest.gct

echo -n Reading from KallistoTPM:
python3 CompareDataframes.py Tests/InputData/KallistoTPMTest.tsv Tests/InputData/KallistoTPMTest.zip kallistotpm
echo -n Reading from Kallisto_est_counts:
python3 CompareDataframes.py Tests/InputData/Kallisto_est_counts_Test.tsv Tests/InputData/KallistoTPMTest.zip kallisto_est_counts

echo -n Reading from GEO:
python3 CompareDataframes.py Tests/InputData/GEOtest.tsv GSE66952 geo

echo -n Reading from .gctx:
python3 CompareDataframes.py Tests/InputData/GCTXUnitTest.tsv GCTXUnitTest.gctx

echo -n Reading from Star:
python3 CompareDataframes.py Tests/InputData/StarTest.tsv Tests/InputData/StarTest.zip starreads

echo -n Reading from SalmonTPM:
python3 CompareDataframes.py Tests/InputData/SalmonTPMTest.tsv Tests/InputData/SalmonTPMTest.zip salmontpm
echo -n Reading from SalmonNumReads:
python3 CompareDataframes.py Tests/InputData/SalmonNumReadsTest.tsv Tests/InputData/SalmonTPMTest.zip salmonnumreads

echo Testing reading from gzipped files...
for i in "${extensionsForReading[@]}"
do
	echo -n Reading from gzipped .$i: 
	python3 CompareDataframes.py $inputFile1 Tests/InputData/GzippedInput/gzipped.$i.gz
done
echo -n Reading from gzipped .gct:
python3 CompareDataframes.py Tests/InputData/GCTUnitTest.gct Tests/InputData/GzippedInput/gzipped.gct.gz


