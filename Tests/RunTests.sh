#!/bin/bash

set -euxo pipefail

basicMasterFile="Tests/InputData/InputParquet1.pq"
gctInput="Tests/InputData/GCTUnitTest.tsv"
outputDir1="Tests/OutputData/Parquet1ToTsv"
outputDir2="Tests/OutputData/Parquet1ToOtherFormats"
keyDir1="Tests/OutputData/Parquet1ToTsvKey"
keyDir2="Tests/OutputData/Parquet1ToOtherFormatsKey"
writeToFileKey="Tests/OutputData/WriteToFileKey"

fileNames="NoChange SimpleTranspose FloatFilter IntFilter DiscreteFilter DiscreteDoubleFilter BooleanFilter SampleFilter MultiFilter ColumnsOnly FilterWithColumn FilterWithManyColumns FilterWithAllColumns NullFilter1 NullFilter2 SetIndex"
filterList=("" "-t" "-f \"float1 > 9.1\"" "-f \"int2 <= 12\"" "-f \"discrete1 = hot\"" "-f \"discrete1 = hot medium\"" "-f \"bool1 = True\"" "-f \"Sample = A\"" "-f \"Sample = A\" \"float1 < 2\" \"int1 > 3\" \"discrete2 = blue\" \"bool1 = True\"" "-f \"float1 < 8\" -c int1" "-f \"float1 < 8\" -c int1 discrete1 bool1 float2" "-f \"float1 < 8\" -a")

#When testing new file types, add your file type's extension to the appropriate list(s) below!
extensionsForReading=("csv" "json" "xlsx" "hdf" "pq" "mp" "dta" "pkl" "db" "arff" "pdf")
extensionsForWriting=("rmd" "ipynb")
extensionsForFiltering=("csv" "json" "xlsx" "hdf" "pq" "mp" "dta" "pkl" "db" "arff")

mkdir -p $outputDir1
mkdir -p $outputDir2
rm -f $outputDir1/*
rm -f $outputDir2/*

echo Convert master file to TSV
python3 ConvertFile.py $basicMasterFile $outputDir1/NoChange.tsv

echo Transpose master file
python3 ConvertFile.py $basicMasterFile $outputDir1/SimpleTranspose.tsv -t

echo Perform a variety of filters
python3 ConvertFile.py $basicMasterFile $outputDir1/FloatFilter.tsv -f "float1 > 9.1"
python3 ConvertFile.py $basicMasterFile $outputDir1/IntFilter.tsv -f "int2 <= 12"
python3 ConvertFile.py $basicMasterFile $outputDir1/DiscreteFilter.tsv -f "discrete1 == 'hot'"
python3 ConvertFile.py $basicMasterFile $outputDir1/DiscreteDoubleFilter.tsv -f  "discrete1 == 'hot' or discrete1== 'medium'" 
python3 ConvertFile.py $basicMasterFile $outputDir1/BooleanFilter.tsv -f "bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir1/SampleFilter.tsv -f "Sample == 'A'"
python3 ConvertFile.py $basicMasterFile $outputDir1/MultiFilter.tsv -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir1/ColumnsOnly.tsv -c int1,int2,bool1,bool2
python3 ConvertFile.py $basicMasterFile $outputDir1/FilterWithColumn.tsv -f  "float1 < 8" -c int1
python3 ConvertFile.py $basicMasterFile $outputDir1/FilterWithManyColumns.tsv -f  "float1 < 8" -c int1,discrete1,bool1,float2
python3 ConvertFile.py $basicMasterFile $outputDir1/FilterWithAllColumns.tsv -f  "float1 < 8" -a
python3 ConvertFile.py $basicMasterFile $outputDir1/NullFilter1.tsv -f "int1>2 or null1 != None"
python3 ConvertFile.py $basicMasterFile $outputDir1/NullFilter2.tsv -f "null1 ==None and int1>2"
python3 ConvertFile.py $basicMasterFile $outputDir1/SetIndex.tsv -f "int1>5" -s bool1

echo Compare each filtered file against the key file
for i in ${fileNames}
do
  echo python3 CompareFiles.py $outputDir1/$i.tsv $keyDir1/$i.tsv
  python3 CompareFiles.py $outputDir1/$i.tsv $keyDir1/$i.tsv
done

echo Filter and export to basic formats
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.csv -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.json -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.xlsx -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.hdf -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.db -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.pq -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.mp -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.dta -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.pkl -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.html -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ConvertFile.py $basicMasterFile $outputDir2/MultiFilter.arff -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"

echo Test exporting to base file types...
for i in "${extensionsForFiltering[@]}"
do
	echo -n Writing to .$i: 
	python3 CompareUsingEA.py $keyDir2/MultiFilter.tsv $outputDir2/MultiFilter.$i
done

echo Convert the formats that can only be exported
for i in "${extensionsForWriting[@]}"
do
    echo -n Writing $i:
    python3 ConvertFile.py $basicMasterFile $outputDir2/output.$i
done

echo Test exporting to additional file types...
for i in "${extensionsForWriting[@]}"
do
    echo -n Writing to .$i: 
    python3 CompareFiles.py $writeToFileKey/input.$i $outputDir2/output.$i
done

echo Testing reading all file types to Pandas...
for i in "${extensionsForReading[@]}"
do
	echo -n Reading from .$i: 
	python3 CompareUsingEA.py $basicMasterFile Tests/InputData/InputToRead/input.$i
done

echo -n Reading from .gct:
python3 CompareUsingEA.py $gctInput Tests/InputData/GCTUnitTest.gct

echo -n Reading from KallistoTPM:
python3 CompareUsingEA.py Tests/InputData/KallistoTPMTest.tsv Tests/InputData/KallistoTPMTest.zip kallistotpm

echo -n Reading from Kallisto_est_counts:
python3 CompareUsingEA.py Tests/InputData/Kallisto_est_counts_Test.tsv Tests/InputData/KallistoTPMTest.zip kallisto_est_counts

echo -n Reading from GEO:
python3 CompareUsingEA.py Tests/InputData/GEOtest.tsv Tests/InputData/GEOtest.txt.gz geo

echo -n Reading from .gctx:
python3 CompareUsingEA.py Tests/InputData/GCTXUnitTest.tsv Tests/InputData/GCTXUnitTest.gctx

echo -n Reading from STAR:
python3 CompareUsingEA.py Tests/InputData/StarTest.tsv Tests/InputData/StarTest.zip starreads

echo -n Reading from HT-Seq:
python3 CompareUsingEA.py Tests/InputData/HTSEQOut.tsv Tests/InputData/HTSEQ.zip htseq

echo -n Reading from CBIO file:
python3 CompareUsingEA.py Tests/InputData/CBioUnitOut.tsv Tests/InputData/CBioTest.tsv cbio

echo -n Reading from SalmonTPM:
python3 CompareUsingEA.py Tests/InputData/SalmonTPMTest.tsv Tests/InputData/SalmonTPMTest.zip salmontpm

echo -n Reading from SalmonNumReads:
python3 CompareUsingEA.py Tests/InputData/SalmonNumReadsTest.tsv Tests/InputData/SalmonTPMTest.zip salmonnumreads

echo Testing reading from gzipped files...
for i in "${extensionsForReading[@]}"
do
	echo -n Reading from gzipped .$i: 
	python3 CompareUsingEA.py $basicMasterFile Tests/InputData/GzippedInput/gzipped.$i.gz
done

echo "All tests passed!!"
