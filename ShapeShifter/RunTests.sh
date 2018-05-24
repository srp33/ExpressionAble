#!/bin/bash

inputFile1="Tests/InputData/InputParquet1.pq"
outputDir1="Tests/OutputData/Parquet1ToTsv"
outputDir2="Tests/OutputData/Parquet1ToOtherFormats"
keyDir1="Tests/OutputData/Parquet1ToTsvKey"
keyDir2="Tests/OutputData/Parquet1ToOtherFormatsKey"

fileNames=("NoChange" "SimpleTranspose" "FloatFilter" "IntFilter" "DiscreteFilter" "DiscreteDoubleFilter" "BooleanFilter" "SampleFilter" "MultiFilter" "FilterWithColumn" "FilterWithManyColumns" "FilterWithAllColumns")

filterList=("" "-t" "-f \"float1 > 9.1\"" "-f \"int2 <= 12\"" "-f \"discrete1 = hot\"" "-f \"discrete1 = hot medium\"" "-f \"bool1 = True\"" "-f \"Sample = A\"" "-f \"Sample = A\" \"float1 < 2\" \"int1 > 3\" \"discrete2 = blue\" \"bool1 = True\"" "-f \"float1 < 8\" -c int1" "-f \"float1 < 8\" -c int1 discrete1 bool1 float2" "-f \"float1 < 8\" -a")

extensions=("csv" "json" "xlsx" "hdf" "feather" "pq" "mp" "dta" "pkl" "html")

#echo for loop!
#for i in ${!fileNames[*]}; do
#	echo ${filterList[$i]}
#	python3 ParseArgs.py $inputFile1 $outputDir1/${fileNames[$i]}.csv ${filterList[$i]}
#done

#list of queries
echo Building output files...
python3 ParseArgs.py $inputFile1 $outputDir1/NoChange.tsv
python3 ParseArgs.py $inputFile1 $outputDir1/SimpleTranspose.tsv -t
python3 ParseArgs.py $inputFile1 $outputDir1/FloatFilter.tsv -f "float1 > 9.1"
python3 ParseArgs.py $inputFile1 $outputDir1/IntFilter.tsv -f "int2 <= 12"
python3 ParseArgs.py $inputFile1 $outputDir1/DiscreteFilter.tsv -f "discrete1 == 'hot'"
python3 ParseArgs.py $inputFile1 $outputDir1/DiscreteDoubleFilter.tsv -f  "discrete1 == 'hot' or discrete1== 'medium'" 
python3 ParseArgs.py $inputFile1 $outputDir1/BooleanFilter.tsv -f "bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir1/SampleFilter.tsv -f "Sample == 'A'"
python3 ParseArgs.py $inputFile1 $outputDir1/MultiFilter.tsv -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithColumn.tsv -f  "float1 < 8" -c int1
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithManyColumns.tsv -f  "float1 < 8" -c int1,discrete1,bool1,float2
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithAllColumns.tsv -f  "float1 < 8" -a

python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.csv -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.json -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.xlsx -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.hdf -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.feather -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.pq -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.mp -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True" 
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.dta -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.pkl -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.html -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"
#python3 ParseArgs.py $inputFile1 $outputDir2/MultiFilter.arff -f "Sample == 'A' and float1 < 2 and int1 > 3 and discrete2 == 'blue' and bool1 == True"




#compare with key
echo Comparing tsv output with key...
for i in "${fileNames[@]}"
do
	python3 CompareFiles.py $outputDir1/$i.tsv $keyDir1/$i.tsv
done

echo Comparing other file type output with key...
for i in "${extensions[@]}"
do
	python3 CompareDataframes.py $keyDir2/MultiFilter.tsv $outputDir2/MultiFilter.$i
done

