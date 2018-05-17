#!/bin/bash

inputFile1="Tests/InputData/InputParquet1.pq"
outputDir1="Tests/OutputData/OutputParquet1"
keyDir1="Tests/OutputData/OutputParquet1Key"

fileNames=("NoChange" "SimpleTranspose" "FloatFilter" "IntFilter" "DiscreteFilter" "DiscreteDoubleFilter" "BooleanFilter" "SampleFilter" "MultiFilter" "FilterWithColumn" "FilterWithManyColumns" "FilterWithAllColumns")

filterList=("" "-t" "-f \"float1 > 9.1\"" "-f \"int2 <= 12\"" "-f \"discrete1 = hot\"" "-f \"discrete1 = hot medium\"" "-f \"bool1 = True\"" "-f \"Sample = A\"" "-f \"Sample = A\" \"float1 < 2\" \"int1 > 3\" \"discrete2 = blue\" \"bool1 = True\"" "-f \"float1 < 8\" -c int1" "-f \"float1 < 8\" -c int1 discrete1 bool1 float2" "-f \"float1 < 8\" -a")

#echo for loop!
#for i in ${!fileNames[*]}; do
#	echo ${filterList[$i]}
#	python3 ParseArgs.py $inputFile1 $outputDir1/${fileNames[$i]}.csv ${filterList[$i]}
#done

#list of queries
echo Building output files...
python3 ParseArgs.py $inputFile1 $outputDir1/NoChange.csv
python3 ParseArgs.py $inputFile1 $outputDir1/SimpleTranspose.csv -t
python3 ParseArgs.py $inputFile1 $outputDir1/FloatFilter.csv -f "float1 > 9.1"
python3 ParseArgs.py $inputFile1 $outputDir1/IntFilter.csv -f "int2 <= 12"
python3 ParseArgs.py $inputFile1 $outputDir1/DiscreteFilter.csv -f "discrete1 = hot"
python3 ParseArgs.py $inputFile1 $outputDir1/DiscreteDoubleFilter.csv -f  "discrete1 = hot medium" 
python3 ParseArgs.py $inputFile1 $outputDir1/BooleanFilter.csv -f "bool1 = True"
python3 ParseArgs.py $inputFile1 $outputDir1/SampleFilter.csv -f "Sample == A"
python3 ParseArgs.py $inputFile1 $outputDir1/MultiFilter.csv -f "Sample = A" "float1 < 2" "int1 > 3" "discrete2 = blue" "bool1 = True"
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithColumn.csv -f  "float1 < 8" -c int1
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithManyColumns.csv -f  "float1 < 8" -c int1 discrete1 bool1 float2
python3 ParseArgs.py $inputFile1 $outputDir1/FilterWithAllColumns.csv -f  "float1 < 8" -a

#compare with key
echo Comparing output with key...
for i in "${fileNames[@]}"
do
	python3 CompareFiles.py $outputDir1/$i.csv $keyDir1/$i.csv
done



