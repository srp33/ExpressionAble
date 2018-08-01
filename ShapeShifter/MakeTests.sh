#!/bin/bash

extensions=("tsv" "json" "xlsx" "hdf" "pq" "mp" "dta" "pkl" "html" "db" "arff" "gct")
for i in "${extensions[@]}"
do
	echo $i
	python3 ParseArgs.py Tests/InputData/InputParquet1.pq result.$i
done


