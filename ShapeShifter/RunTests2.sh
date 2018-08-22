#!/bin/bash

results=ssTestResults.txt

rm -f $results
echo Running tests...
/bin/bash RunTests.sh >>$results 2>&1
if grep -q FAIL $results;
then
	echo The following tests failed:
	grep -A 1 FAIL $results
	echo Here is an error log:
	grep Error $results
	#echo The following tests passed:
	#grep PASS $results
	rm -f $results
	exit 1
else
	#grep PASS $results
	echo TESTS PASS
fi

rm -f $results
