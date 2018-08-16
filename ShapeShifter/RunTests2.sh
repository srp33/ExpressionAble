#!/bin/bash

results=ssTestResults.txt

rm -f $results
echo Running tests...
/bin/bash RunTests.sh >>$results 2>&1
if grep -q FAIL $results;
then
	echo The following tests failed:
	grep FAIL $results
	exit 1
else
	echo TESTS PASS
fi
rm -f $results
