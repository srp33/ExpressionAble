#!/bin/bash

set -o errexit

tmpDir=/tmp/buildSS_${USER}

mkdir -p $tmpDir
rm -rf $tmpDir/*

cp Dockerfile $tmpDir/
cp Tests/RunTests*.sh $tmpDir/
cp VERSION $tmpDir/
cp MANIFEST.in $tmpDir/
cp README.md $tmpDir/

mkdir $tmpDir/Tests/
cp -r Tests/InputData $tmpDir/Tests/
cp -r Tests/OutputData $tmpDir/Tests/
cp Tests/*.py $tmpDir/
cp -r expressionable $tmpDir
cp setup.py $tmpDir

cd $tmpDir

#docker build --no-cache -t srp33/expressionable:version$(cat VERSION) .
docker build -t srp33/expressionable:version$(cat VERSION) .

docker run -i --rm srp33/expressionable:version$(cat VERSION) /RunTests.sh

cd -
rm -rf $tmpDir/
