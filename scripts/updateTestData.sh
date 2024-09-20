#!/usr/bin/env bash

bucketName="eigenlayer-sidecar-testdata"
testdataVersionFile="./.testdataVersion"

if git status --porcelain | grep -q .;
then
    echo "You have uncommitted changes. Please commit or stash them before running this script."
    # exit 1
fi

newVersion=$(git rev-parse HEAD)

currentVersion=$(cat $testdataVersionFile)
if [[ -z $currentVersion ]]; then
    echo "No current version found"
else
    echo "Current version: $currentVersion"
fi

if [[ $currentVersion == $newVersion ]]; then
    echo "Current version is the same as the new version. Exiting."
    exit 0
fi

echo "New version: $newVersion"

tar -cvf "${newVersion}.tar" internal/tests/testdata

aws s3 cp "${newVersion}.tar" "s3://${bucketName}/"

rm "${newVersion}.tar"

echo -n $newVersion > $testdataVersionFile

git add .
git commit -m "Updated testdata version to $newVersion"

