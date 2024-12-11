#!/usr/bin/env bash

NETWORK=$1
if [[ -z $NETWORK ]]; then
    echo "Usage: $0 <network>"
    exit 1
fi

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

bucketPath="s3://${bucketName}/"

if [[ $NETWORK == "mainnet-reduced" ]]; then
    bucketPath="${bucketPath}mainnet-reduced/"
fi
if [[ $NETWORK == "testnet-reduced" ]]; then
    bucketPath="${bucketPath}testnet-reduced/"
fi
if [[ $NETWORK == "preprod-rewardsv2" ]]; then
    bucketPath="${bucketPath}preprod-rewardsv2/"
fi

aws s3 cp "${newVersion}.tar" $bucketPath

rm "${newVersion}.tar"

echo -n $newVersion > $testdataVersionFile

git add .
git commit -m "Updated testdata version to $newVersion"

