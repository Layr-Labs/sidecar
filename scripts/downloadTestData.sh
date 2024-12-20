#!/usr/bin/env bash

NETWORK=$1
if [[ -z $NETWORK ]]; then
    echo "Usage: $0 <network>"
    exit 1
fi

version=$(cat .testdataVersion)
bucketName="eigenlayer-sidecar-testdata"

dataUrl="https://${bucketName}.s3.amazonaws.com/${NETWORK}/${version}.tar"

if [[ -z $version ]]; then
  echo "No version found in .testdataVersion"
  exit 1
fi
echo "Downloading testdata version $dataUrl"

curl -L $dataUrl | tar xvz -C ./
