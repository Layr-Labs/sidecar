#!/usr/bin/env bash

NETWORK=$1

sqlFileName="generateExpectedResults.sql"
outputFile="expectedResults.csv"

if [[ -z $NETWORK ]]; then
    echo "Usage: $0 <network>"
    exit 1
fi

if [[ $NETWORK == "mainnet-reduced" ]]; then
    sqlFileName="mainnetReduced_${sqlFileName}"
fi

if [[ $NETWORK == "testnet-reduced" ]]; then
    sqlFileName="testnetReduced_${sqlFileName}"
fi

for d in 7_goldStaging; do
    echo "Processing directory: $d"
    if [[ $d == "7_goldStaging" ]]; then
        files=$(ls "./${d}" | grep "_generateExpectedResults_")
        echo "Found SQL files: $files"
        for f in $files;
        do
            snapshotDate=$(echo $f | cut -d '_' -f3 | cut -d '.' -f1)
            echo "Snapshot date: $snapshotDate"
            sqlFileWithPath="${d}/$f"
            outputFileWithPath="${d}/expectedResults_${snapshotDate}.csv"
            echo "Generating expected results for ${sqlFileWithPath} to ${outputFileWithPath}"
            psql --host localhost --port 5434 --user blocklake --dbname blocklake --password < $sqlFileWithPath > $outputFileWithPath
        done
    else
        echo "Generating expected results for $d"
        sqlFileWithPath="${d}/${sqlFileName}"
        outputFileWithPath="${d}/${outputFile}"
        psql --host localhost --port 5434 --user blocklake --dbname blocklake --password < $sqlFileWithPath > $outputFileWithPath
    fi
done
