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

for d in operatorAvsRegistrationSnapshots operatorRestakedStrategies operatorShareSnapshots stakerDelegationSnapshots stakerShareSnapshots 1_goldActiveRewards; do
    echo "Generating expected results for $d"
    sqlFileWithPath="${d}/${sqlFileName}"
    outputFileWithPath="${d}/${outputFile}"
    psql --host localhost --port 5434 --user blocklake --dbname blocklake --password < $sqlFileWithPath > $outputFileWithPath
done
