---
title: Rewards Data
description: How to access the rewards data from the Sidecar
sidebar_position: 2
---

This method of accessing the rewards data from the Sidecar replaces the previous method of downloading the data from the S3 file published by EigenLabs.

## Accessing the data

There are two ways to access the rewards data from the Sidecar:

* Through your terminal or a bash script with `curl` and `grpcurl`
* Using the gRPC or HTTP clients published in the [protocol-apis](https://github.com/Layr-Labs/protocol-apis) Go package.

## Listing Distribution Roots

```bash
# grpcurl
grpcurl -plaintext -d '{ }' localhost:7100 eigenlayer.sidecar.v1.rewards.Rewards/ListDistributionRoots | jq '.distributionRoots[0]'

# curl
curl -s http://localhost:7101/rewards/v1/distribution-roots


{
  "root": "0x2888a89a97b1d022688ef24bc2dd731ff5871465339a067874143629d92c9e49",
  "rootIndex": "217",
  "rewardsCalculationEnd": "2025-02-22T00:00:00Z",
  "rewardsCalculationEndUnit": "snapshot",
  "activatedAt": "2025-02-24T19:00:48Z",
  "activatedAtUnit": "timestamp",
  "createdAtBlockNumber": "3418350",
  "transactionHash": "0x769b4efbefb99c6c80738405ae5d082829d8e2e6f97ee20da615fa7073c16d90",
  "blockHeight": "3418350",
  "logIndex": "544"
}
```

## Fetching Rewards Data

```bash

```
