---
title: Boot the Sidecar from a Snapshot
description: How to use a snapshot to start or restore your Sidecar
---

Snapshots are a quicker way to sync to tip and get started.

## Snapshot Sources

* Mainnet Ethereum (not yet available)
* Testnet Holesky ([2025-01-22](https://eigenlayer-sidecar.s3.us-east-1.amazonaws.com/snapshots/testnet-holesky/sidecar-testnet-holesky_v3.0.0-rc.1_public_20250122.dump))

## Example boot from testnet snapshot
```bash
./bin/sidecar restore-snapshot \
  --snapshot.input=https://eigenlayer-sidecar.s3.us-east-1.amazonaws.com/snapshots/testnet-holesky/sidecar-testnet-holesky_v3.0.0-rc.1_public_20250122.dump \
  --snapshot.verify-input=false \
  --database.host=localhost \
  --database.user=sidecar \
  --database.password=... \
  --database.port=5432 \
  --database.db_name=sidecar \
  --database.schema_name=public 
```

## `restore-snapshot`
```bash
./bin/sidecar restore-snapshot --help
Restore the database from a previously created snapshot file.

Note: This command restores --database.schema_name only if it's present in Input snapshot.
The input can be a local file path or a URL, url of type http, https is supported.
Follow the snapshot docs if you need to convert the snapshot to a different schema name than was used during snapshot creation.

Usage:
  sidecar restore-snapshot [flags]

Flags:
  -h, --help                    help for restore-snapshot
      --snapshot.input string   Path to the snapshot file either a URL or a local file (required)
      --snapshot.verify-input   Boolean to verify the input file against its .sha256sum file, if input is a url then it downloads the file, (default is true) (default true)

Global Flags:
  -c, --chain string                              The chain to use (mainnet, holesky, preprod (default "mainnet")
      --database.db_name string                   PostgreSQL database name (default "sidecar")
      --database.host string                      PostgreSQL host (default "localhost")
      --database.password string                  PostgreSQL password
      --database.port int                         PostgreSQL port (default 5432)
      --database.schema_name string               PostgreSQL schema name (default "public")
      --database.user string                      PostgreSQL username (default "sidecar")
      --datadog.statsd.enabled                    e.g. "true" or "false"
      --datadog.statsd.url string                 e.g. "localhost:8125"
      --debug                                     "true" or "false"
      --ethereum.chunked_batch_call_size int      The number of calls to make in parallel when using the chunked batch call method (default 10)
      --ethereum.contract_call_batch_size int     The number of contract calls to batch together when fetching data from the Ethereum node (default 25)
      --ethereum.native_batch_call_size int       The number of calls to batch together when using the native eth_call method (default 500)
      --ethereum.rpc-url string                   e.g. "http://<hostname>:8545"
      --ethereum.use_native_batch_call            Use the native eth_call method for batch calls (default true)
      --prometheus.enabled                        e.g. "true" or "false"
      --prometheus.port int                       The port to run the prometheus server on (default 2112)
      --rewards.generate_staker_operators_table   Generate staker operators table while indexing
      --rewards.validate_rewards_root             Validate rewards roots while indexing (default true)
      --rpc.grpc-port int                         gRPC port (default 7100)
      --rpc.http-port int                         http rpc port (default 7101)
```

## `create-snapshot`
```bash
./bin/sidecar create-snapshot --help
Create a snapshot of the database.

Usage:
  sidecar create-snapshot [flags]

Flags:
  -h, --help                          help for create-snapshot
      --snapshot.output-file string   Path to save the snapshot file to (required), also creates a hash file

Global Flags:
  -c, --chain string                              The chain to use (mainnet, holesky, preprod (default "mainnet")
      --database.db_name string                   PostgreSQL database name (default "sidecar")
      --database.host string                      PostgreSQL host (default "localhost")
      --database.password string                  PostgreSQL password
      --database.port int                         PostgreSQL port (default 5432)
      --database.schema_name string               PostgreSQL schema name (default "public")
      --database.user string                      PostgreSQL username (default "sidecar")
      --datadog.statsd.enabled                    e.g. "true" or "false"
      --datadog.statsd.url string                 e.g. "localhost:8125"
      --debug                                     "true" or "false"
      --ethereum.chunked_batch_call_size int      The number of calls to make in parallel when using the chunked batch call method (default 10)
      --ethereum.contract_call_batch_size int     The number of contract calls to batch together when fetching data from the Ethereum node (default 25)
      --ethereum.native_batch_call_size int       The number of calls to batch together when using the native eth_call method (default 500)
      --ethereum.rpc-url string                   e.g. "http://<hostname>:8545"
      --ethereum.use_native_batch_call            Use the native eth_call method for batch calls (default true)
      --prometheus.enabled                        e.g. "true" or "false"
      --prometheus.port int                       The port to run the prometheus server on (default 2112)
      --rewards.generate_staker_operators_table   Generate staker operators table while indexing
      --rewards.validate_rewards_root             Validate rewards roots while indexing (default true)
      --rpc.grpc-port int                         gRPC port (default 7100)
      --rpc.http-port int                         http rpc port (default 7101)
```

#### Example use:
```
./bin/sidecar create-snapshot \     
  --database.host=localhost \
  --database.user=sidecar \
  --database.password=sidecar \
  --database.port=5432 \
  --database.db_name=sidecar \
  --database.schema_name=public \
  --snapshot.output-file=example.dump
```