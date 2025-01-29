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
curl -LO https://eigenlayer-sidecar.s3.us-east-1.amazonaws.com/snapshots/testnet-holesky/sidecar-testnet-holesky_v3.0.0-rc.1_public_20250122.dump

./bin/sidecar restore-snapshot \
  --snapshot.input=sidecar-testnet-holesky_v3.0.0-rc.1_public_20250122.dump \
  --snapshot.verify_input=false \
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
The input can be a local file path or a URL, url of type http, https, or ftp is supported.
Follow the snapshot docs if you need to convert the snapshot to a different schema name than was used during snapshot creation.

Usage:
  sidecar restore-snapshot [flags]

Flags:
  -h, --help                    help for restore-snapshot
      --snapshot.input string   Path to the snapshot file either a URL or a local file (required)
      --snapshot.verify_input   Boolean to verify the input file against its .sha256sum file, if input is a url then it downloads the file, (default is true) (default true)

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
      --snapshot.output_file string   Path to save the snapshot file to (required), also creates a hash file

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
  --snapshot.output_file=example.dump
```


## Advanced: Converting the Schema of a Dump

If you're using a custom schema and want to use a public snapshot, you likely want to convert the dump.

This section provides a step-by-step runbook for converting a snapshot dump to use a different schema name.

Commonly the input schema is 

```
# Can use the script
./scripts/convertSnapshotSchema.sh 

```bash
./scripts/convertSnapshotSchema.sh <inputSchema> <outputSchema> input_file.dump output_file.dump <db_username> <db_password>
```


```bash
# Open your terminal and create a temporary database to work with:
psql -c "CREATE DATABASE temp_sidecar_dump_schema_conversion_db;"

# Use the Sidecar CLI to restore the snapshot dump into the temporary database:
./bin/sidecar restore-snapshot \
    --database.host=localhost \
    --database.user=... \
    --database.password=... \
    --database.port=5432 \
    --database.db_name=temp_sidecar_dump_schema_conversion_db \
    --database.schema_name=<input schema name> \
    --snapshot.input=snapshot.dump
    --snapshot.verify_input=false

# Connect to the temporary database and execute the SQL command to rename the schema:
psql -d temp_sidecar_dump_schema_conversion_db -c "ALTER SCHEMA <input schema name> RENAME TO <output schema name>;"

# Use the Sidecar CLI to create a new snapshot with the updated schema:
./bin/sidecar create-snapshot \
    --database.host=localhost \
    --database.user=... \
    --database.password=... \
    --database.port=5432 \
    --database.db_name=temp_sidecar_dump_schema_conversion_db \
    --database.schema_name=<output schema name> \
    --snapshot.output_file=new_snapshot.dump

# Drop the temporary database to free up resources:
psql -c "DROP DATABASE IF EXISTS temp_sidecar_dump_schema_conversion_db;"
```