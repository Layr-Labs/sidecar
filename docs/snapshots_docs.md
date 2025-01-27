# Snapshots
Snapshots are useful for backing up the current state of the database and quickly booting up and reducing the sync to tip time.

Below shows how to create your own snapshots, restore from the snapshot and how to use custom schemas


### `create-snapshot`
```bash
go run main.go create-snapshot --help
Create a snapshot of the database.

Usage:
  sidecar create-snapshot [flags]

Flags:
  -h, --help                          help for create-snapshot
      --output_file string            Path to save the snapshot file to (required)

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
go run main.go create-snapshot \     
  --database.host=localhost \
  --database.user=sidecar \
  --database.password=sidecar \
  --database.port=5432 \
  --database.db_name=sidecar \
  --database.schema_name=public \
  --database.create_snapshot_output=example.dump
```

### `restore-snapshot`
```bash
go run main.go restore-snapshot --help
Restore the database from a previously created snapshot file.

Note: This command restores --database.schema_name only if it's present in InputFile snapshot.
Follow the snapshot docs if you need to convert the snapshot to a different schema name than was used during snapshot creation.

Usage:
  sidecar restore-snapshot [flags]

Flags:
  -h, --help                help for restore-snapshot
      [WIP] --metadata_url string   URL for the snapshot metadata (default is sidecar.eigenlayer.xyz/snapshots_data.json, and will get the latest snapshot with a public schema matching your given chain and version if it's available)
      --input_file string   Path to the snapshot file (can override the metadata_url latest snapshot with an input_file or input_url)
      --input_url string   URL to the snapshot file (can override the metadata_url latest snapshot with an input_file or input_url)
      --hash_file string  Path to save the snapshot file to (can override the metadata_url to get a snapshot hash with a hash_file)
      --hash_url string  Path to save the snapshot file to (can override the metadata_url to get a snapshot hash with a hash_url)

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


## Converting the Schema of a Dump

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
    --input_file=snapshot.dump

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
    --output_file=new_snapshot.dump

# Drop the temporary database to free up resources:
psql -c "DROP DATABASE IF EXISTS temp_sidecar_dump_schema_conversion_db;"
```


