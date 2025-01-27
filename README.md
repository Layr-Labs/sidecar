# EigenLayer Sidecar

The EigenLayer Sidecar is an open source, permissionless, verified indexer enabling anyone (AVS, operator, etc) to access EigenLayer’s protocol rewards in real-time.

A core responsibility of the Sidecar is facilitating the calculations of [rewards](https://docs.eigenlayer.xyz/eigenlayer/rewards-claiming/rewards-claiming-overview) distributed to stakers and operators by AVSs.

# Current versions

* Mainnet: Sidecar v1 ([v1.0.0-rc.9](https://github.com/Layr-Labs/sidecar/releases/tag/v1.0.0-rc.9))
* Testnet: Sidecar v1 ([v1.0.0-rc.9](https://github.com/Layr-Labs/sidecar/releases/tag/v1.0.0-rc.9))
* Preprod: Rewards V2 ([v1.0.0-preprod.1](https://github.com/Layr-Labs/sidecar/releases/tag/v1.0.0-preprod.1))

**Helpful Links**

* [Rewards overview](https://docs.eigenlayer.xyz/eigenlayer/rewards-claiming/rewards-claiming-overview)
* [RewardsCoordinator contract technical documentation](https://github.com/Layr-Labs/eigenlayer-contracts/blob/dev/docs/core/RewardsCoordinator.md)
* [EigenLayer Rewards Calculation Process](https://hackmd.io/u-NHKEvtQ7m7CVDb4_42bA)

# Runtime dependencies

* MacOS or Linux (arm64 or amd64)
* PostgreSQL >= 15.x
* Access to an Ethereum archive node (execution client)

# Development

## Dependencies

* Go 1.22
* PostgreSQL >= 15.x
* Homebrew (if on MacOS)

## Supported build environments

* MacOS
* Linux (Ubuntu/Debian)

## Environment setup

If you have basic build tools like `make` already installed, you can run:

```bash
make deps
```

If you are starting from a fresh linux install with nothing, run:
```bash
./scripts/installDeps.sh

make deps
```

## Testing

First run:

```bash
make build
```

This will build everything you need, including the sqlite extensions if they have not yet been built.

### Entire suite

```bash
make test
```

### One off tests

`goTest.sh` is a convenience script that sets up all relevant environment variables and runs the tests.

```bash
./scripts/goTest.sh -v ./internal/types/numbers -v -p 1 -run '^Test_Numbers$' 
```

### Long-running Rewards tests

The rewards tests are time and resource intensive and are not enabled to run by default.

*Download the test data*

```bash
./scripts/downloadTestData.sh testnet-reduced
```
Run the rewards tests

```bash
REWARDS_TEST_CONTEXT=testnet-reduced TEST_REWARDS=true ./scripts/goTest.sh -timeout 0 ./pkg/rewards -v -p 1 -run '^Test_Rewards$'
````

Options:
* `REWARDS_TEST_CONTEXT` determines which test data to use.
* `TEST_REWARDS` enables the rewards tests.

# Build

This will build the go binary and the associated sqlite3 extensions:

```bash
make deps

make build
```

# Running

## Commands

Use `go run main.go --help`  to get help 
```text
Available Commands:
  completion                   Generate the autocompletion script for the specified shell
  create-snapshot              Create a snapshot of the database
  help                         Help about any command
  restore-snapshot             Restore database from a snapshot file
  run                          Run the sidecar
```

### `run` options

```text
Run the sidecar

Usage:
  sidecar run [flags]

  Note: This command restores --database.schema_name only if it's present in input snapshot file.
  Follow the snapshot docs if you need to convert the snapshot to a different schema name than was used during snapshot creation. sidecar.eigenlayer.xyz snapshots are only served in the public schema.

Flags:
  -h, --help   help for run
      [WIP] --snapshot.metadata_url string   URL for the snapshot metadata (default is sidecar.eigenlayer.xyz/snapshots_data.json, and will get the latest snapshot with a public schema matching your given chain and version, it will exit if not available)
      --snapshot.input_file string   Path to the snapshot file (can override the metadata_url latest snapshot with an input_file or input_url)
      --snapshot.input_url string   URL to the snapshot file (can override the metadata_url latest snapshot with an input_file or input_url)
      --snapshot.hash_file string  Path to save the snapshot file to (can override the metadata_url to get a snapshot hash with a hash_file)
      --snapshot.hash_url string  Path to save the snapshot file to (can override the metadata_url to get a snapshot hash with a hash_url)

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


### Bring Your Own PostgreSQL database

See [PostgreSQL Setup](docs/postgresql_setup.md) for instructions on setting up a PostgreSQL database.

### Directly using Go

_Assumes you have PosgresSQL running locally already_

```bash
make build
./bin/sidecar run \
    --ethereum.rpc-url="http://<hostname>:8545" \
    --chain="holesky" \
    --statsd.url="localhost:8125" \
    --database.host="localhost" \
    --database.port="5432" \
    --database.user="sidecar" \
    --database.password="..." \
    --database.db_name="sidecar"

# OR with go run
go run main.go run \
    --ethereum.rpc-url="http://<hostname>:8545" \
    --chain="holesky" \
    --statsd.url="localhost:8125" \
    --database.host="localhost" \
    --database.port="5432" \
    --database.user="sidecar" \
    --database.password="..." \
    --database.db_name="sidecar"
```

### Using the public Docker container

_Assumes you have PosgresSQL running locally already_

```bash
docker run -it --rm \
  -e SIDECAR_DEBUG=false \
  -e SIDECAR_ETHEREUM_RPC_URL="http://<hostname>:8545" \
  -e SIDECAR_CHAIN="holesky" \
  -e SIDECAR_STATSD_URL="localhost:8125" \
  -e SIDECAR_DATABASE_HOST="localhost" \
  -e SIDECAR_DATABASE_PORT="5432" \
  -e SIDECAR_DATABASE_USER="sidecar" \
  -e SIDECAR_DATABASE_PASSWORD="..." \
  -e SIDECAR_DATABASE_DB_NAME="sidecar" \
  -p 7100:7100 \
  --tty -i \
  public.ecr.aws/z6g0f8n7/go-sidecar:latest run
```

### Build and run a container locally

_Assumes you have PosgresSQL running locally already_

```bash
make docker-buildx-self

docker run \
  -e "SIDECAR_DEBUG=false" \
  -e "SIDECAR_ETHEREUM_RPC_URL=http://<hostname>:8545" \
  -e "SIDECAR_CHAIN=holesky" \
  -e "SIDECAR_STATSD_URL=localhost:8125" \
  -e SIDECAR_DATABASE_HOST="localhost" \
  -e SIDECAR_DATABASE_PORT="5432" \
  -e SIDECAR_DATABASE_USER="sidecar" \
  -e SIDECAR_DATABASE_PASSWORD="..." \
  -e SIDECAR_DATABASE_DB_NAME="sidecar" \
  --tty -i \
  go-sidecar:latest run
```

### Running with docker-compose

By default, this will build the sidecar locally with Docker and run it.

If you wish to use the pre-built image, uncomment the `image` property in the `sidecar` service in `docker-compose.yml` and remove the `build` section

```yaml
services:
  sidecar:
    image: public.ecr.aws/z6g0f8n7/sidecar:latest run
    #build:
    #  context: .
    #  dockerfile: Dockerfile
```

```bash
POSTGRES_DATA_PATH=<path to store postgres data> docker-compose up
```

# Snapshots
Snapshots are a quicker way to sync to tip and get started. 

See [Snapshots Docs](docs/snapshots_docs.md) for instructions on creating and restoring snapshots

### Snapshot Sources

* Mainnet Ethereum (not yet available)
* Testnet Holesky ([2024-11-22](https://eigenlayer-sidecar.s3.us-east-1.amazonaws.com/snapshots/testnet-holesky/sidecar-testnet-holesky-20241122.tar.gz))

### Example boot from testnet snapshot (default schema)
```bash
curl -LO https://eigenlayer-sidecar.s3.amazonaws.com/snapshots/testnet-holesky/sidecar-testnet-holesky-20241122.tar.gz

tar -xvf sidecar-testnet-2024-11-22.tar.gz

./bin/sidecar restore-snapshot \
  --input_file=sidecar-testnet-2024-11-22.dump \
  --database.host=localhost \
  --database.user=sidecar \
  --database.password=... \
  --database.port=5432 \
  --database.db_name=sidecar \
  --database.schema_name=public 
```

## RPC Routes

### Get current block height

```bash
grpcurl -plaintext -d '{}'  localhost:7100 eigenlayer.sidecar.api.v1.Rpc/GetBlockHeight
```

### Get the stateroot at a block height

```bash
grpcurl -plaintext -d '{ "blockNumber": 1140438 }'  localhost:7100 eigenlayer.sidecar.api.v1.Rpc/GetStateRoot
```