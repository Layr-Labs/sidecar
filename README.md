# EigenLayer Sidecar

The EigenLayer Sidecar is an open source, permissionless, verified indexer enabling anyone (AVS, operator, etc) to access EigenLayer’s protocol rewards in real-time.

A core responsibility of the Sidecar is facilitating the calculations of [rewards](https://docs.eigenlayer.xyz/eigenlayer/rewards-claiming/rewards-claiming-overview) distributed to stakers and operators by AVSs.

**Helpful Links**

* [Rewards overview](https://docs.eigenlayer.xyz/eigenlayer/rewards-claiming/rewards-claiming-overview)
* [RewardsCoordinator contract technical documentation](https://github.com/Layr-Labs/eigenlayer-contracts/blob/dev/docs/core/RewardsCoordinator.md)
* [EigenLayer Rewards Calculation Process](https://hackmd.io/u-NHKEvtQ7m7CVDb4_42bA)

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

```text
Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  run         Run the sidecar

```

### `run` options

```text
Run the sidecar

Usage:
  sidecar run [flags]

Flags:
  -h, --help   help for run

Global Flags:
  -c, --chain string               The chain to use (mainnet, holesky, preprod (default "mainnet")
      --database.db_name string    Defaults to 'sidecar' (default "sidecar")
      --database.host string       Defaults to 'localhost'. Set to something else if you are running PostgreSQL on your own (default "localhost")
      --database.password string   
      --database.port int          Defaults to '5432' (default 5432)
      --database.user string       Defaults to 'sidecar' (default "sidecar")
      --debug                      "true" or "false"
      --ethereum.rpc-url string    e.g. "http://34.229.43.36:8545"
      --ethereum.ws-url string     e.g. "ws://34.229.43.36:8546"
      --rpc.grpc-port int          e.g. 7100 (default 7100)
      --rpc.http-port int          e.g. 7101 (default 7101)
      --statsd.url string          e.g. "localhost:8125"

```


### Bring Your Own PostgreSQL database

See [PostgreSQL Setup](docs/postgresql_setup.md) for instructions on setting up a PostgreSQL database.

### Directly using Go

_Assumes you have PosgresSQL running locally already_

```bash
make build
./bin/sidecar run \
    --ethereum.rpc-url="http://34.229.43.36:8545" \
    --chain="holesky" \
    --statsd.url="localhost:8125" \
    --database.host="localhost" \
    --database.port="5432" \
    --database.user="sidecar" \
    --database.password="..." \
    --database.db_name="sidecar"

# OR with go run
go run main.go run \
    --ethereum.rpc-url="http://34.229.43.36:8545" \
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
  -e SIDECAR_ETHEREUM_RPC_URL="http://34.229.43.36:8545" \
  -e SIDECAR_CHAIN="holesky" \
  -e SIDECAR_STATSD_URL="localhost:8125" \
  -e SIDECAR_DATABASE_HOST="localhost" \
  -e SIDECAR_DATABASE_PORT="5432" \
  -e SIDECAR_DATABASE_USER="sidecar" \
  -e SIDECAR_DATABASE_PASSWORD="..." \
  -e SIDECAR_DATABASE_DB_NAME="sidecar" \
  --tty -i \
  public.ecr.aws/z6g0f8n7/go-sidecar:latest run
```

### Build and run a container locally

_Assumes you have PosgresSQL running locally already_

```bash
make docker-buildx-self

docker run \
  -e "SIDECAR_DEBUG=false" \
  -e "SIDECAR_ETHEREUM_RPC_URL=http://34.229.43.36:8545" \
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

# Boot from a snapshot

* Mainnet (not yet available)
* Testnet ([2024-11-22](https://eigenlayer-sidecar.s3.us-east-1.amazonaws.com/snapshots/testnet-holesky/sidecar-testnet-holesky-20241122.dump))

```bash
curl -LO https://eigenlayer-sidecar.s3.amazonaws.com/snapshots/testnet/sidecar-testnet-2024-11-22.tar.gz

tar -xvf sidecar-testnet-2024-11-22.tar.gz

pg_restore --host <hostname> --port 5432 --username <username> --dbname <dbname> --no-owner sidecar-testnet-2024-11-22.dump
```

## RPC Routes

### Get current block height

```bash
grpcurl -plaintext -d '{}'  localhost:7100 eigenlayer.sidecar.api.v1.Rpc/GetBlockHeight
```

### Get the stateroot at a block height

```bash
grpcurl -plaintext -d '{ "blockNumber": 1140438 }'  localhost:7100 eigenlayer.sidecar.api.v1.Rpc/GetStateRoot
