---
title: Building the Sidecar
description: How to build the Sidecar from source
---

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

## Build

This will build the go binary and the associated sqlite3 extensions:

```bash
make deps

make build
```
