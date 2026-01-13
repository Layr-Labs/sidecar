# Rewards V2.2 E2E Anvil Tests

E2E tests for Rewards V2.2 against a mainnet Ethereum Anvil fork.

## Architecture

E2E testing uses two repositories:

| Repository | Role |
|------------|------|
| **eigenlayer-contracts** | Seeds test data on Anvil (via zeus + forge) |
| **sidecar** | Indexes events and validates snapshots (Go tests) |

```
eigenlayer-contracts/                         sidecar/
┌─────────────────┐                    ┌─────────────────┐
│  zeus run       │                    │  Go tests       │
│  --env mainnet  │ ──── Anvil ────>   │  (validates     │
│  forge script   │                    │   snapshots)    │
└─────────────────┘                    └─────────────────┘
```

## Quick Start

### 1. Start Anvil (Mainnet Fork)

```bash
# Terminal 1
anvil --fork-url https://patient-serene-dew.quiknode.pro/3b4bd24cc31606cd06ac2932bad538ea2d322d7a/ \
      --port 8545 \
      --accounts 20 \
      --balance 10000
```

### 2. Upgrade Contracts to V2.2 (eigenlayer-contracts)

Use **PR #1667** for the mainnet upgrade script:

```bash
# Terminal 2 - In eigenlayer-contracts repo
cd ~/eigenlayer-contracts
git fetch origin pull/1667/head:pr-1667
git checkout pr-1667
forge build

# Run upgrade script via zeus (check PR #1667 for exact command)
zeus run --env mainnet --command "forge script <script-from-pr-1667> --rpc-url http://localhost:8545 --broadcast -vvv"
```

### 3. Seed Test Data (eigenlayer-contracts)

```bash
# Use IntegrationDeployer tests to seed deposits, delegations, etc.
zeus run --env mainnet --command "forge test --match-test testFuzz_deposit --fork-url http://localhost:8545 -vvv"

# Or use direct cast commands (see TESTING_PLAN.md for details)
```

### 4. Run Sidecar Tests

```bash
# Terminal 2 (or 3) - In sidecar repo
cd ~/sidecar/scripts/anvil_e2e_tests

# Run all E2E tests
./run_sidecar_tests.sh

# Or run specific test
./run_sidecar_tests.sh Test_E2E_Anvil_SSS_1
```

## Test Scenarios

See `TESTING_PLAN.md` for detailed scenarios and commands.

### Quick Reference

| Category | Scenario | How to Seed |
|----------|----------|-------------|
| SSS | Deposit + Withdrawal | `depositIntoStrategy()` → `queueWithdrawals()` |
| SSS | Slash | Deposit + delegate → `slashOperator()` |
| OAS | Allocation | `modifyAllocations()` |
| OAS | Deallocation | `modifyAllocations()` (allocate then deallocate) |

## Available Scripts

| Script | Purpose |
|--------|---------|
| `run_sidecar_tests.sh` | Run Go E2E tests against Anvil |
| `TESTING_PLAN.md` | Full testing plan with commands |

## Environment Variables

```bash
# Anvil
export ANVIL_RPC=http://localhost:8545

# PostgreSQL
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

## Mainnet Contract Addresses

| Contract | Address |
|----------|---------|
| StrategyManager | `0x858646372CC42E1A627fcE94aa7A7033e7CF075A` |
| DelegationManager | `0x39053D51B77DC0d36036Fc1fCc8Cb819df8Ef37A` |
| AllocationManager | `0x948a420b8Cc1d6BfD0B6087c2E7C344A2Cd0bc39` |
| RewardsCoordinator | `0x7750d328b314EfFa365A0402CcfD489B80B0adda` |
| AVSDirectory | `0x135DDa560e946695d6f155dACaFC6f1F25C1F5AF` |

## Troubleshooting

### Anvil not responding
```bash
cast block-number --rpc-url http://localhost:8545
```

### PostgreSQL issues
```bash
psql -h localhost -U postgres -c "SELECT 1;"
```

### Test failures
```bash
# Run with verbose output
TEST_E2E_ANVIL=true go test -v ./pkg/rewards -run Test_E2E_Anvil -timeout 30m
```
