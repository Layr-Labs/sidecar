# Rewards V2.2 E2E Testing Plan

## Overview

E2E testing for Rewards V2.2 requires two repositories:

1. **eigenlayer-contracts** - Upgrades contracts to V2.2 and seeds test data using zeus
2. **sidecar** - Indexes events and validates snapshot generation

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Two-Repository Testing Flow                          │
└─────────────────────────────────────────────────────────────────────────┘

   eigenlayer-contracts/                         sidecar/
   ════════════════════                          ═══════
   
   ┌─────────────────┐                    ┌─────────────────┐
   │  zeus run       │                    │  Go tests       │
   │  --env mainnet  │                    │  (e2e_anvil_    │
   │  --command      │                    │   test.go)      │
   │  "forge script" │                    │                 │
   └────────┬────────┘                    └────────┬────────┘
            │                                      │
            │  1. Fork mainnet                     │  4. Index events
            │  2. Upgrade contracts to V2.2        │  5. Generate snapshots
            │  3. Seed test data                   │  6. Validate results
            │     (deposits, withdrawals,          │
            │      delegations, slashes)           │
            │                                      │
            └──────────────┬───────────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │     Anvil       │
                  │  (Mainnet Fork  │
                  │   + V2.2        │
                  │   Contracts)    │
                  │  localhost:8545 │
                  └─────────────────┘
```

## Step-by-Step Testing Plan

### Step 1: Start Anvil (Mainnet Fork)

```bash
# Terminal 1 - Start Anvil
anvil --fork-url https://patient-serene-dew.quiknode.pro/3b4bd24cc31606cd06ac2932bad538ea2d322d7a/ \
      --port 8545 \
      --accounts 20 \
      --balance 10000
```

### Step 2: Upgrade Contracts to V2.2 (eigenlayer-contracts)

Since mainnet doesn't have V2.2 contracts deployed yet, use PR #1667 upgrade script:

```bash
# Terminal 2 - In eigenlayer-contracts repo
cd ~/eigenlayer-contracts

# Checkout PR #1667 (upgrade script)
git fetch origin pull/1667/head:pr-1667
git checkout pr-1667
forge build

# Run the upgrade script via zeus
# This deploys new implementations and upgrades proxies on the Anvil fork
zeus run --env mainnet --command "forge script script/releases/v1.0.0-slashing/1-deployContracts.s.sol --rpc-url http://localhost:8545 --broadcast -vvv"

# Or if PR #1667 has a different script path, use that instead
# Check the PR for the exact script location and command
```

**Note**: PR #1667 contains the official upgrade script for mainnet. Check the PR description for:
- Exact script path (e.g., `script/releases/...`)
- Required environment variables
- Zeus command format

### Step 3: Seed Test Data (eigenlayer-contracts)

After contracts are upgraded, seed test data:

```bash
# Option A: Use IntegrationDeployer tests to seed data
# These tests create operators, stakers, deposits, delegations
zeus run --env mainnet --command "forge test --match-test testFuzz_deposit --fork-url http://localhost:8545 -vvv"

# Option B: Write a custom seeding script (similar to seedPreprod but for mainnet fork)
# Create script/utils/seedMainnetFork/SeedTestData.s.sol
zeus run --env mainnet --command "forge script script/utils/seedMainnetFork/SeedTestData.s.sol --rpc-url http://localhost:8545 --broadcast -vvv"

# Option C: Direct contract calls via cast
# Register operator
cast send 0x39053D51B77DC0d36036Fc1fCc8Cb819df8Ef37A "registerAsOperator((address,address,uint32),string)" \
    "(0xYourAddress,0x0000000000000000000000000000000000000000,0)" "" \
    --rpc-url http://localhost:8545 \
    --private-key $ANVIL_PRIVATE_KEY

# Deposit into strategy
cast send 0x858646372CC42E1A627fcE94aa7A7033e7CF075A "depositIntoStrategy(address,address,uint256)" \
    <strategy> <token> <amount> \
    --rpc-url http://localhost:8545 \
    --private-key $ANVIL_PRIVATE_KEY
```

### Step 4: Run Sidecar Tests (sidecar)

```bash
# Terminal 2 (or 3) - In sidecar repo
cd ~/sidecar/scripts/anvil_e2e_tests

# Run all E2E tests
./run_sidecar_tests.sh

# Or run specific test
./run_sidecar_tests.sh Test_E2E_Anvil_SSS_1
```

## Test Scenarios to Seed

### SSS Scenarios (Staker Share Snapshots)

| Scenario | Contract Calls Needed |
|----------|----------------------|
| SSS-1: Deposit + Queue Withdrawal | `StrategyManager.depositIntoStrategy()` → `DelegationManager.queueWithdrawals()` |
| SSS-2: Deposit Day 1, Withdraw Day 2 | Deposit → `vm.warp()` → Queue withdrawal |
| SSS-3: Partial Withdrawal | Deposit → Queue partial withdrawal |
| SSS-5: Deposit + Slash | Deposit + delegate → `AllocationManager.slashOperator()` |
| SSS-8: Multiple Slashes | Multiple `slashOperator()` calls with time advances |

### OAS Scenarios (Operator Allocation Snapshots)

| Scenario | Contract Calls Needed |
|----------|----------------------|
| OAS-1: Basic Allocation | `AllocationManager.modifyAllocations()` |
| OAS-6: Same-day Alloc/Dealloc | `modifyAllocations()` (allocate) → `modifyAllocations()` (deallocate) same block |
| OAS-18: Slash During Deallocation | Allocate → Start deallocation → `slashOperator()` |

### Using IntegrationDeployer Helpers

From `eigenlayer-contracts/src/test/integration/IntegrationDeployer.t.sol`:

```solidity
// Create random staker with assets
User staker = _newRandomStaker();

// Create and register operator
User operator = _newRandomOperator();

// Staker deposits and delegates
staker.depositIntoEigenlayer(staker.assets());
staker.delegateTo(operator);

// Operator allocates to operator set
operator.modifyAllocations(operatorSet, strategies, magnitudes);

// Queue withdrawal
staker.queueWithdrawals(strategies, shares);

// Slash operator
avs.slashOperator(operator, strategies, wadToSlash);

// Time manipulation
timeMachine.warpToPresent();
timeMachine.advanceEpoch();
```

### Direct Contract Calls (via cast)

```bash
# Get Anvil test account private key
ANVIL_PRIVATE_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

# Register as operator
cast send $DELEGATION_MANAGER "registerAsOperator((address,address,uint32),string)" \
    "($OPERATOR_ADDRESS,0x0000000000000000000000000000000000000000,0)" "" \
    --rpc-url http://localhost:8545 --private-key $ANVIL_PRIVATE_KEY

# Deposit (after approving token)
cast send $STRATEGY_MANAGER "depositIntoStrategy(address,address,uint256)" \
    $STRATEGY $TOKEN $AMOUNT \
    --rpc-url http://localhost:8545 --private-key $ANVIL_PRIVATE_KEY

# Delegate to operator
cast send $DELEGATION_MANAGER "delegateTo(address,(bytes,uint256),bytes32)" \
    $OPERATOR "(0x,0)" 0x0 \
    --rpc-url http://localhost:8545 --private-key $ANVIL_PRIVATE_KEY

# Allocate (V2.2)
cast send $ALLOCATION_MANAGER "modifyAllocations(address,(address,uint32,address[],uint64[])[])" \
    $OPERATOR "[($AVS,$OPSET_ID,[$STRATEGY],[$MAGNITUDE])]" \
    --rpc-url http://localhost:8545 --private-key $ANVIL_PRIVATE_KEY
```

## Quick Test Run

### Minimal Test (Connection Only)

```bash
# 1. Start Anvil
anvil --fork-url $MAINNET_RPC_URL --port 8545

# 2. Run connection test (no seeding needed)
cd ~/sidecar/scripts/anvil_e2e_tests
./run_sidecar_tests.sh Test_E2E_Anvil_Connection
```

### Full Test with Data

```bash
# 1. Start Anvil with mainnet fork
anvil --fork-url $MAINNET_RPC_URL --port 8545

# 2. Upgrade contracts to V2.2 using PR #1667
cd ~/eigenlayer-contracts
git fetch origin pull/1667/head:pr-1667
git checkout pr-1667
forge build
# Run upgrade script (check PR #1667 for exact command)
zeus run --env mainnet --command "forge script <script-from-pr-1667> --rpc-url http://localhost:8545 --broadcast -vvv"

# 3. Seed test data (from eigenlayer-contracts)
# Use integration tests or custom seeding script
zeus run --env mainnet --command "forge test --match-test testFuzz_deposit --fork-url http://localhost:8545 -vvv"

# 4. Run sidecar tests
cd ~/sidecar/scripts/anvil_e2e_tests
./run_sidecar_tests.sh Test_E2E_Anvil_Full
```

## Environment Variables

### eigenlayer-contracts

Zeus handles most environment variables. Key ones:
- `ZEUS_ENV=mainnet` (set by zeus run --env)
- RPC URL passed via `--rpc-url` flag

### sidecar

```bash
export ANVIL_RPC=http://localhost:8545
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export TEST_E2E_ANVIL=true
export SIDECAR_CHAIN=mainnet
export SIDECAR_REWARDS_V2_2_ENABLED=true
```

## Troubleshooting

### Zeus not finding environment

```bash
# Check zeus is configured
zeus --version

# List available environments
zeus env list

# Verify mainnet env exists
zeus env show mainnet
```

### Anvil connection issues

```bash
# Verify Anvil is running
cast block-number --rpc-url http://localhost:8545

# Check chain ID (should be 1 for mainnet fork)
cast chain-id --rpc-url http://localhost:8545
```

### Sidecar test failures

```bash
# Run with verbose output
TEST_E2E_ANVIL=true go test -v ./pkg/rewards -run Test_E2E_Anvil -timeout 30m

# Check if events were indexed
psql -h localhost -U postgres -d sidecar_e2e_test -c "SELECT COUNT(*) FROM staker_share_deltas;"
```

## File Locations

### eigenlayer-contracts (contract upgrades & seeding)
- **PR #1667** - Mainnet upgrade script (use this for Step 2)
- `script/releases/Env.sol` - Zeus environment helpers
- `src/test/integration/IntegrationDeployer.t.sol` - Integration test base with helpers

### sidecar (validation tests)
- `scripts/anvil_e2e_tests/run_sidecar_tests.sh` - Test runner
- `pkg/rewards/e2e_anvil_test.go` - Go E2E tests

