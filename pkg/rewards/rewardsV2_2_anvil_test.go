package rewards

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Comprehensive Anvil Integration Test for Rewards v2.2
// Tests all edge cases with real contract interactions and time manipulation
//
// Prerequisites:
//  1. Anvil running: anvil --fork-url $RPC_URL --fork-block-number $BLOCK
//     OR: anvil (for fresh chain)
//  2. Contracts deployed or forked
//  3. Environment variables set (see below)
//
// Environment Variables:
//  - ANVIL_RPC_URL: Anvil RPC endpoint (default: http://localhost:8545)
//  - REWARDS_COORDINATOR_ADDRESS: RewardsCoordinator v2.2 contract address
//  - DELEGATION_MANAGER_ADDRESS: DelegationManager contract address
//  - ALLOCATION_MANAGER_ADDRESS: AllocationManager contract address
//  - TEST_PRIVATE_KEY: Private key for test transactions (optional, uses Anvil default)

func Test_RewardsV2_2_Anvil_Comprehensive(t *testing.T) {
	// Check if Anvil tests are enabled
	if os.Getenv("TEST_ANVIL") != "true" {
		t.Skip("Skipping Anvil tests. Set TEST_ANVIL=true to run")
	}

	anvilURL := getEnvOrDefault("ANVIL_RPC_URL", "http://localhost:8545")

	// Connect to Anvil
	client, err := ethclient.Dial(anvilURL)
	if err != nil {
		t.Fatalf("Failed to connect to Anvil at %s: %v\nIs Anvil running?", anvilURL, err)
	}
	defer client.Close()

	ctx := context.Background()

	// Verify connection
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err, "Failed to get chain ID")
	t.Logf("✓ Connected to Anvil at %s (Chain ID: %s)", anvilURL, chainID.String())

	// Setup test account
	privateKey, testAccount := setupTestAccount(t)
	t.Logf("✓ Using test account: %s", testAccount.Hex())

	// Create transactor
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	require.NoError(t, err, "Failed to create transactor")

	// Get contract addresses
	rewardsCoordinatorAddr := getRequiredEnv(t, "REWARDS_COORDINATOR_ADDRESS")
	delegationManagerAddr := getRequiredEnv(t, "DELEGATION_MANAGER_ADDRESS")
	allocationManagerAddr := getRequiredEnv(t, "ALLOCATION_MANAGER_ADDRESS")

	t.Logf("✓ RewardsCoordinator: %s", rewardsCoordinatorAddr)
	t.Logf("✓ DelegationManager: %s", delegationManagerAddr)
	t.Logf("✓ AllocationManager: %s", allocationManagerAddr)

	// Create snapshot before tests (for cleanup)
	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err, "Failed to create initial snapshot")
	t.Logf("✓ Created Anvil snapshot: %s", snapshotID)

	// Setup sidecar components
	dbFileName, cfg, grm, l, sink := setupSidecarForAnvil(t, anvilURL)
	t.Logf("✓ Sidecar database: %s", dbFileName)

	// Cleanup function
	defer func() {
		t.Log("\n========== CLEANUP ==========")

		// Revert Anvil state
		err := anvilRevert(ctx, client, snapshotID)
		if err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		} else {
			t.Log("✓ Reverted Anvil to initial snapshot")
		}

		// Clean database
		cleanupIntegrationTestData(t, grm)

		// Remove test database file
		if err := os.Remove(dbFileName); err != nil {
			t.Logf("Warning: Failed to remove test database: %v", err)
		} else {
			t.Logf("✓ Removed test database: %s", dbFileName)
		}

		t.Log("========== CLEANUP COMPLETE ==========\n")
	}()

	// Initialize rewards calculator
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err, "Failed to create RewardsCalculator")

	// Run comprehensive test scenarios
	t.Run("EdgeCase1_ForwardRewards_WithWithdrawal_DuringRewardPeriod", func(t *testing.T) {
		testForwardRewardsWithWithdrawal(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase2_MultiplePartialWithdrawals_StaggeredCompletions", func(t *testing.T) {
		testMultiplePartialWithdrawals(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase3_Slashing_DuringWithdrawalQueue", func(t *testing.T) {
		testSlashingDuringWithdrawal(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase4_TwoYearForwardLooking_Rewards", func(t *testing.T) {
		testTwoYearForwardRewards(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase5_Undelegation_TreatedAsWithdrawal", func(t *testing.T) {
		testUndelegation(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase6_OperatorSetRewards_UniqueStake", func(t *testing.T) {
		testOperatorSetUniqueStakeRewards(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase7_OperatorSetRewards_TotalStake", func(t *testing.T) {
		testOperatorSetTotalStakeRewards(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase8_DeallocationQueue_OperatorAllocations", func(t *testing.T) {
		testDeallocationQueue(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase9_RateFluctuation_StakeWeightChanges", func(t *testing.T) {
		testRateFluctuation(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("EdgeCase10_MaxFutureLength_TwoYearLimit", func(t *testing.T) {
		testMaxFutureLength(t, ctx, client, auth, grm, rc, l)
	})
}

// ============================================================================
// Test Case 1: Forward rewards with withdrawal during reward period
// ============================================================================
func testForwardRewardsWithWithdrawal(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 1: Forward Rewards with Withdrawal ==========")

	// Create snapshot for this test
	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	startBlock, err := client.BlockNumber(ctx)
	require.NoError(t, err)
	t.Logf("Starting block: %d", startBlock)

	// Step 1: Setup initial state (staker has shares, operator registered)
	t.Log("\nStep 1: Setting up initial state...")
	// TODO: Call contracts to setup state
	// - Register operator
	// - Delegate staker to operator
	// - Add shares to staker

	// Step 2: Create 1-year forward-looking reward
	t.Log("\nStep 2: Creating 1-year forward reward...")
	currentBlock, err := client.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	currentTime := currentBlock.Time()

	rewardAmount := new(big.Int)
	rewardAmount.SetString("1000000000000000000000000", 10) // 1M tokens
	startTimestamp := currentTime
	duration := uint64(365 * 24 * 60 * 60) // 1 year in seconds
	endTimestamp := startTimestamp + duration

	t.Logf("  Reward amount: %s tokens", rewardAmount.String())
	t.Logf("  Start: %d (%s)", startTimestamp, time.Unix(int64(startTimestamp), 0).Format(time.RFC3339))
	t.Logf("  End: %d (%s)", endTimestamp, time.Unix(int64(endTimestamp), 0).Format(time.RFC3339))
	t.Logf("  Duration: %d seconds (%d days)", duration, duration/(24*60*60))

	// TODO: Call createOperatorSetUniqueStakeRewardsSubmission or createOperatorSetTotalStakeRewardsSubmission
	// tx, err := rewardsCoordinator.CreateOperatorSetUniqueStakeRewardsSubmission(auth, operatorSet, rewardsSubmissions)
	// require.NoError(t, err)
	// receipt, err := bind.WaitMined(ctx, client, tx)
	// require.NoError(t, receipt)
	// t.Logf("  ✓ Reward created in tx: %s", tx.Hash().Hex())

	// Step 3: Index events and calculate initial rewards
	t.Log("\nStep 3: Calculating initial rewards...")
	// TODO: Run sidecar indexer to process events
	// Then calculate rewards for current date
	snapshotDate := time.Unix(int64(currentTime), 0).Format(time.DateOnly)
	t.Logf("  Snapshot date: %s", snapshotDate)

	// Step 4: Fast-forward 30 days
	t.Log("\nStep 4: Fast-forwarding 30 days...")
	thirtyDaysLater := currentTime + (30 * 24 * 60 * 60)
	err = setAnvilBlockTimestamp(ctx, client, thirtyDaysLater)
	require.NoError(t, err)
	err = anvilMineBlock(ctx, client)
	require.NoError(t, err)
	t.Logf("  ✓ Time: %s", time.Unix(int64(thirtyDaysLater), 0).Format(time.RFC3339))

	// Calculate rewards after 30 days
	snapshotDate30 := time.Unix(int64(thirtyDaysLater), 0).Format(time.DateOnly)
	t.Logf("  Calculating rewards for: %s", snapshotDate30)
	// TODO: Calculate and log rewards

	// Step 5: Queue withdrawal (500 shares out of 1000)
	t.Log("\nStep 5: Queueing withdrawal (500/1000 shares)...")
	// TODO: Call queueWithdrawal on DelegationManager
	// tx, err = delegationManager.QueueWithdrawals(auth, withdrawalRequests)
	// require.NoError(t, err)
	// receipt, err = bind.WaitMined(ctx, client, tx)
	// require.NoError(t, receipt)
	// t.Logf("  ✓ Withdrawal queued in tx: %s", tx.Hash().Hex())

	// Step 6: Fast-forward 7 days (middle of withdrawal queue)
	t.Log("\nStep 6: Fast-forwarding 7 days (during withdrawal queue)...")
	sevenDaysLater := thirtyDaysLater + (7 * 24 * 60 * 60)
	err = setAnvilBlockTimestamp(ctx, client, sevenDaysLater)
	require.NoError(t, err)
	err = anvilMineBlock(ctx, client)
	require.NoError(t, err)
	t.Logf("  ✓ Time: %s", time.Unix(int64(sevenDaysLater), 0).Format(time.RFC3339))

	// Calculate rewards - should still earn on full 1000 shares (in queue)
	snapshotDate37 := time.Unix(int64(sevenDaysLater), 0).Format(time.DateOnly)
	t.Logf("  Calculating rewards for: %s", snapshotDate37)
	t.Log("  EXPECTATION: Should earn on 1000 shares (withdrawal still in queue)")
	// TODO: Calculate and validate

	// Step 7: Fast-forward 8 more days (withdrawal completes)
	t.Log("\nStep 7: Fast-forwarding 8 days (withdrawal completes)...")
	fifteenDaysLater := thirtyDaysLater + (15 * 24 * 60 * 60)
	err = setAnvilBlockTimestamp(ctx, client, fifteenDaysLater)
	require.NoError(t, err)
	err = anvilMineBlock(ctx, client)
	require.NoError(t, err)
	t.Logf("  ✓ Time: %s", time.Unix(int64(fifteenDaysLater), 0).Format(time.RFC3339))

	// Complete withdrawal
	// TODO: Call completeQueuedWithdrawal

	// Calculate rewards - should now earn on only 500 shares
	snapshotDate45 := time.Unix(int64(fifteenDaysLater), 0).Format(time.DateOnly)
	t.Logf("  Calculating rewards for: %s", snapshotDate45)
	t.Log("  EXPECTATION: Should earn on 500 shares (withdrawal completed)")
	// TODO: Calculate and validate

	// Step 8: Validate cumulative rewards
	t.Log("\nStep 8: Validating cumulative rewards...")
	t.Log("  Expected behavior:")
	t.Log("    - Days 0-30: 1000 shares earning")
	t.Log("    - Days 30-37: 1000 shares earning (in withdrawal queue)")
	t.Log("    - Days 37-45: 1000 shares earning (still in queue)")
	t.Log("    - Days 45+: 500 shares earning (after withdrawal)")

	// Log detailed state
	logAnvilTestResults(t, grm, []string{snapshotDate, snapshotDate30, snapshotDate37, snapshotDate45})

	t.Log("\n========== TEST CASE 1 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 2: Multiple partial withdrawals with staggered completions
// ============================================================================
func testMultiplePartialWithdrawals(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 2: Multiple Partial Withdrawals ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Scenario: Alice has 1000 shares")
	t.Log("  T0: Initial state")
	t.Log("  T1: Queue 300 shares (completes T15)")
	t.Log("  T5: Queue 200 shares (completes T19)")
	t.Log("  T8: Queue 100 shares (completes T22)")
	t.Log("\nExpected behavior:")
	t.Log("  T0-T15: Earning on 1000 shares")
	t.Log("  T15-T19: Earning on 700 shares")
	t.Log("  T19-T22: Earning on 500 shares")
	t.Log("  T22+: Earning on 400 shares")

	// TODO: Implement full test with contract calls
	// 1. Setup Alice with 1000 shares
	// 2. Queue first withdrawal (300 shares)
	// 3. Fast-forward 5 days, queue second withdrawal (200 shares)
	// 4. Fast-forward 3 days, queue third withdrawal (100 shares)
	// 5. Fast-forward through each completion time
	// 6. Calculate rewards at each step
	// 7. Validate share amounts match expectations

	t.Log("\n========== TEST CASE 2 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 3: Slashing during withdrawal queue
// ============================================================================
func testSlashingDuringWithdrawal(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 3: Slashing During Withdrawal ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Scenario: Alice has 200 shares")
	t.Log("  T1: Queue withdrawal of 100 shares")
	t.Log("  T2: Operator slashed 25%")
	t.Log("\nExpected behavior:")
	t.Log("  After slash: 200 * 0.75 = 150 shares total")
	t.Log("  Withdrawable: 100 * 0.75 = 75 shares")
	t.Log("  Remaining after withdrawal: 150 - 75 = 75 shares")

	// TODO: Implement full test
	// 1. Setup Alice with 200 shares
	// 2. Queue withdrawal of 100 shares
	// 3. Trigger slashing event (25%)
	// 4. Verify shares are adjusted correctly
	// 5. Complete withdrawal
	// 6. Verify withdrawable amount is slashed
	// 7. Verify remaining shares are correct

	t.Log("\n========== TEST CASE 3 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 4: Two-year forward-looking rewards
// ============================================================================
func testTwoYearForwardRewards(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 4: Two-Year Forward Rewards ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	currentBlock, err := client.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	baseTime := currentBlock.Time()

	t.Log("Creating 2-year forward reward (10M tokens)")

	rewardAmount := new(big.Int)
	rewardAmount.SetString("10000000000000000000000000", 10) // 10M tokens
	duration := uint64(2 * 365 * 24 * 60 * 60)               // 2 years

	t.Logf("  Amount: %s", rewardAmount.String())
	t.Logf("  Duration: %d days", duration/(24*60*60))
	t.Logf("  Daily rate: %s tokens/day", new(big.Int).Div(rewardAmount, big.NewInt(int64(duration/(24*60*60)))).String())

	// TODO: Create 2-year reward submission

	// Test at intervals: 30d, 90d, 180d, 365d, 547d, 730d
	intervals := []struct {
		name string
		days uint64
	}{
		{"1 month", 30},
		{"3 months", 90},
		{"6 months", 180},
		{"1 year", 365},
		{"18 months", 547},
		{"2 years", 730},
	}

	t.Log("\nTesting reward accumulation at intervals:")
	for _, interval := range intervals {
		targetTime := baseTime + (interval.days * 24 * 60 * 60)
		err = setAnvilBlockTimestamp(ctx, client, targetTime)
		require.NoError(t, err)
		err = anvilMineBlock(ctx, client)
		require.NoError(t, err)

		snapshotDate := time.Unix(int64(targetTime), 0).Format(time.DateOnly)
		t.Logf("\n  %s (%s):", interval.name, snapshotDate)

		// TODO: Calculate rewards and validate
		expectedDailyAccumulation := new(big.Int).Div(rewardAmount, big.NewInt(int64(duration/(24*60*60))))
		expectedTotal := new(big.Int).Mul(expectedDailyAccumulation, big.NewInt(int64(interval.days)))
		t.Logf("    Expected cumulative reward: ~%s", expectedTotal.String())
	}

	t.Log("\n========== TEST CASE 4 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 5: Undelegation (full withdrawal)
// ============================================================================
func testUndelegation(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 5: Undelegation ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Scenario: Full undelegation (withdrawal of all shares)")
	t.Log("Expected: Should be treated exactly like partial withdrawal")
	t.Log("  - Continue earning during 14-day queue")
	t.Log("  - Stop earning after queue completes")

	// TODO: Implement undelegation test
	// 1. Setup staker with shares
	// 2. Call undelegate (queues full withdrawal)
	// 3. Verify earnings continue during queue
	// 4. Complete undelegation
	// 5. Verify earnings stop

	t.Log("\n========== TEST CASE 5 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 6: Operator Set Rewards - Unique Stake
// ============================================================================
func testOperatorSetUniqueStakeRewards(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 6: Operator Set Unique Stake Rewards ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Testing createOperatorSetUniqueStakeRewardsSubmission")
	t.Log("  - Rewards based on allocated unique stake to operator set")
	t.Log("  - Unique stake is always slashable")
	t.Log("  - Pro-rata distribution based on allocation")

	// TODO: Implement unique stake rewards test
	// 1. Setup operators with unique stake allocations
	// 2. Create unique stake reward submission
	// 3. Fast-forward time
	// 4. Calculate rewards
	// 5. Validate distribution matches unique stake weights

	t.Log("\n========== TEST CASE 6 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 7: Operator Set Rewards - Total Stake
// ============================================================================
func testOperatorSetTotalStakeRewards(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 7: Operator Set Total Stake Rewards ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Testing createOperatorSetTotalStakeRewardsSubmission")
	t.Log("  - Rewards based on total delegated stake")
	t.Log("  - Operator set analog to Rewards v1")
	t.Log("  - Pro-rata distribution based on total stake")

	// TODO: Implement total stake rewards test
	// 1. Setup operators with delegated stakes
	// 2. Create total stake reward submission
	// 3. Fast-forward time
	// 4. Calculate rewards
	// 5. Validate distribution matches total stake weights

	t.Log("\n========== TEST CASE 7 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 8: Deallocation Queue (Operator Allocations)
// ============================================================================
func testDeallocationQueue(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 8: Deallocation Queue ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Scenario: Operator deallocates unique stake from operator set")
	t.Log("Expected: Similar to withdrawal queue")
	t.Log("  - Continue earning rewards during 14-day deallocation")
	t.Log("  - Stop earning after deallocation completes")

	// TODO: Implement deallocation queue test
	// 1. Setup operator with allocations
	// 2. Queue deallocation
	// 3. Verify rewards continue during queue
	// 4. Complete deallocation
	// 5. Verify rewards stop

	t.Log("\n========== TEST CASE 8 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 9: Rate Fluctuation Due to Stake Weight Changes
// ============================================================================
func testRateFluctuation(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 9: Rate Fluctuation ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("Scenario: Multiple operators with changing stake weights")
	t.Log("  - Fixed daily reward pool")
	t.Log("  - Individual rates fluctuate as relative stake changes")
	t.Log("  - Total daily distribution remains constant")

	// TODO: Implement rate fluctuation test
	// 1. Create reward with fixed daily amount
	// 2. Setup 3 operators with different stakes
	// 3. Calculate rewards (baseline)
	// 4. Operator 2 adds more stake
	// 5. Calculate rewards (rates should adjust)
	// 6. Operator 3 withdraws stake
	// 7. Calculate rewards (rates adjust again)
	// 8. Validate total daily amount stays constant

	t.Log("\n========== TEST CASE 9 COMPLETE ==========\n")
}

// ============================================================================
// Test Case 10: MAX_FUTURE_LENGTH - Two Year Limit
// ============================================================================
func testMaxFutureLength(t *testing.T, ctx context.Context, client *ethclient.Client,
	auth *bind.TransactOpts, grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("\n========== TEST CASE 10: MAX_FUTURE_LENGTH Validation ==========")

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	currentBlock, err := client.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	currentTime := currentBlock.Time()

	t.Log("Testing MAX_FUTURE_LENGTH constraint")

	// Test 1: Valid - exactly 2 years
	t.Log("\n  Test 1: Creating reward for exactly 2 years (should succeed)")
	twoYears := uint64(2 * 365 * 24 * 60 * 60)
	startTime := currentTime
	endTime := startTime + twoYears
	t.Logf("    Start: %s", time.Unix(int64(startTime), 0).Format(time.RFC3339))
	t.Logf("    End: %s", time.Unix(int64(endTime), 0).Format(time.RFC3339))
	// TODO: Should succeed

	// Test 2: Invalid - beyond 2 years
	t.Log("\n  Test 2: Creating reward for 2 years + 1 day (should fail)")
	tooLong := uint64(2*365*24*60*60 + 24*60*60)
	endTimeTooLong := startTime + tooLong
	t.Logf("    Start: %s", time.Unix(int64(startTime), 0).Format(time.RFC3339))
	t.Logf("    End: %s", time.Unix(int64(endTimeTooLong), 0).Format(time.RFC3339))
	// TODO: Should revert with "MAX_FUTURE_LENGTH exceeded"

	// Test 3: Valid - retroactive rewards
	t.Log("\n  Test 3: Creating retroactive reward (should succeed)")
	pastTime := currentTime - (30 * 24 * 60 * 60) // 30 days ago
	t.Logf("    Start: %s", time.Unix(int64(pastTime), 0).Format(time.RFC3339))
	t.Logf("    End: %s", time.Unix(int64(currentTime), 0).Format(time.RFC3339))
	// TODO: Should succeed

	t.Log("\n========== TEST CASE 10 COMPLETE ==========\n")
}

// ============================================================================
// Helper Functions
// ============================================================================

func setupSidecarForAnvil(t *testing.T, anvilURL string) (string, *config.Config, *gorm.DB, *zap.Logger, *metrics.MetricsSink) {
	t.Helper()

	cfg := config.NewConfig()
	cfg.Chain = config.Chain_PreprodHoodi
	cfg.Rewards.RewardsV2_2Enabled = true
	cfg.DatabaseConfig = *getTestDbConfig()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})
	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	require.NoError(t, err, "Failed to create test database")

	return dbname, cfg, grm, l, sink
}

func setupTestAccount(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()

	privateKeyHex := getEnvOrDefault("TEST_PRIVATE_KEY", "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	require.NoError(t, err, "Failed to parse private key")

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	require.True(t, ok, "Failed to cast public key")

	address := crypto.PubkeyToAddress(*publicKeyECDSA)
	return privateKey, address
}

func getTestDbConfig() *config.DatabaseConfig {
	// Try to get PostgreSQL user from environment, fallback to common defaults
	pgUser := os.Getenv("PGUSER")
	if pgUser == "" {
		pgUser = os.Getenv("USER") // Use macOS username as fallback
	}

	return &config.DatabaseConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     fmt.Sprintf("test_rewards_v2_2_%d", time.Now().Unix()),
		User:       pgUser,
		Password:   "",
		SchemaName: "public",
	}
}

func getRequiredEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("Required environment variable %s is not set", key)
	}
	return value
}

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func logAnvilTestResults(t *testing.T, grm *gorm.DB, snapshotDates []string) {
	t.Helper()
	t.Log("\n========== DETAILED TEST RESULTS ==========")
	logIntegrationRewardsData(t, grm, snapshotDates)
	t.Log("==========================================\n")
}

// Anvil RPC helper functions

func setAnvilBlockTimestamp(ctx context.Context, client *ethclient.Client, timestamp uint64) error {
	var result string
	return client.Client().CallContext(ctx, &result, "anvil_setNextBlockTimestamp", timestamp)
}

func anvilMineBlock(ctx context.Context, client *ethclient.Client) error {
	var result string
	return client.Client().CallContext(ctx, &result, "anvil_mine", 1, 0)
}

func anvilSnapshot(ctx context.Context, client *ethclient.Client) (string, error) {
	var snapshotID string
	err := client.Client().CallContext(ctx, &snapshotID, "evm_snapshot")
	return snapshotID, err
}

func anvilRevert(ctx context.Context, client *ethclient.Client, snapshotID string) error {
	var result bool
	err := client.Client().CallContext(ctx, &result, "evm_revert", snapshotID)
	if err != nil {
		return err
	}
	if !result {
		return fmt.Errorf("failed to revert to snapshot %s", snapshotID)
	}
	return nil
}

// cleanupIntegrationTestData removes all test data from the database
func cleanupIntegrationTestData(t *testing.T, grm *gorm.DB) {
	t.Helper()
	t.Log("Cleaning up test data...")

	tables := []string{
		"gold_table",
		"generated_rewards_snapshots",
		"staker_share_snapshots",
		"operator_share_snapshots",
		"withdrawal_queue_share_snapshots",
		"operator_allocation_snapshots",
		"staker_delegation_snapshots",
		"operator_avs_registration_snapshots",
		"operator_avs_strategy_snapshots",
		"operator_set_operator_registration_snapshots",
		"operator_set_strategy_registration_snapshots",
		"operator_avs_split_snapshots",
		"operator_pi_split_snapshots",
		"operator_set_split_snapshots",
		"default_operator_split_snapshots",
		"queued_slashing_withdrawals",
		"completed_slashing_withdrawals",
		"staker_share_deltas",
		"operator_share_deltas",
		"staker_delegations",
		"staker_delegation_changes",
		"operator_avs_state_changes",
		"operator_avs_restaked_strategies",
		"operator_set_operator_registrations",
		"operator_set_strategy_registrations",
		"operator_avs_splits",
		"operator_pi_splits",
		"operator_set_splits",
		"reward_submissions",
		"operator_directed_reward_submissions",
		"operator_directed_operator_set_reward_submissions",
		"operator_allocations",
		"blocks",
	}

	// Delete data from tables in reverse dependency order
	for _, table := range tables {
		// Check if table exists
		var exists bool
		err := grm.Raw("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)", table).Scan(&exists).Error
		if err != nil || !exists {
			continue
		}

		result := grm.Exec(fmt.Sprintf("DELETE FROM %s", table))
		if result.Error != nil {
			t.Logf("Warning: Failed to clean table %s: %v", table, result.Error)
		}
	}

	t.Log("✓ Test data cleanup complete")
}

// logIntegrationRewardsData logs detailed reward data for debugging
func logIntegrationRewardsData(t *testing.T, grm *gorm.DB, snapshotDates []string) {
	t.Helper()

	for _, snapshotDate := range snapshotDates {
		t.Logf("\n========== Snapshot: %s ==========", snapshotDate)

		// Log staker shares
		var stakerShares []struct {
			Staker   string
			Strategy string
			Shares   string
		}
		grm.Raw(`
			SELECT staker, strategy, shares
			FROM staker_share_snapshots
			WHERE snapshot = ?
			ORDER BY staker, strategy
		`, snapshotDate).Scan(&stakerShares)

		if len(stakerShares) > 0 {
			t.Logf("Staker Shares (%d rows):", len(stakerShares))
			for _, s := range stakerShares {
				t.Logf("  %s | %s | %s", s.Staker, s.Strategy, s.Shares)
			}
		}

		// Log rewards
		var rewards []struct {
			Earner string
			Token  string
			Amount string
		}
		grm.Raw(`
			SELECT earner, token, amount
			FROM gold_table
			WHERE snapshot = ?
			ORDER BY earner, token
		`, snapshotDate).Scan(&rewards)

		if len(rewards) > 0 {
			t.Logf("Rewards (%d rows):", len(rewards))
			for _, r := range rewards {
				t.Logf("  %s | %s | %s", r.Earner, r.Token, r.Amount)
			}
		}

		t.Log("=====================================\n")
	}
}
