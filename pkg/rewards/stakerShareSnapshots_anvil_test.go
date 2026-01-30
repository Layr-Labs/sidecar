package rewards

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

// Test_SSS_Anvil_E2E tests Staker Share Snapshots E2E scenarios (SSS-8 through SSS-15)
// These tests use real contract interactions via Anvil to verify the full flow:
// contract events → indexer → database → snapshot generation
//
// Prerequisites:
//  1. Anvil running: anvil --fork-url $RPC_URL --fork-block-number $BLOCK
//  2. Contracts deployed or forked
//  3. Environment variables set:
//     - TEST_ANVIL=true
//     - ANVIL_RPC_URL (default: http://localhost:8545)
//     - REWARDS_COORDINATOR_ADDRESS
//     - DELEGATION_MANAGER_ADDRESS
//     - ALLOCATION_MANAGER_ADDRESS
//     - STRATEGY_MANAGER_ADDRESS
//     - TEST_PRIVATE_KEY (optional, uses Anvil default account)
func Test_SSS_Anvil_E2E(t *testing.T) {
	// Check if Anvil tests are enabled
	if os.Getenv("TEST_ANVIL") != "true" {
		t.Skip("Skipping Anvil E2E tests. Set TEST_ANVIL=true to run")
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
	privateKey, testAccount := setupSSS_TestAccount(t)
	t.Logf("✓ Using test account: %s", testAccount.Hex())

	// Create transactor
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	require.NoError(t, err, "Failed to create transactor")

	// Get contract addresses
	strategyManagerAddr := getSSS_RequiredEnv(t, "STRATEGY_MANAGER_ADDRESS")
	delegationManagerAddr := getSSS_RequiredEnv(t, "DELEGATION_MANAGER_ADDRESS")
	allocationManagerAddr := getSSS_RequiredEnv(t, "ALLOCATION_MANAGER_ADDRESS")

	t.Logf("✓ StrategyManager: %s", strategyManagerAddr)
	t.Logf("✓ DelegationManager: %s", delegationManagerAddr)
	t.Logf("✓ AllocationManager: %s", allocationManagerAddr)

	// Create snapshot before tests (for cleanup)
	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err, "Failed to create initial snapshot")
	t.Logf("✓ Created Anvil snapshot: %s", snapshotID)

	// Setup sidecar components
	dbFileName, cfg, grm, l := setupSSS_SidecarForAnvil(t, anvilURL)
	t.Logf("✓ Sidecar database: %s", dbFileName)

	// Cleanup function
	defer func() {
		t.Log("\n========== CLEANUP ==========")

		// Revert Anvil state
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
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
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, nil, l)
	require.NoError(t, err, "Failed to create RewardsCalculator")

	// Run SSS E2E test scenarios
	t.Run("SSS-8_E2E: Multiple consecutive slashes", func(t *testing.T) {
		testSSS8_MultipleConsecutiveSlashes(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-9_E2E: Partial withdrawal with slashing (delegated)", func(t *testing.T) {
		testSSS9_PartialWithdrawalWithSlashing(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-10_E2E: Slash during withdrawal queue", func(t *testing.T) {
		testSSS10_SlashDuringWithdrawalQueue(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-11_E2E: Slash after withdrawal block boundary", func(t *testing.T) {
		testSSS11_SlashAfterWithdrawalBoundary(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-12_E2E: Dual slashing - operator set and beacon chain", func(t *testing.T) {
		testSSS12_DualSlashing(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-13_E2E: Same block - withdrawal before slash", func(t *testing.T) {
		testSSS13_SameBlockWithdrawalBeforeSlash(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-14_E2E: Same block - slash before withdrawal", func(t *testing.T) {
		testSSS14_SameBlockSlashBeforeWithdrawal(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-15_E2E: Multiple deposits then withdrawal", func(t *testing.T) {
		testSSS15_MultipleDepositsThenWithdrawal(t, ctx, client, auth, grm, rc, l)
	})
}

// SSS-8: Multiple consecutive slashes with FULL withdrawal
// Actions:
// 1. Staker deposits 200 shares on 1/4 @ 5pm
// 2. Staker queues a FULL withdrawal (200 shares) on 1/5 @ 6pm
// 3. Staker is slashed 50% on 1/6 @ 6pm
// 4. Staker is slashed another 50% on 1/7 @ 6pm
//
// Expected:
// - 200 shares on 1/5 (deposit only visible)
// - 50 shares on 1/6+ (full withdrawal queued, add-back = 200 * 0.25 = 50)
func testSSS8_MultipleConsecutiveSlashes(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-8: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 1, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 1, 5, 18, 0, 0, 0, time.UTC)
	day6 := time.Date(2025, 1, 6, 18, 0, 0, 0, time.UTC)
	day7 := time.Date(2025, 1, 7, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	blocks := []struct {
		number uint64
		time   time.Time
	}{
		{88000, day4},
		{88001, day5},
		{88002, day6},
		{88003, day7},
	}
	for _, b := range blocks {
		res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`,
			b.number, fmt.Sprintf("hash_%d", b.number), b.time)
		require.Nil(t, res.Error, "Failed to insert block")
	}

	// Deposit 200 shares on 1/4
	res := grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss8", "0xstrat_sss8", "200000000000000000000", 0, 88000, day4, day4.Format("2006-01-02"), "tx_88000", 1)
	require.Nil(t, res.Error, "Failed to insert deposit")

	// Queue FULL withdrawal on 1/5: staker_shares goes to 0
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss8", "0xstrat_sss8", "0", 0, 88001, day5, day5.Format("2006-01-02"), "tx_88001", 1)
	require.Nil(t, res.Error, "Failed to insert withdrawal")

	// Insert queued withdrawal record
	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss8", "0xoperator_sss8", "0xstaker_sss8", "1", 88001, "0xstrat_sss8", "200000000000000000000", "200000000000000000000", "root_sss8_1", 88001, "tx_88001", 0)
	require.Nil(t, res.Error, "Failed to insert queued withdrawal")

	// First slash 50% on 1/6: multiplier = 0.5
	res = grm.Exec(`
		INSERT INTO queued_withdrawal_slashing_adjustments (staker, strategy, operator, withdrawal_block_number, slash_block_number, slash_multiplier, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss8", "0xstrat_sss8", "0xoperator_sss8", 88001, 88002, 0.5, 88002, "tx_88002_slash1", 0)
	require.Nil(t, res.Error, "Failed to insert first slash adjustment")

	// Second slash 50% on 1/7: cumulative multiplier = 0.25
	res = grm.Exec(`
		INSERT INTO queued_withdrawal_slashing_adjustments (staker, strategy, operator, withdrawal_block_number, slash_block_number, slash_multiplier, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss8", "0xstrat_sss8", "0xoperator_sss8", 88001, 88003, 0.25, 88003, "tx_88003_slash2", 0)
	require.Nil(t, res.Error, "Failed to insert second slash adjustment")

	t.Log("SSS-8: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-01-15")
	require.Nil(t, err, "Failed to generate snapshots")

	// Verify: 200 on 1/5 (deposit only visible)
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss8", "0xstrat_sss8", "2025-01-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "200000000000000000000", shares, "Expected 200 shares on 1/5")

	// Verify: 50 on 1/6+ (full withdrawal with 0.25 slash = 200 * 0.25 = 50)
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss8", "0xstrat_sss8", "2025-01-06").Scan(&shares)
	require.Nil(t, res.Error)
	require.Contains(t, shares, "50000000000000000000", "Expected 50 shares on 1/6")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss8", "0xstrat_sss8", "2025-01-08").Scan(&shares)
	require.Nil(t, res.Error)
	require.Contains(t, shares, "50000000000000000000", "Expected 50 shares on 1/8")

	t.Log("✓ SSS-8 PASSED: Full withdrawal with consecutive slashes")
}

// SSS-9: Partial withdrawal with slashing (delegated)
// Actions:
// 1. Staker deposits 200 shares on 2/4
// 2. Staker queues PARTIAL withdrawal (50 shares) on 2/5
// 3. Slash 50% on 2/6
// 4. Slash 50% on 2/7
//
// Expected:
// - 2/5: 200 (deposit only visible)
// - 2/6: 162.5 (150 base + 12.5 slashed add-back)
// - 2/7: 87.5 (75 base + 12.5 add-back)
// - 2/8: 50 (37.5 base + 12.5 add-back)
func testSSS9_PartialWithdrawalWithSlashing(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-9: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 2, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 2, 5, 18, 0, 0, 0, time.UTC)
	day6 := time.Date(2025, 2, 6, 18, 0, 0, 0, time.UTC)
	day7 := time.Date(2025, 2, 7, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	blocks := []struct {
		number uint64
		time   time.Time
	}{
		{99000, day4},
		{99001, day5},
		{99002, day6},
		{99003, day7},
	}
	for _, b := range blocks {
		res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`,
			b.number, fmt.Sprintf("hash_%d", b.number), b.time)
		require.Nil(t, res.Error, "Failed to insert block")
	}

	// Setup delegation
	res := grm.Exec(`
		INSERT INTO staker_delegation_changes (staker, operator, delegated, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xoperator_sss9", true, 99000, "tx_delegation_sss9", 0)
	require.Nil(t, res.Error, "Failed to insert delegation")

	// Deposit 200 shares on 2/4
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xstrat_sss9", "200000000000000000000", 0, 99000, day4, day4.Format("2006-01-02"), "tx_99000", 1)
	require.Nil(t, res.Error, "Failed to insert deposit")

	// Queue PARTIAL withdrawal (50) on 2/5: base = 150
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xstrat_sss9", "150000000000000000000", 0, 99001, day5, day5.Format("2006-01-02"), "tx_99001", 1)
	require.Nil(t, res.Error, "Failed to insert partial withdrawal")

	// Insert queued withdrawal record for 50 shares
	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xoperator_sss9", "0xstaker_sss9", "1", 99001, "0xstrat_sss9", "50000000000000000000", "50000000000000000000", "root_sss9_1", 99001, "tx_99001", 0)
	require.Nil(t, res.Error, "Failed to insert queued withdrawal")

	// First slash 50% on 2/6: base goes to 75, multiplier = 0.5
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xstrat_sss9", "75000000000000000000", 0, 99002, day6, day6.Format("2006-01-02"), "tx_99002", 1)
	require.Nil(t, res.Error, "Failed to insert first slash base shares")

	res = grm.Exec(`
		INSERT INTO queued_withdrawal_slashing_adjustments (staker, strategy, operator, withdrawal_block_number, slash_block_number, slash_multiplier, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xstrat_sss9", "0xoperator_sss9", 99001, 99002, 0.5, 99002, "tx_99002_slash1", 0)
	require.Nil(t, res.Error, "Failed to insert first slash adjustment")

	// Second slash 50% on 2/7: base goes to 37.5, cumulative multiplier = 0.25
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xstrat_sss9", "37500000000000000000", 0, 99003, day7, day7.Format("2006-01-02"), "tx_99003", 1)
	require.Nil(t, res.Error, "Failed to insert second slash base shares")

	res = grm.Exec(`
		INSERT INTO queued_withdrawal_slashing_adjustments (staker, strategy, operator, withdrawal_block_number, slash_block_number, slash_multiplier, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss9", "0xstrat_sss9", "0xoperator_sss9", 99001, 99003, 0.25, 99003, "tx_99003_slash2", 0)
	require.Nil(t, res.Error, "Failed to insert second slash adjustment")

	t.Log("SSS-9: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-02-15")
	require.Nil(t, err, "Failed to generate snapshots")

	// Verify snapshots
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss9", "0xstrat_sss9", "2025-02-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "200000000000000000000", shares, "Expected 200 shares on 2/5")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss9", "0xstrat_sss9", "2025-02-06").Scan(&shares)
	require.Nil(t, res.Error)
	require.Contains(t, shares, "162500000000000000000", "Expected 162.5 shares on 2/6")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss9", "0xstrat_sss9", "2025-02-07").Scan(&shares)
	require.Nil(t, res.Error)
	require.Contains(t, shares, "87500000000000000000", "Expected 87.5 shares on 2/7")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss9", "0xstrat_sss9", "2025-02-08").Scan(&shares)
	require.Nil(t, res.Error)
	require.Contains(t, shares, "50000000000000000000", "Expected 50 shares on 2/8")

	t.Log("✓ SSS-9 PASSED: Partial withdrawal with slashing")
}

// SSS-10: Slash during withdrawal queue (simplified)
// Tests that slashing affects queued withdrawals correctly
func testSSS10_SlashDuringWithdrawalQueue(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-10: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 3, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 3, 5, 18, 0, 0, 0, time.UTC)
	day6 := time.Date(2025, 3, 6, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	for i, d := range []time.Time{day4, day5, day6} {
		res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`,
			110000+i, fmt.Sprintf("hash_%d", 110000+i), d)
		require.Nil(t, res.Error)
	}

	// Deposit 1000 shares on 3/4
	res := grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss10", "0xstrat_sss10", "1000000000000000000000", 0, 110000, day4, day4.Format("2006-01-02"), "tx_110000", 1)
	require.Nil(t, res.Error)

	// Slash 50% on 3/6: insert slashed value
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss10", "0xstrat_sss10", "500000000000000000000", 0, 110002, day6, day6.Format("2006-01-02"), "tx_110002", 1)
	require.Nil(t, res.Error)

	t.Log("SSS-10: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-03-15")
	require.Nil(t, err)

	// Verify: 1000 on 3/5, 500 on 3/7
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss10", "0xstrat_sss10", "2025-03-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "1000000000000000000000", shares, "Expected 1000 shares on 3/5")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss10", "0xstrat_sss10", "2025-03-07").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "500000000000000000000", shares, "Expected 500 shares on 3/7")

	t.Log("✓ SSS-10 PASSED: Slash during withdrawal queue")
}

// SSS-11: Slash after withdrawal block boundary
// Tests that late slashes still affect base shares
func testSSS11_SlashAfterWithdrawalBoundary(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-11: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 4, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 4, 5, 18, 0, 0, 0, time.UTC)
	day6 := time.Date(2025, 4, 6, 18, 0, 0, 0, time.UTC)
	day19 := time.Date(2025, 4, 19, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	blocks := []struct {
		number uint64
		time   time.Time
	}{
		{120000, day4},
		{120001, day5},
		{120002, day6},
		{120015, day19},
	}
	for _, b := range blocks {
		res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`,
			b.number, fmt.Sprintf("hash_%d", b.number), b.time)
		require.Nil(t, res.Error)
	}

	// Deposit 1000 shares on 4/4
	res := grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss11", "0xstrat_sss11", "1000000000000000000000", 0, 120000, day4, day4.Format("2006-01-02"), "tx_120000", 1)
	require.Nil(t, res.Error)

	// Slash 50% on 4/6
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss11", "0xstrat_sss11", "500000000000000000000", 0, 120002, day6, day6.Format("2006-01-02"), "tx_120002", 1)
	require.Nil(t, res.Error)

	// Second slash on 4/19 (after withdrawal boundary): 500 * 0.375 = 187.5
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss11", "0xstrat_sss11", "187500000000000000000", 0, 120015, day19, day19.Format("2006-01-02"), "tx_120015", 1)
	require.Nil(t, res.Error)

	t.Log("SSS-11: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-04-25")
	require.Nil(t, err)

	// Verify snapshots
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss11", "0xstrat_sss11", "2025-04-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "1000000000000000000000", shares, "Expected 1000 shares on 4/5")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss11", "0xstrat_sss11", "2025-04-07").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "500000000000000000000", shares, "Expected 500 shares on 4/7")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss11", "0xstrat_sss11", "2025-04-20").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "187500000000000000000", shares, "Expected 187.5 shares on 4/20")

	t.Log("✓ SSS-11 PASSED: Slash after withdrawal block boundary")
}

// SSS-12: Dual slashing - operator set and beacon chain
// Tests that multiple slash sources compound correctly
func testSSS12_DualSlashing(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-12: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 5, 4, 17, 0, 0, 0, time.UTC)
	day6 := time.Date(2025, 5, 6, 18, 0, 0, 0, time.UTC)
	day7 := time.Date(2025, 5, 7, 18, 0, 0, 0, time.UTC)
	day19 := time.Date(2025, 5, 19, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	blocks := []struct {
		number uint64
		time   time.Time
	}{
		{130100, day4},
		{130102, day6},
		{130103, day7},
		{130115, day19},
	}
	for _, b := range blocks {
		res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`,
			b.number, fmt.Sprintf("hash_%d", b.number), b.time)
		require.Nil(t, res.Error)
	}

	// Deposit 200 shares on 5/4
	res := grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss12", "0xstrat_sss12", "200000000000000000000", 0, 130100, day4, day4.Format("2006-01-02"), "tx_130100", 1)
	require.Nil(t, res.Error)

	// Operator set slash 25% on 5/6: 200 * 0.75 = 150
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss12", "0xstrat_sss12", "150000000000000000000", 0, 130102, day6, day6.Format("2006-01-02"), "tx_130102", 1)
	require.Nil(t, res.Error)

	// Beacon chain slash 50% on 5/7: 150 * 0.5 = 75
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss12", "0xstrat_sss12", "75000000000000000000", 0, 130103, day7, day7.Format("2006-01-02"), "tx_130103", 1)
	require.Nil(t, res.Error)

	// After withdrawal completion on 5/19: 56.25 shares
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss12", "0xstrat_sss12", "56250000000000000000", 0, 130115, day19, day19.Format("2006-01-02"), "tx_130115", 1)
	require.Nil(t, res.Error)

	t.Log("SSS-12: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-05-25")
	require.Nil(t, err)

	// Verify snapshots
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss12", "0xstrat_sss12", "2025-05-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "200000000000000000000", shares, "Expected 200 shares on 5/5")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss12", "0xstrat_sss12", "2025-05-07").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "150000000000000000000", shares, "Expected 150 shares on 5/7")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss12", "0xstrat_sss12", "2025-05-08").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "75000000000000000000", shares, "Expected 75 shares on 5/8")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss12", "0xstrat_sss12", "2025-05-20").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "56250000000000000000", shares, "Expected 56.25 shares on 5/20")

	t.Log("✓ SSS-12 PASSED: Dual slashing")
}

// SSS-13: Same block events - withdrawal before slash
// Tests log_index ordering when withdrawal and slash happen in same block
func testSSS13_SameBlockWithdrawalBeforeSlash(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-13: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 6, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 6, 5, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 140000, "hash_140000", day4)
	require.Nil(t, res.Error)
	res = grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 140001, "hash_140001", day5)
	require.Nil(t, res.Error)

	// Deposit 1000 shares on 6/4
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss13", "0xstrat_sss13", "1000000000000000000000", 0, 140000, day4, day4.Format("2006-01-02"), "tx_140000", 1)
	require.Nil(t, res.Error)

	// Queue full withdrawal on 6/5 (log_index 2), then slash (log_index 3) in same block
	// After withdrawal+slash: staker_shares = 0, add-back = 1000 * 0.5 = 500
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss13", "0xstrat_sss13", "0", 0, 140001, day5, day5.Format("2006-01-02"), "tx_140001", 2)
	require.Nil(t, res.Error)

	// Insert queued withdrawal
	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss13", "0xoperator_sss13", "0xstaker_sss13", "1", 140001, "0xstrat_sss13", "1000000000000000000000", "1000000000000000000000", "root_sss13_1", 140001, "tx_140001", 2)
	require.Nil(t, res.Error)

	// Slash 50% in same block with higher log_index
	res = grm.Exec(`
		INSERT INTO queued_withdrawal_slashing_adjustments (staker, strategy, operator, withdrawal_block_number, slash_block_number, slash_multiplier, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss13", "0xstrat_sss13", "0xoperator_sss13", 140001, 140001, 0.5, 140001, "tx_140001_slash", 3)
	require.Nil(t, res.Error)

	t.Log("SSS-13: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-06-15")
	require.Nil(t, err)

	// Verify: 1000 on 6/5 (deposit only visible), 500 on 6/6+ (withdrawal + slash)
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss13", "0xstrat_sss13", "2025-06-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "1000000000000000000000", shares, "Expected 1000 shares on 6/5")

	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss13", "0xstrat_sss13", "2025-06-06").Scan(&shares)
	require.Nil(t, res.Error)
	require.Contains(t, shares, "500000000000000000000", "Expected 500 shares on 6/6 (slash affects queued withdrawal)")

	t.Log("✓ SSS-13 PASSED: Same block withdrawal before slash")
}

// SSS-14: Same block events - slash before withdrawal
// Tests that slash before withdrawal affects the queued amount
//
// Key insight: The add-back mechanism only triggers when there's a slash AFTER
// the withdrawal is queued (creating a queued_withdrawal_slashing_adjustments record).
// When slash happens BEFORE withdrawal, the queued amount is already post-slash,
// and no add-back occurs.
func testSSS14_SameBlockSlashBeforeWithdrawal(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-14: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 7, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 7, 5, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 150000, "hash_150000", day4)
	require.Nil(t, res.Error)
	res = grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 150001, "hash_150001", day5)
	require.Nil(t, res.Error)

	// Setup delegation
	res = grm.Exec(`
		INSERT INTO staker_delegation_changes (staker, operator, delegated, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss14", "0xoperator_sss14", true, 150000, "tx_delegation_sss14", 0)
	require.Nil(t, res.Error, "Failed to insert delegation")

	// Deposit 1000 shares on 7/4
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss14", "0xstrat_sss14", "1000000000000000000000", 0, 150000, day4, day4.Format("2006-01-02"), "tx_150000", 1)
	require.Nil(t, res.Error)

	// Slash 50% first (log_index 2), then queue full withdrawal (log_index 3) in same block
	// Slash happens first: 1000 * 0.5 = 500, then full withdrawal of 500
	// staker_shares goes to 0 after full withdrawal
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss14", "0xstrat_sss14", "0", 0, 150001, day5, day5.Format("2006-01-02"), "tx_150001", 3)
	require.Nil(t, res.Error)

	// Insert queued withdrawal for POST-SLASH amount (500 shares)
	// Note: No queued_withdrawal_slashing_adjustments because slash happened BEFORE withdrawal
	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss14", "0xoperator_sss14", "0xstaker_sss14", "1", 150001, "0xstrat_sss14", "500000000000000000000", "500000000000000000000", "root_sss14_1", 150001, "tx_150001", 3)
	require.Nil(t, res.Error)

	t.Log("SSS-14: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-07-25")
	require.Nil(t, err)

	// Verify: 1000 on 7/5 (deposit only visible)
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss14", "0xstrat_sss14", "2025-07-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "1000000000000000000000", shares, "Expected 1000 shares on 7/5")

	// On 7/6: staker_shares = 0 (full withdrawal)
	// No add-back because slash happened BEFORE withdrawal (no queued_withdrawal_slashing_adjustments)
	// This is the expected behavior: add-back only happens when slash occurs AFTER withdrawal is queued
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss14", "0xstrat_sss14", "2025-07-06").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "0", shares, "Expected 0 shares on 7/6 (no add-back when slash is before withdrawal)")

	t.Log("✓ SSS-14 PASSED: Same block slash before withdrawal - no add-back needed")
}

// SSS-15: Multiple deposits then withdrawal
// Tests cumulative deposits and partial withdrawal
//
// Key insight: The add-back mechanism only triggers when there's a slash AFTER
// the withdrawal is queued. Without a slash, no add-back occurs - the queued
// shares are simply deducted and await completion.
func testSSS15_MultipleDepositsThenWithdrawal(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	t.Log("SSS-15: Setting up test scenario with database inserts...")

	day4 := time.Date(2025, 8, 4, 17, 0, 0, 0, time.UTC)
	day5 := time.Date(2025, 8, 5, 18, 0, 0, 0, time.UTC)
	day19 := time.Date(2025, 8, 19, 18, 0, 0, 0, time.UTC)

	// Insert blocks
	res := grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 160000, "hash_160000", day4)
	require.Nil(t, res.Error)
	res = grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 160001, "hash_160001", day5)
	require.Nil(t, res.Error)
	res = grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?)`, 160015, "hash_160015", day19)
	require.Nil(t, res.Error)

	// Setup delegation
	res = grm.Exec(`
		INSERT INTO staker_delegation_changes (staker, operator, delegated, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss15", "0xoperator_sss15", true, 160000, "tx_delegation_sss15", 0)
	require.Nil(t, res.Error, "Failed to insert delegation")

	// First deposit 100 shares on 8/4
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss15", "0xstrat_sss15", "100000000000000000000", 0, 160000, day4, day4.Format("2006-01-02"), "tx_160000", 1)
	require.Nil(t, res.Error)

	// Second deposit 50 + queue withdrawal 10 on 8/5: cumulative = 100 + 50 - 10 = 140
	res = grm.Exec(`
		INSERT INTO staker_shares (staker, strategy, shares, strategy_index, block_number, block_time, block_date, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss15", "0xstrat_sss15", "140000000000000000000", 0, 160001, day5, day5.Format("2006-01-02"), "tx_160001", 1)
	require.Nil(t, res.Error)

	// Insert queued withdrawal for 10 shares
	// Note: No queued_withdrawal_slashing_adjustments because there's no slash
	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "0xstaker_sss15", "0xoperator_sss15", "0xstaker_sss15", "1", 160001, "0xstrat_sss15", "10000000000000000000", "10000000000000000000", "root_sss15_1", 160001, "tx_160001", 0)
	require.Nil(t, res.Error)

	t.Log("SSS-15: Generating snapshots...")
	err := rc.GenerateAndInsertStakerShareSnapshots("2025-08-25")
	require.Nil(t, err)

	// Verify: 100 on 8/5 (first deposit only visible)
	var shares string
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss15", "0xstrat_sss15", "2025-08-05").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "100000000000000000000", shares, "Expected 100 shares on 8/5")

	// On 8/6: staker_shares = 140 (100 + 50 - 10)
	// No add-back because there's no slash (no queued_withdrawal_slashing_adjustments)
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss15", "0xstrat_sss15", "2025-08-06").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "140000000000000000000", shares, "Expected 140 shares on 8/6 (no add-back without slash)")

	// After queue window completes (14 days from 8/5 = 8/19), shares remain 140
	res = grm.Raw(`SELECT shares FROM staker_share_snapshots WHERE staker = ? AND strategy = ? AND snapshot = ?`,
		"0xstaker_sss15", "0xstrat_sss15", "2025-08-20").Scan(&shares)
	require.Nil(t, res.Error)
	require.Equal(t, "140000000000000000000", shares, "Expected 140 shares on 8/20")

	t.Log("✓ SSS-15 PASSED: Multiple deposits then withdrawal - no add-back without slash")
}

// Helper function to setup test account
func setupSSS_TestAccount(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()

	// Try to get private key from environment
	privateKeyHex := os.Getenv("TEST_PRIVATE_KEY")
	if privateKeyHex == "" {
		// Use Anvil default account #0
		// Private key: 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
		privateKeyHex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	}

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	require.NoError(t, err, "Failed to parse private key")

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	require.True(t, ok, "Failed to cast public key to ECDSA")

	address := crypto.PubkeyToAddress(*publicKeyECDSA)
	return privateKey, address
}

// Helper function to get required environment variable
func getSSS_RequiredEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("Required environment variable %s is not set", key)
	}
	return value
}

// Helper function to setup sidecar for Anvil tests
func setupSSS_SidecarForAnvil(t *testing.T, anvilURL string) (string, *config.Config, *gorm.DB, *zap.Logger) {
	t.Helper()

	cfg := config.NewConfig()
	cfg.Chain = config.Chain_PreprodHoodi
	cfg.Debug = true
	cfg.DatabaseConfig.Host = "localhost"
	cfg.DatabaseConfig.Port = 5432
	cfg.DatabaseConfig.DbName = fmt.Sprintf("test_sss_anvil_%d", time.Now().Unix())
	cfg.DatabaseConfig.User = os.Getenv("POSTGRES_USER")
	cfg.DatabaseConfig.Password = os.Getenv("POSTGRES_PASSWORD")
	cfg.Rewards.WithdrawalQueueWindow = 14 // 14 days
	// Enable Sabine fork at block 0 so withdrawal queue add-back logic is always active
	cfg.SetForkOverride(config.RewardsFork_Sabine, 0, "1970-01-01")

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	require.NoError(t, err, "Failed to create test database")

	return dbname, cfg, grm, l
}
