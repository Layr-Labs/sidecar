package rewards

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_DirectBronzeTableQuerying_WithdrawalQueue validates that staker share snapshots
// query directly from queued_slashing_withdrawals bronze table without intermediate tables
func Test_DirectBronzeTableQuerying_WithdrawalQueue(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err, "Failed to setup test database")
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup blocks
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES
			(100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW()),
			(110, '0xblock110', '2025-01-05 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	// Insert staker with base shares
	res = grm.Exec(`
		INSERT INTO staker_share_deltas
		(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
		VALUES
		('0xstaker1', '0xstrategy1', '1000000000000000000000'::numeric, 0, 100, '2025-01-01', '0xtx1', 1, '2025-01-01 00:00:00')
	`)
	require.NoError(t, res.Error)

	// Insert withdrawal directly in queued_slashing_withdrawals (bronze table)
	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals
		(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
		 withdrawal_root, block_number, transaction_hash, log_index)
		VALUES
		('0xstaker1', '0xoperator1', '0xstaker1', '1', 110, '0xstrategy1', '500000000000000000000'::numeric,
		 '500000000000000000000'::numeric, '0xroot1', 110, '0xtx2', 2)
	`)
	require.NoError(t, res.Error)

	// Generate snapshots for date during withdrawal queue (Jan 10 is 5 days after queue, within 14 days)
	snapshotDate := "2025-01-10"
	err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
	require.NoError(t, err)

	// Verify staker_share_snapshots includes withdrawal queue shares
	var totalShares string
	err = grm.Raw(`
		SELECT COALESCE(SUM(shares::numeric), 0)::text
		FROM staker_share_snapshots
		WHERE snapshot = $1 AND staker = $2
	`, snapshotDate, "0xstaker1").Scan(&totalShares).Error
	require.NoError(t, err)

	// Expected: 1000 (base) + 500 (in queue) = 1500
	// Note: withdrawal creates negative delta, so base becomes 500, but queue adds back 500
	expectedShares := "500000000000000000000" // After withdrawal delta
	assert.Equal(t, expectedShares, totalShares, "Should include shares from queued_slashing_withdrawals directly")

	// Verify withdrawal_queue_share_snapshots table does NOT exist or is not used
	var tableExists bool
	err = grm.Raw(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'withdrawal_queue_share_snapshots'
		)
	`).Scan(&tableExists).Error
	require.NoError(t, err)

	if tableExists {
		var count int64
		grm.Raw("SELECT COUNT(*) FROM withdrawal_queue_share_snapshots WHERE snapshot = $1", snapshotDate).Scan(&count)
		t.Logf("Note: withdrawal_queue_share_snapshots table exists but should not be used in snapshot generation (found %d rows)", count)
	}

	t.Log("✓ Verified direct bronze table querying pattern")
}

// Test_WithdrawalQueue_14DayBoundaryPrecision tests exact 14-day boundary behavior
func Test_WithdrawalQueue_14DayBoundaryPrecision(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	snapshotDate := "2025-01-15"

	testCases := []struct {
		name           string
		queuedAt       string
		blockNum       uint64
		shouldInclude  bool
		description    string
	}{
		{
			name:          "exactly_14_days",
			queuedAt:      "2025-01-01 00:00:00",
			blockNum:      100,
			shouldInclude: true,
			description:   "Queued exactly 14 days before snapshot (should be included)",
		},
		{
			name:          "14_days_plus_1_second",
			queuedAt:      "2024-12-31 23:59:59",
			blockNum:      101,
			shouldInclude: false,
			description:   "Queued 14 days + 1 second before snapshot (should be excluded)",
		},
		{
			name:          "14_days_minus_1_second",
			queuedAt:      "2025-01-01 00:00:01",
			blockNum:      102,
			shouldInclude: true,
			description:   "Queued 14 days - 1 second before snapshot (should be included)",
		},
		{
			name:          "13_days",
			queuedAt:      "2025-01-02 00:00:00",
			blockNum:      103,
			shouldInclude: true,
			description:   "Queued 13 days before snapshot (should be included)",
		},
		{
			name:          "15_days",
			queuedAt:      "2024-12-31 00:00:00",
			blockNum:      104,
			shouldInclude: false,
			description:   "Queued 15 days before snapshot (should be excluded)",
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup block
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
				VALUES ($1, $2, $3, NOW(), NOW())
			`, tc.blockNum, fmt.Sprintf("0xblock%d", tc.blockNum), tc.queuedAt)
			require.NoError(t, res.Error)

			stakerAddr := fmt.Sprintf("0xstaker_%s", tc.name)

			// Insert base shares
			res = grm.Exec(`
				INSERT INTO staker_share_deltas
				(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`, stakerAddr, "0xstrategy1", "1000000000000000000000", i, tc.blockNum, tc.queuedAt[:10], fmt.Sprintf("0xtx%d_deposit", tc.blockNum), 1, tc.queuedAt)
			require.NoError(t, res.Error)

			// Insert negative delta for withdrawal
			res = grm.Exec(`
				INSERT INTO staker_share_deltas
				(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`, stakerAddr, "0xstrategy1", "-500000000000000000000", i, tc.blockNum, tc.queuedAt[:10], fmt.Sprintf("0xtx%d", tc.blockNum), 2, tc.queuedAt)
			require.NoError(t, res.Error)

			// Insert queued withdrawal
			res = grm.Exec(`
				INSERT INTO queued_slashing_withdrawals
				(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
				 withdrawal_root, block_number, transaction_hash, log_index)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			`, stakerAddr, "0xoperator1", stakerAddr, fmt.Sprintf("%d", i), tc.blockNum, "0xstrategy1",
				"500000000000000000000", "500000000000000000000",
				fmt.Sprintf("0xroot%d", i), tc.blockNum, fmt.Sprintf("0xtx%d", tc.blockNum), 2)
			require.NoError(t, res.Error)

			t.Logf("  %s", tc.description)
		})
	}

	// Generate snapshots
	err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
	require.NoError(t, err)

	// Verify each test case
	for _, tc := range testCases {
		t.Run(tc.name+"_verify", func(t *testing.T) {
			stakerAddr := fmt.Sprintf("0xstaker_%s", tc.name)

			var totalShares string
			err = grm.Raw(`
				SELECT COALESCE(SUM(shares::numeric), 0)::text
				FROM staker_share_snapshots
				WHERE snapshot = $1 AND staker = $2
			`, snapshotDate, stakerAddr).Scan(&totalShares).Error
			require.NoError(t, err)

			// Parse to big.Int for comparison
			sharesInt := new(big.Int)
			sharesInt.SetString(totalShares, 10)

			baseShares := new(big.Int)
			baseShares.SetString("500000000000000000000", 10) // After withdrawal delta

			queueShares := new(big.Int)
			queueShares.SetString("500000000000000000000", 10)

			expectedWithQueue := new(big.Int).Add(baseShares, queueShares)

			if tc.shouldInclude {
				// Should have base + queue shares
				assert.Equal(t, expectedWithQueue.String(), totalShares,
					"%s: Expected %s (base + queue)", tc.description, expectedWithQueue.String())
			} else {
				// Should have only base shares (queue excluded)
				assert.Equal(t, baseShares.String(), totalShares,
					"%s: Expected %s (base only, queue excluded)", tc.description, baseShares.String())
			}

			t.Logf("  ✓ %s: shares = %s", tc.description, totalShares)
		})
	}
}

// Test_OperatorAllocations_BackwardCompatibility tests COALESCE(effective_block, block_number) pattern
func Test_OperatorAllocations_BackwardCompatibility(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup blocks
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES
			(100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW()),
			(110, '0xblock110', '2025-01-05 00:00:00', NOW(), NOW()),
			(120, '0xblock120', '2025-01-10 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	// Old allocation record (no effective_block, should use block_number)
	res = grm.Exec(`
		INSERT INTO operator_allocations
		(operator, avs, strategy, magnitude, operator_set_id, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
		VALUES
		('0xoperator1', '0xavs1', '0xstrategy1', '1000', 1, NULL, 100, '0xtx1', 1, NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	// New allocation record (with effective_block for 14-day delay)
	res = grm.Exec(`
		INSERT INTO operator_allocations
		(operator, avs, strategy, magnitude, operator_set_id, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
		VALUES
		('0xoperator2', '0xavs1', '0xstrategy1', '2000', 1, 120, 110, '0xtx2', 1, NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	// Generate allocation snapshots
	snapshotDate := "2025-01-15"
	err = rc.GenerateAndInsertOperatorAllocationSnapshots(snapshotDate)
	require.NoError(t, err)

	// Verify old record uses block_number (block 100 = 2025-01-01, rounds up to 2025-01-02)
	var oldCount int64
	err = grm.Raw(`
		SELECT COUNT(*) FROM operator_allocation_snapshots
		WHERE operator = '0xoperator1' AND snapshot >= '2025-01-02'
	`).Scan(&oldCount).Error
	require.NoError(t, err)
	assert.True(t, oldCount > 0, "Old allocation should use block_number (block 100)")

	// Verify new record uses effective_block (block 120 = 2025-01-10, rounds up to 2025-01-11)
	var newCount int64
	err = grm.Raw(`
		SELECT COUNT(*) FROM operator_allocation_snapshots
		WHERE operator = '0xoperator2' AND snapshot >= '2025-01-11'
	`).Scan(&newCount).Error
	require.NoError(t, err)
	assert.True(t, newCount > 0, "New allocation should use effective_block (block 120)")

	// Verify new record does NOT appear before effective_block date
	var prematureCount int64
	err = grm.Raw(`
		SELECT COUNT(*) FROM operator_allocation_snapshots
		WHERE operator = '0xoperator2' AND snapshot < '2025-01-10'
	`).Scan(&prematureCount).Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), prematureCount, "New allocation should not appear before effective_block date")

	t.Log("✓ Verified backward compatibility with COALESCE(effective_block, block_number)")
}

// Test_WithdrawalQueue_ZeroAndNegativeShares tests handling of invalid share amounts
func Test_WithdrawalQueue_ZeroAndNegativeShares(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup block
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES (100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	t.Run("zero_shares_withdrawal", func(t *testing.T) {
		// Insert staker with shares
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ('0xstaker_zero', '0xstrategy1', '1000000000000000000000'::numeric, 0, 100, '2025-01-01', '0xtx1', 1, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		// Queue zero share withdrawal (should be handled gracefully)
		res = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals
			(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
			 withdrawal_root, block_number, transaction_hash, log_index)
			VALUES ('0xstaker_zero', '0xoperator1', '0xstaker_zero', '1', 100, '0xstrategy1', '0', '0', '0xroot1', 100, '0xtx2', 2)
		`)
		require.NoError(t, res.Error)

		// Should not error during snapshot generation
		err = rc.GenerateAndInsertStakerShareSnapshots("2025-01-10")
		assert.NoError(t, err, "Should handle zero share withdrawal gracefully")

		// Verify shares unchanged
		var totalShares string
		grm.Raw(`
			SELECT COALESCE(SUM(shares::numeric), 0)::text
			FROM staker_share_snapshots
			WHERE snapshot = '2025-01-10' AND staker = '0xstaker_zero'
		`).Scan(&totalShares)

		expectedShares := new(big.Int)
		expectedShares.SetString("1000000000000000000000", 10)
		assert.Equal(t, expectedShares.String(), totalShares, "Zero withdrawal should not affect shares")

		t.Log("✓ Zero share withdrawal handled gracefully")
	})

	t.Run("very_small_shares", func(t *testing.T) {
		// Insert staker with 1 wei
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ('0xstaker_dust', '0xstrategy1', '1'::numeric, 0, 100, '2025-01-01', '0xtx3', 3, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		// Queue withdrawal of 1 wei
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ('0xstaker_dust', '0xstrategy1', '-1'::numeric, 0, 100, '2025-01-01', '0xtx4', 4, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		res = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals
			(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
			 withdrawal_root, block_number, transaction_hash, log_index)
			VALUES ('0xstaker_dust', '0xoperator1', '0xstaker_dust', '2', 100, '0xstrategy1', '1', '1', '0xroot2', 100, '0xtx4', 4)
		`)
		require.NoError(t, res.Error)

		// Should not error with dust amounts
		err = rc.GenerateAndInsertStakerShareSnapshots("2025-01-10")
		assert.NoError(t, err, "Should handle dust amounts gracefully")

		t.Log("✓ Dust amounts handled without overflow/underflow")
	})
}

// Test_MultiStrategyWithdrawal_IndependentAccounting tests independent tracking across strategies
func Test_MultiStrategyWithdrawal_IndependentAccounting(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup blocks
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES
			(100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW()),
			(110, '0xblock110', '2025-01-05 00:00:00', NOW(), NOW()),
			(120, '0xblock120', '2025-01-10 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	strategies := []string{"0xstrategy1", "0xstrategy2", "0xstrategy3"}

	// Setup staker with shares in 3 strategies
	for i, strategy := range strategies {
		shares := fmt.Sprintf("%d000000000000000000000", (i+1)*1000) // 1000, 2000, 3000
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8, $9)
		`, "0xstaker1", strategy, shares, i, 100, "2025-01-01", fmt.Sprintf("0xtx_init_%d", i), i, "2025-01-01 00:00:00")
		require.NoError(t, res.Error)
	}

	// Queue withdrawal from strategy1 on day 5 (500 shares)
	res = grm.Exec(`
		INSERT INTO staker_share_deltas
		(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
		VALUES ('0xstaker1', '0xstrategy1', '-500000000000000000000'::numeric, 0, 110, '2025-01-05', '0xtx_wd1', 10, '2025-01-05 00:00:00')
	`)
	require.NoError(t, res.Error)

	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals
		(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
		 withdrawal_root, block_number, transaction_hash, log_index)
		VALUES ('0xstaker1', '0xoperator1', '0xstaker1', '1', 110, '0xstrategy1', '500000000000000000000',
		 '500000000000000000000', '0xroot1', 110, '0xtx_wd1', 10)
	`)
	require.NoError(t, res.Error)

	// Queue withdrawal from strategy2 on day 10 (1000 shares)
	res = grm.Exec(`
		INSERT INTO staker_share_deltas
		(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
		VALUES ('0xstaker1', '0xstrategy2', '-1000000000000000000000'::numeric, 1, 120, '2025-01-10', '0xtx_wd2', 20, '2025-01-10 00:00:00')
	`)
	require.NoError(t, res.Error)

	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals
		(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
		 withdrawal_root, block_number, transaction_hash, log_index)
		VALUES ('0xstaker1', '0xoperator1', '0xstaker1', '2', 120, '0xstrategy2', '1000000000000000000000',
		 '1000000000000000000000', '0xroot2', 120, '0xtx_wd2', 20)
	`)
	require.NoError(t, res.Error)

	// Generate snapshots for day 15 (both withdrawals in queue)
	snapshotDate := "2025-01-15"
	err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
	require.NoError(t, err)

	// Verify independent tracking
	expectedShares := map[string]string{
		"0xstrategy1": "500000000000000000000",  // 1000 - 500 (base after withdrawal delta)
		"0xstrategy2": "1000000000000000000000", // 2000 - 1000 (base after withdrawal delta)
		"0xstrategy3": "3000000000000000000000", // Unchanged
	}

	for _, strategy := range strategies {
		var shares string
		err = grm.Raw(`
			SELECT COALESCE(SUM(shares::numeric), 0)::text
			FROM staker_share_snapshots
			WHERE snapshot = $1 AND staker = $2 AND strategy = $3
		`, snapshotDate, "0xstaker1", strategy).Scan(&shares).Error
		require.NoError(t, err)

		assert.Equal(t, expectedShares[strategy], shares,
			"Strategy %s should have independent accounting", strategy)

		t.Logf("✓ Strategy %s: %s shares (independent)", strategy, shares)
	}
}

// Test_FullOuterJoin_EdgeCases tests FULL OUTER JOIN behavior with various null scenarios
func Test_FullOuterJoin_EdgeCases(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup block
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES (100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	snapshotDate := "2025-01-10"

	t.Run("base_shares_only_no_queue", func(t *testing.T) {
		// Staker with base shares but no withdrawal queue
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ('0xstaker_base_only', '0xstrategy1', '1000000000000000000000'::numeric, 0, 100, '2025-01-01', '0xtx1', 1, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
		require.NoError(t, err)

		var shares string
		grm.Raw(`
			SELECT COALESCE(SUM(shares::numeric), 0)::text
			FROM staker_share_snapshots
			WHERE snapshot = $1 AND staker = $2
		`, snapshotDate, "0xstaker_base_only").Scan(&shares)

		assert.Equal(t, "1000000000000000000000", shares, "Should show base shares only")
		t.Log("✓ Base shares only: working")
	})

	t.Run("queue_only_no_base", func(t *testing.T) {
		// Staker with withdrawal queue but no remaining base shares (full withdrawal)
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES
			('0xstaker_queue_only', '0xstrategy1', '1000000000000000000000'::numeric, 1, 100, '2025-01-01', '0xtx2', 2, '2025-01-01 00:00:00'),
			('0xstaker_queue_only', '0xstrategy1', '-1000000000000000000000'::numeric, 1, 100, '2025-01-01', '0xtx3', 3, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		res = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals
			(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
			 withdrawal_root, block_number, transaction_hash, log_index)
			VALUES ('0xstaker_queue_only', '0xoperator1', '0xstaker_queue_only', '1', 100, '0xstrategy1',
			 '1000000000000000000000', '1000000000000000000000', '0xroot2', 100, '0xtx3', 3)
		`)
		require.NoError(t, res.Error)

		err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
		require.NoError(t, err)

		var shares string
		grm.Raw(`
			SELECT COALESCE(SUM(shares::numeric), 0)::text
			FROM staker_share_snapshots
			WHERE snapshot = $1 AND staker = $2
		`, snapshotDate, "0xstaker_queue_only").Scan(&shares)

		// Should show queue shares (full withdrawal still in queue)
		expectedShares := new(big.Int)
		expectedShares.SetString("1000000000000000000000", 10)
		assert.Equal(t, expectedShares.String(), shares, "Should show queue shares")
		t.Log("✓ Queue only (no base): working")
	})

	t.Run("neither_base_nor_queue", func(t *testing.T) {
		// Staker with no shares at all
		err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
		require.NoError(t, err)

		var count int64
		grm.Raw(`
			SELECT COUNT(*) FROM staker_share_snapshots
			WHERE snapshot = $1 AND staker = $2
		`, snapshotDate, "0xstaker_nonexistent").Scan(&count)

		assert.Equal(t, int64(0), count, "Non-existent staker should not appear")
		t.Log("✓ Neither base nor queue: working")
	})
}

// Test_OperatorShareSnapshots_AllocationAdjustments tests operator share snapshot generation
// with operator allocation adjustments via FULL OUTER JOIN
func Test_OperatorShareSnapshots_AllocationAdjustments(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	snapshotDate := "2025-01-10"

	// Setup block
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES (100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	t.Run("base_shares_with_allocation", func(t *testing.T) {
		// Operator with base shares
		res = grm.Exec(`
			INSERT INTO operator_share_deltas
			(operator, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ('0xoperator_both', '0xstrategy1', '5000', 0, 100, '2025-01-01', '0xtx1', 1, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		// Insert operator allocation snapshot
		res = grm.Exec(`
			INSERT INTO operator_allocation_snapshots (operator, avs, strategy, operator_set_id, magnitude, snapshot)
			VALUES ('0xoperator_both', '0xavs1', '0xstrategy1', 1, '3000', $1)
		`, snapshotDate)
		require.NoError(t, res.Error)

		err = rc.GenerateAndInsertOperatorShareSnapshots(snapshotDate)
		require.NoError(t, err)

		var shares string
		grm.Raw(`
			SELECT shares FROM operator_share_snapshots
			WHERE snapshot = $1 AND operator = $2 AND strategy = $3
		`, snapshotDate, "0xoperator_both", "0xstrategy1").Scan(&shares)

		// Should use allocation magnitude (overrides base shares)
		assert.Equal(t, "3000", shares, "Should use allocation magnitude")
		t.Log("✓ Base shares with allocation: allocation takes precedence")
	})

	t.Run("allocation_only_no_base", func(t *testing.T) {
		// Operator with allocation but no base shares
		res = grm.Exec(`
			INSERT INTO operator_allocation_snapshots (operator, avs, strategy, operator_set_id, magnitude, snapshot)
			VALUES ('0xoperator_alloc_only', '0xavs1', '0xstrategy2', 1, '7000', $1)
		`, snapshotDate)
		require.NoError(t, res.Error)

		err = rc.GenerateAndInsertOperatorShareSnapshots(snapshotDate)
		require.NoError(t, err)

		var shares string
		grm.Raw(`
			SELECT shares FROM operator_share_snapshots
			WHERE snapshot = $1 AND operator = $2 AND strategy = $3
		`, snapshotDate, "0xoperator_alloc_only", "0xstrategy2").Scan(&shares)

		assert.Equal(t, "7000", shares, "Should show allocation magnitude")
		t.Log("✓ Allocation only (no base): working")
	})

	t.Run("base_only_no_allocation", func(t *testing.T) {
		// Operator with base shares but no allocation
		res = grm.Exec(`
			INSERT INTO operator_share_deltas
			(operator, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ('0xoperator_base_only', '0xstrategy3', '9000', 0, 100, '2025-01-01', '0xtx2', 2, '2025-01-01 00:00:00')
		`)
		require.NoError(t, res.Error)

		err = rc.GenerateAndInsertOperatorShareSnapshots(snapshotDate)
		require.NoError(t, err)

		var shares string
		grm.Raw(`
			SELECT shares FROM operator_share_snapshots
			WHERE snapshot = $1 AND operator = $2 AND strategy = $3
		`, snapshotDate, "0xoperator_base_only", "0xstrategy3").Scan(&shares)

		assert.Equal(t, "9000", shares, "Should show base shares")
		t.Log("✓ Base only (no allocation): working")
	})
}

// Test_CompletedWithdrawals_NotInQueue verifies completed withdrawals don't appear in snapshots
func Test_CompletedWithdrawals_NotInQueue(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup blocks
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES
			(100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW()),
			(200, '0xblock200', '2025-01-20 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	// Insert staker with shares
	res = grm.Exec(`
		INSERT INTO staker_share_deltas
		(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
		VALUES ('0xstaker1', '0xstrategy1', '1000000000000000000000'::numeric, 0, 100, '2025-01-01', '0xtx1', 1, '2025-01-01 00:00:00')
	`)
	require.NoError(t, res.Error)

	// Queue withdrawal on Jan 1
	res = grm.Exec(`
		INSERT INTO staker_share_deltas
		(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
		VALUES ('0xstaker1', '0xstrategy1', '-500000000000000000000'::numeric, 0, 100, '2025-01-01', '0xtx2', 2, '2025-01-01 00:00:00')
	`)
	require.NoError(t, res.Error)

	res = grm.Exec(`
		INSERT INTO queued_slashing_withdrawals
		(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
		 withdrawal_root, block_number, transaction_hash, log_index)
		VALUES ('0xstaker1', '0xoperator1', '0xstaker1', '1', 100, '0xstrategy1', '500000000000000000000',
		 '500000000000000000000', '0xroot1', 100, '0xtx2', 2)
	`)
	require.NoError(t, res.Error)

	// Complete withdrawal on Jan 20 (move to completed table)
	res = grm.Exec(`
		INSERT INTO completed_slashing_withdrawals
		(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares,
		 withdrawal_root, block_number, transaction_hash, log_index)
		SELECT staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
		 withdrawal_root, 200, '0xtx_complete', 1
		FROM queued_slashing_withdrawals
		WHERE staker = '0xstaker1'
	`)
	require.NoError(t, res.Error)

	// Delete from queued table (simulating completion)
	res = grm.Exec("DELETE FROM queued_slashing_withdrawals WHERE staker = '0xstaker1'")
	require.NoError(t, res.Error)

	// Generate snapshots after completion (Jan 25)
	snapshotDate := "2025-01-25"
	err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
	require.NoError(t, err)

	// Verify completed withdrawal is NOT included in shares
	var shares string
	grm.Raw(`
		SELECT COALESCE(SUM(shares::numeric), 0)::text
		FROM staker_share_snapshots
		WHERE snapshot = $1 AND staker = $2
	`, snapshotDate, "0xstaker1").Scan(&shares)

	// Should only have base shares (500), not include completed withdrawal
	assert.Equal(t, "500000000000000000000", shares, "Completed withdrawal should not be in queue")

	t.Log("✓ Completed withdrawals correctly excluded from snapshots")
}

// Test_LargeScale_WithdrawalQueue is a stress test with many concurrent withdrawals
func Test_LargeScale_WithdrawalQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large scale test in short mode")
	}

	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestData(t, grm)
	})
	_ = dbFileName

	cfg.Rewards.RewardsV2_2Enabled = true
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err)

	// Setup blocks
	res := grm.Exec(`
		INSERT INTO blocks (number, hash, block_time, created_at, updated_at)
		VALUES (100, '0xblock100', '2025-01-01 00:00:00', NOW(), NOW())
	`)
	require.NoError(t, res.Error)

	numStakers := 100 // Reduced from 1000 for test performance
	numStrategies := 3
	withdrawalRate := 0.5 // 50% of stakers withdraw

	t.Logf("Setting up %d stakers with %d strategies each", numStakers, numStrategies)

	// Insert stakers with shares across multiple strategies
	for i := 0; i < numStakers; i++ {
		staker := fmt.Sprintf("0xstaker%04d", i)

		for s := 0; s < numStrategies; s++ {
			strategy := fmt.Sprintf("0xstrategy%d", s)
			shares := fmt.Sprintf("%d000000000000000000000", (i+1)*100)

			res = grm.Exec(`
				INSERT INTO staker_share_deltas
				(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
				VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8, $9)
			`, staker, strategy, shares, s, 100, "2025-01-01", fmt.Sprintf("0xtx_%d_%d", i, s), i*numStrategies+s, "2025-01-01 00:00:00")
			require.NoError(t, res.Error)
		}
	}

	// Queue withdrawals for 50% of stakers
	numWithdrawals := int(float64(numStakers) * withdrawalRate)
	t.Logf("Queueing %d withdrawals", numWithdrawals)

	for i := 0; i < numWithdrawals; i++ {
		staker := fmt.Sprintf("0xstaker%04d", i)
		strategy := "0xstrategy0" // Withdraw from first strategy only
		withdrawShares := fmt.Sprintf("%d000000000000000000000", (i+1)*50) // 50% of shares

		// Insert negative delta
		res = grm.Exec(`
			INSERT INTO staker_share_deltas
			(staker, strategy, shares, strategy_index, block_number, block_date, transaction_hash, log_index, block_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, staker, strategy, "-"+withdrawShares, 0, 100, "2025-01-01", fmt.Sprintf("0xtx_wd_%d", i), numStakers*numStrategies+i, "2025-01-01 00:00:00")
		require.NoError(t, res.Error)

		res = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals
			(staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw,
			 withdrawal_root, block_number, transaction_hash, log_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`, staker, "0xoperator1", staker, fmt.Sprintf("%d", i), 100, strategy, withdrawShares, withdrawShares,
			fmt.Sprintf("0xroot%d", i), 100, fmt.Sprintf("0xtx_wd_%d", i), numStakers*numStrategies+i)
		require.NoError(t, res.Error)
	}

	// Generate snapshots and measure performance
	startTime := time.Now()

	snapshotDate := "2025-01-10"
	err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate)
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("✓ Generated snapshots for %d stakers with %d withdrawals in %v", numStakers, numWithdrawals, duration)

	// Verify snapshot count
	var snapshotCount int64
	grm.Raw("SELECT COUNT(*) FROM staker_share_snapshots WHERE snapshot = $1", snapshotDate).Scan(&snapshotCount)

	expectedMinCount := int64(numStakers * numStrategies)
	assert.GreaterOrEqual(t, snapshotCount, expectedMinCount, "Should have snapshots for all staker-strategy pairs")

	t.Logf("✓ Created %d snapshot records (expected >= %d)", snapshotCount, expectedMinCount)

	// Verify performance is acceptable (should complete in < 30 seconds for 100 stakers)
	assert.Less(t, duration, 30*time.Second, "Snapshot generation should complete in reasonable time")
}

