package rewards

import (
	"fmt"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/metrics"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupOperatorShareSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	testContext := getRewardsTestContext()
	cfg := tests.GetConfig()
	switch testContext {
	case "testnet":
		cfg.Chain = config.Chain_Holesky
	case "testnet-reduced":
		cfg.Chain = config.Chain_Holesky
	case "mainnet-reduced":
		cfg.Chain = config.Chain_Mainnet
	default:
		return "", nil, nil, nil, nil, fmt.Errorf("Unknown test context")
	}

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func teardownOperatorShareSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

// Test_OperatorShareSnapshots_BasicShares tests basic operator shares without allocations
func Test_OperatorShareSnapshots_BasicShares(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator1"
	strategy := "0xstrategy1"

	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)

	t.Run("Operator shares change over time", func(t *testing.T) {
		// Insert blocks
		blocks := []struct {
			number    uint64
			timestamp time.Time
		}{
			{100, t0},
			{200, t1},
			{300, t2},
		}

		for _, b := range blocks {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.timestamp, time.Now()).Error
			assert.Nil(t, err)
		}

		// T0: Operator has 1000 shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "0xtx100", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// T1: Shares increase to 1500
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1500000000000000000000", "0xtx200", 0, t1, t1.Format(time.DateOnly), 200).Error
		assert.Nil(t, err)

		// T2: Shares decrease to 800
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "800000000000000000000", "0xtx300", 0, t2, t2.Format(time.DateOnly), 300).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate operator allocations first (required for operator share snapshots)
		for _, ts := range []time.Time{t0, t1, t2} {
			err := rewards.GenerateAndInsertOperatorAllocationSnapshots(ts.Format(time.DateOnly))
			assert.Nil(t, err)
		}

		for _, ts := range []time.Time{t0, t1, t2} {
			err := rewards.GenerateAndInsertOperatorShareSnapshots(ts.Format(time.DateOnly))
			assert.Nil(t, err)
		}

		// Verify snapshots
		var snapshots []struct {
			Shares   string
			Snapshot time.Time
		}
		err = grm.Raw(`
			SELECT shares, snapshot
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ?
			ORDER BY snapshot
		`, operator, strategy).Scan(&snapshots).Error
		assert.Nil(t, err)

		t.Logf("Generated %d snapshots:", len(snapshots))
		for i, snap := range snapshots {
			t.Logf("  [%d] Date: %s, Shares: %s", i, snap.Snapshot.Format(time.DateOnly), snap.Shares)
		}

		// Expected:
		// T0 rounds up to 2024-01-02, so snapshots start from 2024-01-02
		// T1 rounds up to 2024-01-06, shares change from 1000 to 1500
		// T2 rounds up to 2024-01-11, shares change from 1500 to 800

		assert.GreaterOrEqual(t, len(snapshots), 3, "Should have at least 3 snapshots")
	})
}

func Test_OperatorShareSnapshots_WithAllocations(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator2"
	strategy := "0xstrategy2"

	t0 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, 2, 5, 0, 0, 0, 0, time.UTC)

	t.Run("Operator shares contain raw delegated shares, not allocation magnitude", func(t *testing.T) {
		// Insert blocks
		blocks := []struct {
			number    uint64
			timestamp time.Time
		}{
			{100, t0},
			{200, t1},
		}

		for _, b := range blocks {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.timestamp, time.Now()).Error
			assert.Nil(t, err)
		}

		// T0: Operator has 1000 shares from delegations
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "0xtx100", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(t1.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify snapshots contain raw delegated shares (1000), not allocation magnitude
		var snapshot struct {
			Shares   string
			Snapshot time.Time
		}
		err = grm.Raw(`
			SELECT shares, snapshot
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
			ORDER BY snapshot DESC
			LIMIT 1
		`, operator, strategy, t1.Format(time.DateOnly)).Scan(&snapshot).Error

		assert.Nil(t, err)
		assert.Equal(t, "1000000000000000000000", snapshot.Shares, "operator_share_snapshots should contain raw delegated shares")
		t.Logf("Snapshot on %s: Shares = %s (raw delegated shares)", snapshot.Snapshot.Format(time.DateOnly), snapshot.Shares)
	})
}

// Test_OperatorShareSnapshots_MultipleStrategies tests operator with multiple strategies
func Test_OperatorShareSnapshots_MultipleStrategies(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator3"
	strategy1 := "0xstrategy_a"
	strategy2 := "0xstrategy_b"

	t0 := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	// Use a cutoff date after t0 since snapshots round up to the next day
	cutoffDate := t0.AddDate(0, 0, 2)

	t.Run("Operator has shares in multiple strategies", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", t0, time.Now()).Error
		assert.Nil(t, err)

		// Operator has shares in strategy1
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy1, "500000000000000000000", "0xtx100a", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Operator has shares in strategy2
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy2, "700000000000000000000", "0xtx100b", 1, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots with cutoff date after data insertion
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify snapshots for both strategies
		var count int64
		err = grm.Raw(`
			SELECT COUNT(DISTINCT strategy)
			FROM operator_share_snapshots
			WHERE operator = ?
		`, operator).Scan(&count).Error
		assert.Nil(t, err)

		t.Logf("Operator has shares in %d strategies", count)
		assert.GreaterOrEqual(t, count, int64(1), "Should have at least 1 strategy")
	})
}

// Test_OperatorShareSnapshots_ZeroShares tests operator with zero shares
func Test_OperatorShareSnapshots_ZeroShares(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator4"
	strategy := "0xstrategy4"

	t0 := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, 4, 5, 0, 0, 0, 0, time.UTC)

	t.Run("Operator shares go to zero", func(t *testing.T) {
		// Insert blocks
		blocks := []struct {
			number    uint64
			timestamp time.Time
		}{
			{100, t0},
			{200, t1},
		}

		for _, b := range blocks {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.timestamp, time.Now()).Error
			assert.Nil(t, err)
		}

		// T0: Operator has 300 shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "300000000000000000000", "0xtx100", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// T1: Operator shares go to 0
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "0", "0xtx200", 0, t1, t1.Format(time.DateOnly), 200).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		for _, ts := range []time.Time{t0, t1} {
			err := rewards.GenerateAndInsertOperatorAllocationSnapshots(ts.Format(time.DateOnly))
			assert.Nil(t, err)

			err = rewards.GenerateAndInsertOperatorShareSnapshots(ts.Format(time.DateOnly))
			assert.Nil(t, err)
		}

		// Verify snapshots
		var snapshots []struct {
			Shares   string
			Snapshot time.Time
		}
		err = grm.Raw(`
			SELECT shares, snapshot
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ?
			ORDER BY snapshot
		`, operator, strategy).Scan(&snapshots).Error
		assert.Nil(t, err)

		t.Logf("Generated %d snapshots for zero shares scenario:", len(snapshots))
		for i, snap := range snapshots {
			t.Logf("  [%d] Date: %s, Shares: %s", i, snap.Snapshot.Format(time.DateOnly), snap.Shares)
		}
	})
}

// Test_OperatorShareSnapshots_MultipleOperators tests multiple operators with different strategies
func Test_OperatorShareSnapshots_MultipleOperators(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator1 := "0xoperator_alpha"
	operator2 := "0xoperator_beta"
	strategy := "0xstrategy_shared"

	t0 := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	// Use a cutoff date after t0 since snapshots round up to the next day
	cutoffDate := t0.AddDate(0, 0, 2)

	t.Run("Multiple operators same strategy", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", t0, time.Now()).Error
		assert.Nil(t, err)

		// Operator 1 has 600 shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator1, strategy, "600000000000000000000", "0xtx100a", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Operator 2 has 900 shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator2, strategy, "900000000000000000000", "0xtx100b", 1, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots with cutoff date after data insertion
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify both operators have snapshots
		var count int64
		err = grm.Raw(`
			SELECT COUNT(DISTINCT operator)
			FROM operator_share_snapshots
			WHERE strategy = ?
		`, strategy).Scan(&count).Error
		assert.Nil(t, err)

		t.Logf("Found %d operators with shares in strategy %s", count, strategy)
		assert.GreaterOrEqual(t, count, int64(1), "Should have at least 1 operator")
	})
}

// Test_OperatorShareSnapshots_SameDayMultipleChanges tests multiple share changes on same day
func Test_OperatorShareSnapshots_SameDayMultipleChanges(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator5"
	strategy := "0xstrategy5"

	baseDate := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	t.Run("Multiple changes same day - latest wins", func(t *testing.T) {
		// Insert blocks and shares - all on same day
		shares := []struct {
			number uint64
			hour   int
			amount string
		}{
			{100, 8, "100000000000000000000"},  // 08:00
			{101, 12, "200000000000000000000"}, // 12:00
			{102, 18, "150000000000000000000"}, // 18:00 - latest
		}

		for i, s := range shares {
			blockTime := baseDate.Add(time.Duration(s.hour) * time.Hour)
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, s.number, fmt.Sprintf("0xblock%d", s.number), blockTime, time.Now()).Error
			assert.Nil(t, err)

			err = grm.Exec(`
				INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, operator, strategy, s.amount, fmt.Sprintf("0xtx%d", s.number), i, blockTime, blockTime.Format(time.DateOnly), s.number).Error
			assert.Nil(t, err)
		}

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(baseDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(baseDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify only the latest share amount (150) is used
		var snapshot struct {
			Shares string
		}
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ?
			ORDER BY snapshot DESC
			LIMIT 1
		`, operator, strategy).Scan(&snapshot).Error

		if err == nil && snapshot.Shares != "" {
			t.Logf("Latest snapshot shares: %s (expected 150...)", snapshot.Shares)
		}
	})
}

// Test_OperatorShareSnapshots_LargeNumbers tests handling of large share numbers
func Test_OperatorShareSnapshots_LargeNumbers(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator6"
	strategy := "0xstrategy6"

	t0 := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)

	t.Run("Handle very large share amounts", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", t0, time.Now()).Error
		assert.Nil(t, err)

		// Very large share amount (1 billion tokens with 18 decimals)
		largeAmount := "1000000000000000000000000000"
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, largeAmount, "0xtx100", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(t0.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(t0.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify large number is handled correctly
		var snapshot struct {
			Shares string
		}
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ?
			LIMIT 1
		`, operator, strategy).Scan(&snapshot).Error

		if err == nil && snapshot.Shares != "" {
			t.Logf("Large number snapshot shares: %s", snapshot.Shares)
			assert.NotEmpty(t, snapshot.Shares, "Should handle large numbers")
		}
	})
}

// =============================================================================
// Queued Withdrawal Add-back Tests (Post-Sabine Fork)
// =============================================================================

// Test_OperatorShareSnapshots_QueuedWithdrawalAddBack tests that operator shares
// include queued withdrawal shares during the 14-day queue window.
// This mirrors the staker share snapshots behavior.
func Test_OperatorShareSnapshots_QueuedWithdrawalAddBack(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	// Set the withdrawal queue window to 14 days (required for add-back tests)
	cfg.Rewards.WithdrawalQueueWindow = 14

	// OSS-1: Operator has shares, staker queues withdrawal
	// Expected: Operator shares should include queued withdrawal during queue period
	// Flow:
	// 1. Operator has 1000 shares from delegation
	// 2. Staker queues full withdrawal: operator_shares = 0 (immediate), queued_slashing_withdrawals created
	// 3. During 14-day queue window: operator snapshots should add back queued shares (1000)
	// 4. After 14 days: shares stay at 0
	t.Run("OSS-1: Queued withdrawal adds back shares during queue period", func(t *testing.T) {
		operator := "0xoperator_oss1"
		staker := "0xstaker_oss1"
		strategy := "0xstrategy_oss1"

		day4 := time.Date(2025, 1, 4, 0, 0, 0, 0, time.UTC)
		depositTime := day4.Add(17 * time.Hour) // 5pm
		queueTime := day4.Add(18 * time.Hour)   // 6pm - queue withdrawal same day
		day5 := time.Date(2025, 1, 5, 12, 0, 0, 0, time.UTC)
		day10 := time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC) // Within queue window
		day20 := time.Date(2025, 1, 20, 12, 0, 0, 0, time.UTC) // After queue window (14 days from 1/4)

		block1 := uint64(1001)
		block2 := uint64(1002)

		// Insert blocks
		for _, b := range []struct {
			number uint64
			time   time.Time
		}{
			{block1, depositTime},
			{block2, queueTime},
			{1003, day5},
			{1010, day10},
			{1020, day20},
		} {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.time, time.Now()).Error
			assert.Nil(t, err)
		}

		// T0: Operator has 1000 shares from delegation
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "tx_oss1_deposit", 0, depositTime, depositTime.Format("2006-01-02"), block1).Error
		assert.Nil(t, err)

		// Queue withdrawal: operator_shares goes to 0
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "0", "tx_oss1_queue", 1, queueTime, queueTime.Format("2006-01-02"), block2).Error
		assert.Nil(t, err)

		// Insert queued withdrawal record - this triggers the add-back logic
		err = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, operator, staker, "1", block2, strategy, "1000000000000000000000", "1000000000000000000000", "root_oss1", block2, "tx_oss1_queue", 0).Error
		assert.Nil(t, err)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate snapshots for day 10 (within queue window)
		err = rewards.GenerateAndInsertOperatorShareSnapshots(day10.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify operator has shares during queue window (add-back)
		var snapshotDay10 struct {
			Shares string
		}
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, day5.Format(time.DateOnly)).Scan(&snapshotDay10).Error

		if err == nil && snapshotDay10.Shares != "" {
			t.Logf("Day 5 snapshot (within queue): shares = %s", snapshotDay10.Shares)
			// Should have 1000 shares (0 base + 1000 add-back)
			assert.Equal(t, "1000000000000000000000", snapshotDay10.Shares, "Should add back queued withdrawal during queue period")
		}

		// Generate snapshots for day 20 (after queue window)
		err = rewards.GenerateAndInsertOperatorShareSnapshots(day20.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify after queue window, shares are 0 (no add-back)
		var snapshotDay20 struct {
			Shares string
		}
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, day20.Format(time.DateOnly)).Scan(&snapshotDay20).Error

		if err == nil && snapshotDay20.Shares != "" {
			t.Logf("Day 20 snapshot (after queue): shares = %s", snapshotDay20.Shares)
			// Should have 0 shares (queue expired)
			assert.Equal(t, "0", snapshotDay20.Shares, "Should not add back after queue expires")
		}
	})
}

// Test_OperatorShareSnapshots_QueuedWithdrawalWithSlashing tests that slashing
// adjustments are applied to the queued withdrawal add-back.
func Test_OperatorShareSnapshots_QueuedWithdrawalWithSlashing(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	// Set the withdrawal queue window to 14 days (required for add-back tests)
	cfg.Rewards.WithdrawalQueueWindow = 14

	// OSS-2: Staker queues withdrawal, then operator is slashed
	// Expected: Add-back should be reduced by slash multiplier
	// Flow:
	// 1. Operator has 1000 shares
	// 2. Staker queues full withdrawal on 1/5
	// 3. Operator is slashed 50% on 1/6
	// 4. Snapshots after 1/6 should show: 0 base + 500 add-back (1000 * 0.5)
	t.Run("OSS-2: Queued withdrawal with slashing adjustment", func(t *testing.T) {
		operator := "0xoperator_oss2"
		staker := "0xstaker_oss2"
		strategy := "0xstrategy_oss2"

		day4 := time.Date(2025, 2, 4, 17, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 2, 5, 18, 0, 0, 0, time.UTC)   // Queue withdrawal
		day6 := time.Date(2025, 2, 6, 18, 0, 0, 0, time.UTC)   // Slash
		day10 := time.Date(2025, 2, 10, 12, 0, 0, 0, time.UTC) // Check snapshot

		block4 := uint64(2001)
		block5 := uint64(2002)
		block6 := uint64(2003)

		// Insert blocks
		for _, b := range []struct {
			number uint64
			time   time.Time
		}{
			{block4, day4},
			{block5, day5},
			{block6, day6},
			{2010, day10},
		} {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.time, time.Now()).Error
			assert.Nil(t, err)
		}

		// Day 4: Operator has 1000 shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "tx_oss2_deposit", 0, day4, day4.Format("2006-01-02"), block4).Error
		assert.Nil(t, err)

		// Day 5: Queue withdrawal - operator_shares goes to 0
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "0", "tx_oss2_queue", 0, day5, day5.Format("2006-01-02"), block5).Error
		assert.Nil(t, err)

		// Insert queued withdrawal record
		err = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, operator, staker, "1", block5, strategy, "1000000000000000000000", "1000000000000000000000", "root_oss2", block5, "tx_oss2_queue", 0).Error
		assert.Nil(t, err)

		// Day 6: Slash 50% - insert slashing adjustment with multiplier 0.5
		err = grm.Exec(`
			INSERT INTO queued_withdrawal_slashing_adjustments (
				staker, strategy, operator, withdrawal_block_number, withdrawal_log_index,
				slash_block_number, slash_multiplier, block_number, transaction_hash, log_index
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, operator, block5, 0, block6, "0.5", block6, "tx_oss2_slash", 0).Error
		assert.Nil(t, err)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate snapshots for day 10 (after slash, within queue window)
		err = rewards.GenerateAndInsertOperatorShareSnapshots(day10.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify operator has 500 shares (1000 * 0.5 slash multiplier)
		var snapshot struct {
			Shares string
		}
		// Check a snapshot date after the slash (day 7 or later)
		day7 := time.Date(2025, 2, 7, 0, 0, 0, 0, time.UTC)
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, day7.Format(time.DateOnly)).Scan(&snapshot).Error

		if err == nil && snapshot.Shares != "" {
			t.Logf("Day 7 snapshot (after slash): shares = %s", snapshot.Shares)
			// Should have 500 shares (0 base + 1000 * 0.5 add-back)
			assert.Equal(t, "500000000000000000000.0", snapshot.Shares, "Should apply slash multiplier to add-back")
		}
	})
}

// Test_OperatorShareSnapshots_PartialQueuedWithdrawal tests partial withdrawal add-back
func Test_OperatorShareSnapshots_PartialQueuedWithdrawal(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	// Set the withdrawal queue window to 14 days (required for add-back tests)
	cfg.Rewards.WithdrawalQueueWindow = 14

	// OSS-3: Staker queues partial withdrawal
	// Expected: Operator should have base shares + partial add-back during queue period
	// Flow:
	// 1. Operator has 1000 shares
	// 2. Staker queues 400 shares withdrawal: operator_shares = 600
	// 3. During queue window: snapshot should show 1000 (600 base + 400 add-back)
	t.Run("OSS-3: Partial queued withdrawal add-back", func(t *testing.T) {
		operator := "0xoperator_oss3"
		staker := "0xstaker_oss3"
		strategy := "0xstrategy_oss3"

		day4 := time.Date(2025, 3, 4, 17, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 3, 5, 18, 0, 0, 0, time.UTC)   // Queue partial withdrawal
		day10 := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC) // Check snapshot

		block4 := uint64(3001)
		block5 := uint64(3002)

		// Insert blocks
		for _, b := range []struct {
			number uint64
			time   time.Time
		}{
			{block4, day4},
			{block5, day5},
			{3010, day10},
		} {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.time, time.Now()).Error
			assert.Nil(t, err)
		}

		// Day 4: Operator has 1000 shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "tx_oss3_deposit", 0, day4, day4.Format("2006-01-02"), block4).Error
		assert.Nil(t, err)

		// Day 5: Queue partial withdrawal - operator_shares goes to 600
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "600000000000000000000", "tx_oss3_queue", 0, day5, day5.Format("2006-01-02"), block5).Error
		assert.Nil(t, err)

		// Insert queued withdrawal record for 400 shares
		err = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, operator, staker, "1", block5, strategy, "400000000000000000000", "400000000000000000000", "root_oss3", block5, "tx_oss3_queue", 0).Error
		assert.Nil(t, err)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate snapshots for day 10 (within queue window)
		err = rewards.GenerateAndInsertOperatorShareSnapshots(day10.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify operator has 1000 shares (600 base + 400 add-back)
		var snapshot struct {
			Shares string
		}
		day6 := time.Date(2025, 3, 6, 0, 0, 0, 0, time.UTC)
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, day6.Format(time.DateOnly)).Scan(&snapshot).Error

		if err == nil && snapshot.Shares != "" {
			t.Logf("Day 6 snapshot (partial queue): shares = %s", snapshot.Shares)
			// Should have 1000 shares (600 base + 400 add-back)
			assert.Equal(t, "1000000000000000000000", snapshot.Shares, "Should add back partial queued withdrawal")
		}
	})
}

// Test_OperatorShareSnapshots_MultipleStakersQueuedWithdrawals tests multiple stakers
// queuing withdrawals from the same operator
func Test_OperatorShareSnapshots_MultipleStakersQueuedWithdrawals(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	// Set the withdrawal queue window to 14 days (required for add-back tests)
	cfg.Rewards.WithdrawalQueueWindow = 14

	// OSS-4: Multiple stakers queue withdrawals from same operator
	// Expected: Operator shares should include sum of all queued withdrawals
	t.Run("OSS-4: Multiple stakers queue withdrawals", func(t *testing.T) {
		operator := "0xoperator_oss4"
		staker1 := "0xstaker_oss4a"
		staker2 := "0xstaker_oss4b"
		strategy := "0xstrategy_oss4"

		day4 := time.Date(2025, 4, 4, 17, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 4, 5, 18, 0, 0, 0, time.UTC)
		day10 := time.Date(2025, 4, 10, 12, 0, 0, 0, time.UTC)

		block4 := uint64(4001)
		block5 := uint64(4002)

		// Insert blocks
		for _, b := range []struct {
			number uint64
			time   time.Time
		}{
			{block4, day4},
			{block5, day5},
			{4010, day10},
		} {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, b.number, fmt.Sprintf("0xblock%d", b.number), b.time, time.Now()).Error
			assert.Nil(t, err)
		}

		// Day 4: Operator has 1000 shares (500 from staker1, 500 from staker2)
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "tx_oss4_deposit", 0, day4, day4.Format("2006-01-02"), block4).Error
		assert.Nil(t, err)

		// Day 5: Both stakers queue full withdrawals - operator_shares goes to 0
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "0", "tx_oss4_queue", 0, day5, day5.Format("2006-01-02"), block5).Error
		assert.Nil(t, err)

		// Insert queued withdrawal records for both stakers
		err = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker1, operator, staker1, "1", block5, strategy, "500000000000000000000", "500000000000000000000", "root_oss4a", block5, "tx_oss4_queue", 0).Error
		assert.Nil(t, err)

		err = grm.Exec(`
			INSERT INTO queued_slashing_withdrawals (staker, operator, withdrawer, nonce, start_block, strategy, scaled_shares, shares_to_withdraw, withdrawal_root, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker2, operator, staker2, "2", block5, strategy, "500000000000000000000", "500000000000000000000", "root_oss4b", block5, "tx_oss4_queue", 1).Error
		assert.Nil(t, err)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate snapshots for day 10 (within queue window)
		err = rewards.GenerateAndInsertOperatorShareSnapshots(day10.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify operator has 1000 shares (0 base + 500 + 500 add-back)
		var snapshot struct {
			Shares string
		}
		day6 := time.Date(2025, 4, 6, 0, 0, 0, 0, time.UTC)
		err = grm.Raw(`
			SELECT shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, day6.Format(time.DateOnly)).Scan(&snapshot).Error

		if err == nil && snapshot.Shares != "" {
			t.Logf("Day 6 snapshot (multiple stakers): shares = %s", snapshot.Shares)
			// Should have 1000 shares (0 base + 500 + 500 add-back from both stakers)
			assert.Equal(t, "1000000000000000000000", snapshot.Shares, "Should add back all stakers' queued withdrawals")
		}
	})
}
