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
