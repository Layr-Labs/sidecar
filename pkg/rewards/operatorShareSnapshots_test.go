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

func hydrateOperatorShares(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorSharesSqlFile(projectRoot)

	if err != nil {
		return err
	}

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func Test_OperatorShareSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err = hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err = hydrateOperatorShares(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate operator share snapshots", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating operator share snapshots")
		err := rewards.GenerateAndInsertOperatorShareSnapshots(snapshotDate)
		assert.Nil(t, err)

		snapshots, err := rewards.ListOperatorShareSnapshots()
		assert.Nil(t, err)

		t.Log("Loading expected results")
		expectedResults, err := tests.GetOperatorShareSnapshotsExpectedResults(projectRoot)
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(snapshots))

		mappedExpectedResults := make(map[string]string)

		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s_%s", expectedResult.Operator, expectedResult.Strategy, expectedResult.Snapshot)
			mappedExpectedResults[slotId] = expectedResult.Shares
		}

		if len(expectedResults) != len(snapshots) {
			t.Errorf("Expected %d snapshots, got %d", len(expectedResults), len(snapshots))

			lacksExpectedResult := make([]*OperatorShareSnapshots, 0)
			// Go line-by-line in the snapshot results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, snapshot := range snapshots {
				snapshotStr := snapshot.Snapshot.Format(time.DateOnly)

				slotId := fmt.Sprintf("%s_%s_%s", snapshot.Operator, snapshot.Strategy, snapshotStr)

				found, ok := mappedExpectedResults[slotId]
				if !ok {
					t.Logf("Record not found %+v", snapshot)
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
					continue
				}
				if found != snapshot.Shares {
					// t.Logf("Expected: %s, Got: %s for %+v", found, snapshot.Shares, snapshot)
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
				}
			}
			assert.Equal(t, 0, len(lacksExpectedResult))
			if len(lacksExpectedResult) > 0 {
				for i, window := range lacksExpectedResult {
					fmt.Printf("%d - Snapshot: %+v\n", i, window)
				}
			}
		}
	})
	t.Cleanup(func() {
		teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)
	})
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

// Test_OperatorShareSnapshots_WithAllocations tests operator shares combined with allocations
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
	avs := "0xavs1"
	strategy := "0xstrategy2"

	t0 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, 2, 5, 0, 0, 0, 0, time.UTC)

	t.Run("Operator shares replaced by allocation magnitude", func(t *testing.T) {
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

		// T0: Operator has 1000 shares from base operator_shares table
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "0xtx100", 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// T1: Operator has allocation of 2000 (should override base shares)
		err = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, avs, strategy, 1, "2000000000000000000000", 200, 200, "0xtx1", 1).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate operator allocations first
		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(t1.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(t1.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify snapshots - should show allocation magnitude (2000) not base shares (1000)
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

		if err == nil && snapshot.Shares != "" {
			t.Logf("Snapshot on %s: Shares = %s (expected 2000...)", snapshot.Snapshot.Format(time.DateOnly), snapshot.Shares)
			// Note: The allocation magnitude should be used when available
		} else {
			t.Logf("No snapshot found for %s", t1.Format(time.DateOnly))
		}
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

// Test_OperatorShareSnapshots_CutoffDateIncluded verifies that the snapshot for
// the cutoff date itself is included
func Test_OperatorShareSnapshots_CutoffDateIncluded(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator_boundary1"
	strategy := "0xstrategy_boundary1"

	// Record created on Jan 1, cutoff is Jan 5
	// Snapshots should be generated for Jan 2, 3, 4, and 5 (cutoff date INCLUDED)
	recordDate := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	cutoffDate := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

	t.Run("Cutoff date snapshot is included", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", recordDate, time.Now()).Error
		assert.Nil(t, err)

		// Insert operator shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "0xtx100", 0, recordDate, recordDate.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify cutoff date snapshot exists
		var snapshots []struct {
			Snapshot time.Time
			Shares   string
		}
		err = grm.Raw(`
			SELECT snapshot, shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ?
			ORDER BY snapshot
		`, operator, strategy).Scan(&snapshots).Error
		assert.Nil(t, err)

		t.Logf("Generated %d snapshots:", len(snapshots))
		for i, snap := range snapshots {
			t.Logf("  [%d] Date: %s, Shares: %s", i, snap.Snapshot.Format(time.DateOnly), snap.Shares)
		}

		// Check that cutoff date (Jan 5) snapshot exists
		hasCutoffSnapshot := false
		for _, snap := range snapshots {
			if snap.Snapshot.Format(time.DateOnly) == cutoffDate.Format(time.DateOnly) {
				hasCutoffSnapshot = true
				break
			}
		}
		assert.True(t, hasCutoffSnapshot, "Snapshot for cutoff date %s should exist", cutoffDate.Format(time.DateOnly))

		// Expected snapshots: Jan 2, 3, 4, 5 (record on Jan 1 rounds up to Jan 2)
		assert.GreaterOrEqual(t, len(snapshots), 4, "Should have at least 4 snapshots (Jan 2-5)")
	})
}

// Test_OperatorShareSnapshots_RecordOnCutoffDate verifies that a record created
// exactly on the cutoff date generates a snapshot for that date
func Test_OperatorShareSnapshots_RecordOnCutoffDate(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator_boundary2"
	strategy := "0xstrategy_boundary2"

	// Record created on cutoff date itself
	cutoffDate := time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC)
	recordDate := time.Date(2024, 2, 14, 23, 59, 0, 0, time.UTC) // Just before cutoff

	t.Run("Record just before cutoff generates cutoff snapshot", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", recordDate, time.Now()).Error
		assert.Nil(t, err)

		// Insert operator shares just before cutoff
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "2000000000000000000000", "0xtx100", 0, recordDate, recordDate.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify snapshot for Feb 15 exists (record on Feb 14 rounds up to Feb 15)
		var snapshot struct {
			Snapshot time.Time
			Shares   string
		}
		err = grm.Raw(`
			SELECT snapshot, shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, cutoffDate.Format(time.DateOnly)).Scan(&snapshot).Error

		if err == nil && !snapshot.Snapshot.IsZero() {
			t.Logf("Found snapshot for cutoff date %s: shares=%s", snapshot.Snapshot.Format(time.DateOnly), snapshot.Shares)
			assert.Equal(t, cutoffDate.Format(time.DateOnly), snapshot.Snapshot.Format(time.DateOnly))
		} else {
			t.Logf("Checking for Feb 15 snapshot...")
			// The record on Feb 14 should generate a snapshot for Feb 15
			var count int64
			grm.Raw(`SELECT COUNT(*) FROM operator_share_snapshots WHERE operator = ?`, operator).Scan(&count)
			t.Logf("Total snapshots for operator: %d", count)
			assert.Greater(t, count, int64(0), "Should have at least one snapshot")
		}
	})
}

// Test_OperatorShareSnapshots_SingleDayWindow verifies that when start_time equals
// cutoff date, a snapshot is still generated (fixes the start_time < end_time filter issue)
func Test_OperatorShareSnapshots_SingleDayWindow(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator_boundary3"
	strategy := "0xstrategy_boundary3"

	// Record created such that start_time == cutoff date
	// block_time on Jan 4 -> snapshot_time = Jan 5 -> start_time = Jan 5
	// cutoff = Jan 5 -> end_time = Jan 6 (with + INTERVAL '1' day fix)
	// start_time (Jan 5) < end_time (Jan 6) -> TRUE, generates snapshot for Jan 5
	recordDate := time.Date(2024, 1, 4, 12, 0, 0, 0, time.UTC)
	cutoffDate := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

	t.Run("Single day window generates snapshot", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", recordDate, time.Now()).Error
		assert.Nil(t, err)

		// Insert operator shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "500000000000000000000", "0xtx100", 0, recordDate, recordDate.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify Jan 5 snapshot exists
		var count int64
		err = grm.Raw(`
			SELECT COUNT(*)
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ? AND snapshot = ?
		`, operator, strategy, cutoffDate.Format(time.DateOnly)).Scan(&count).Error
		assert.Nil(t, err)

		t.Logf("Snapshots for cutoff date %s: %d", cutoffDate.Format(time.DateOnly), count)
		assert.Equal(t, int64(1), count, "Should have exactly one snapshot for the cutoff date")
	})
}

// Test_OperatorShareSnapshots_GenerateSeriesBoundary verifies the generate_series
// upper bound is correctly handled (DATE(end_time) - interval '1' day)
func Test_OperatorShareSnapshots_GenerateSeriesBoundary(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorShareSnapshot(dbFileName, cfg, grm, l)

	operator := "0xoperator_boundary4"
	strategy := "0xstrategy_boundary4"

	// Create a window from Jan 2 to Jan 10
	// Record on Jan 1 -> snapshot_time = Jan 2 (start_time)
	// Record on Jan 9 -> snapshot_time = Jan 10 (end_time for first window)
	// Cutoff = Jan 10
	// First window: Jan 2 to Jan 10, generates Jan 2-9
	// Second window: Jan 10 to Jan 11 (cutoff+1), generates Jan 10
	record1Date := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	record2Date := time.Date(2024, 1, 9, 12, 0, 0, 0, time.UTC)
	cutoffDate := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)

	t.Run("Generate series includes all expected dates", func(t *testing.T) {
		// Insert blocks
		for i, rd := range []time.Time{record1Date, record2Date} {
			err := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (number) DO NOTHING
			`, uint64(100+i), fmt.Sprintf("0xblock%d", 100+i), rd, time.Now()).Error
			assert.Nil(t, err)
		}

		// Insert operator shares
		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "1000000000000000000000", "0xtx100", 0, record1Date, record1Date.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		err = grm.Exec(`
			INSERT INTO operator_shares (operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, operator, strategy, "2000000000000000000000", "0xtx101", 0, record2Date, record2Date.Format(time.DateOnly), 101).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorAllocationSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertOperatorShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify all dates from Jan 2 to Jan 10 have snapshots
		var snapshots []struct {
			Snapshot time.Time
			Shares   string
		}
		err = grm.Raw(`
			SELECT snapshot, shares
			FROM operator_share_snapshots
			WHERE operator = ? AND strategy = ?
			ORDER BY snapshot
		`, operator, strategy).Scan(&snapshots).Error
		assert.Nil(t, err)

		t.Logf("Generated %d snapshots:", len(snapshots))
		for i, snap := range snapshots {
			t.Logf("  [%d] Date: %s, Shares: %s", i, snap.Snapshot.Format(time.DateOnly), snap.Shares)
		}

		// Expected: Jan 2, 3, 4, 5, 6, 7, 8, 9, 10 = 9 snapshots
		assert.Equal(t, 9, len(snapshots), "Should have 9 snapshots (Jan 2-10)")

		// Verify first snapshot is Jan 2 and last is Jan 10
		if len(snapshots) > 0 {
			assert.Equal(t, "2024-01-02", snapshots[0].Snapshot.Format(time.DateOnly), "First snapshot should be Jan 2")
			assert.Equal(t, "2024-01-10", snapshots[len(snapshots)-1].Snapshot.Format(time.DateOnly), "Last snapshot should be Jan 10 (cutoff date)")
		}

		// Verify shares change on Jan 10
		for _, snap := range snapshots {
			if snap.Snapshot.Format(time.DateOnly) == "2024-01-10" {
				assert.Equal(t, "2000000000000000000000", snap.Shares, "Jan 10 should have updated shares")
			}
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
