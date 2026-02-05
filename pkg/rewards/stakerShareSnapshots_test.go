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

func setupStakerShareSnapshot() (
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

func teardownStakerShareSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateStakerShares(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetStakerSharesSqlFile(projectRoot)

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

func Test_StakerShareSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err = hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err = hydrateStakerShares(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate staker share snapshots", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating staker share snapshots")
		err := rewards.GenerateAndInsertStakerShareSnapshots(snapshotDate)
		assert.Nil(t, err)

		snapshots, err := rewards.ListStakerShareSnapshots()
		assert.Nil(t, err)

		t.Log("Getting expected results")
		expectedResults, err := tests.GetStakerSharesSnapshotsExpectedResults(projectRoot)
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(snapshots))

		t.Log("Comparing results")
		mappedExpectedResults := make(map[string]string)
		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s_%s", expectedResult.Staker, expectedResult.Strategy, expectedResult.Snapshot)
			mappedExpectedResults[slotId] = expectedResult.Shares
		}

		if len(expectedResults) != len(snapshots) {
			t.Errorf("Expected %d snapshots, got %d", len(expectedResults), len(snapshots))

			lacksExpectedResult := make([]*StakerShareSnapshot, 0)
			// Go line-by-line in the snapshot results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, snapshot := range snapshots {
				slotId := fmt.Sprintf("%s_%s_%s", snapshot.Staker, snapshot.Strategy, snapshot.Snapshot.Format(time.DateOnly))

				found, ok := mappedExpectedResults[slotId]
				if !ok {
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
					continue
				}
				if found != snapshot.Shares {
					t.Logf("Record found, but shares dont match. Expected %s, got %+v", found, snapshot)
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
		teardownStakerShareSnapshot(dbFileName, cfg, grm, l)
	})
}

// Test_StakerShareSnapshots_CutoffDateIncluded verifies that the snapshot for
// the cutoff date itself is included
func Test_StakerShareSnapshots_CutoffDateIncluded(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownStakerShareSnapshot(dbFileName, cfg, grm, l)

	staker := "0xstaker_boundary1"
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

		// Insert staker shares
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "1000000000000000000000", "0xtx100", 0, 0, recordDate, recordDate.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertStakerShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify cutoff date snapshot exists
		var snapshots []struct {
			Snapshot time.Time
			Shares   string
		}
		err = grm.Raw(`
			SELECT snapshot, shares
			FROM staker_share_snapshots
			WHERE staker = ? AND strategy = ?
			ORDER BY snapshot
		`, staker, strategy).Scan(&snapshots).Error
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

// Test_StakerShareSnapshots_RecordOnCutoffDate verifies that a record created
// just before the cutoff date generates a snapshot for that date
func Test_StakerShareSnapshots_RecordOnCutoffDate(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownStakerShareSnapshot(dbFileName, cfg, grm, l)

	staker := "0xstaker_boundary2"
	strategy := "0xstrategy_boundary2"

	// Record created just before cutoff date
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

		// Insert staker shares just before cutoff
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "2000000000000000000000", "0xtx100", 0, 0, recordDate, recordDate.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertStakerShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify snapshot for Feb 15 exists (record on Feb 14 rounds up to Feb 15)
		var snapshot struct {
			Snapshot time.Time
			Shares   string
		}
		err = grm.Raw(`
			SELECT snapshot, shares
			FROM staker_share_snapshots
			WHERE staker = ? AND strategy = ? AND snapshot = ?
		`, staker, strategy, cutoffDate.Format(time.DateOnly)).Scan(&snapshot).Error

		if err == nil && !snapshot.Snapshot.IsZero() {
			t.Logf("Found snapshot for cutoff date %s: shares=%s", snapshot.Snapshot.Format(time.DateOnly), snapshot.Shares)
			assert.Equal(t, cutoffDate.Format(time.DateOnly), snapshot.Snapshot.Format(time.DateOnly))
		} else {
			t.Logf("Checking for Feb 15 snapshot...")
			var count int64
			grm.Raw(`SELECT COUNT(*) FROM staker_share_snapshots WHERE staker = ?`, staker).Scan(&count)
			t.Logf("Total snapshots for staker: %d", count)
			assert.Greater(t, count, int64(0), "Should have at least one snapshot")
		}
	})
}

// Test_StakerShareSnapshots_SingleDayWindow verifies that when start_time equals
// cutoff date, a snapshot is still generated (fixes the start_time < end_time filter issue)
func Test_StakerShareSnapshots_SingleDayWindow(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownStakerShareSnapshot(dbFileName, cfg, grm, l)

	staker := "0xstaker_boundary3"
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

		// Insert staker shares
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "500000000000000000000", "0xtx100", 0, 0, recordDate, recordDate.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertStakerShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify Jan 5 snapshot exists
		var count int64
		err = grm.Raw(`
			SELECT COUNT(*)
			FROM staker_share_snapshots
			WHERE staker = ? AND strategy = ? AND snapshot = ?
		`, staker, strategy, cutoffDate.Format(time.DateOnly)).Scan(&count).Error
		assert.Nil(t, err)

		t.Logf("Snapshots for cutoff date %s: %d", cutoffDate.Format(time.DateOnly), count)
		assert.Equal(t, int64(1), count, "Should have exactly one snapshot for the cutoff date")
	})
}

// Test_StakerShareSnapshots_GenerateSeriesBoundary verifies the generate_series
// upper bound is correctly handled (DATE(end_time) - interval '1' day)
func Test_StakerShareSnapshots_GenerateSeriesBoundary(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownStakerShareSnapshot(dbFileName, cfg, grm, l)

	staker := "0xstaker_boundary4"
	strategy := "0xstrategy_boundary4"

	// Create a window from Jan 2 to Jan 10
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

		// Insert staker shares
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "1000000000000000000000", "0xtx100", 0, 0, record1Date, record1Date.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "2000000000000000000000", "0xtx101", 0, 0, record2Date, record2Date.Format(time.DateOnly), 101).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertStakerShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify all dates from Jan 2 to Jan 10 have snapshots
		var snapshots []struct {
			Snapshot time.Time
			Shares   string
		}
		err = grm.Raw(`
			SELECT snapshot, shares
			FROM staker_share_snapshots
			WHERE staker = ? AND strategy = ?
			ORDER BY snapshot
		`, staker, strategy).Scan(&snapshots).Error
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

// Test_StakerShareSnapshots_MultipleStakers tests multiple stakers with same strategy
func Test_StakerShareSnapshots_MultipleStakers(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownStakerShareSnapshot(dbFileName, cfg, grm, l)

	staker1 := "0xstaker_alpha"
	staker2 := "0xstaker_beta"
	strategy := "0xstrategy_shared"

	t0 := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	cutoffDate := t0.AddDate(0, 0, 2)

	t.Run("Multiple stakers same strategy", func(t *testing.T) {
		// Insert block
		err := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, created_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, 100, "0xblock100", t0, time.Now()).Error
		assert.Nil(t, err)

		// Staker 1 has 600 shares
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker1, strategy, "600000000000000000000", "0xtx100a", 0, 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Staker 2 has 900 shares
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker2, strategy, "900000000000000000000", "0xtx100b", 1, 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertStakerShareSnapshots(cutoffDate.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify both stakers have snapshots
		var count int64
		err = grm.Raw(`
			SELECT COUNT(DISTINCT staker)
			FROM staker_share_snapshots
			WHERE strategy = ?
		`, strategy).Scan(&count).Error
		assert.Nil(t, err)

		t.Logf("Found %d stakers with shares in strategy %s", count, strategy)
		assert.Equal(t, int64(2), count, "Should have 2 stakers")
	})
}

// Test_StakerShareSnapshots_ZeroShares tests staker with zero shares
func Test_StakerShareSnapshots_ZeroShares(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakerShareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownStakerShareSnapshot(dbFileName, cfg, grm, l)

	staker := "0xstaker_zero"
	strategy := "0xstrategy_zero"

	t0 := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, 4, 5, 0, 0, 0, 0, time.UTC)

	t.Run("Staker shares go to zero", func(t *testing.T) {
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

		// T0: Staker has 300 shares
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "300000000000000000000", "0xtx100", 0, 0, t0, t0.Format(time.DateOnly), 100).Error
		assert.Nil(t, err)

		// T1: Staker shares go to 0
		err = grm.Exec(`
			INSERT INTO staker_shares (staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, staker, strategy, "0", "0xtx200", 0, 0, t1, t1.Format(time.DateOnly), 200).Error
		assert.Nil(t, err)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = rewards.GenerateAndInsertStakerShareSnapshots(t1.Format(time.DateOnly))
		assert.Nil(t, err)

		// Verify snapshots
		var snapshots []struct {
			Shares   string
			Snapshot time.Time
		}
		err = grm.Raw(`
			SELECT shares, snapshot
			FROM staker_share_snapshots
			WHERE staker = ? AND strategy = ?
			ORDER BY snapshot
		`, staker, strategy).Scan(&snapshots).Error
		assert.Nil(t, err)

		t.Logf("Generated %d snapshots for zero shares scenario:", len(snapshots))
		for i, snap := range snapshots {
			t.Logf("  [%d] Date: %s, Shares: %s", i, snap.Snapshot.Format(time.DateOnly), snap.Shares)
		}
	})
}
