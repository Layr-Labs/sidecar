package rewards

import (
	"fmt"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupOperatorAllocationSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	cfg := tests.GetConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func teardownOperatorAllocationSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func Test_OperatorAllocationSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorAllocationSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-11-16"

	t.Run("Should generate operator allocation snapshots with rounding", func(t *testing.T) {
		// Create test blocks
		blocks := []struct {
			number    uint64
			blockTime time.Time
		}{
			{100, time.Date(2024, 11, 14, 10, 0, 0, 0, time.UTC)},
			{101, time.Date(2024, 11, 14, 15, 0, 0, 0, time.UTC)},
			{102, time.Date(2024, 11, 15, 12, 0, 0, 0, time.UTC)},
		}

		for _, b := range blocks {
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
				VALUES (?, ?, ?, ?, '', NOW(), NOW())
			`, b.number, fmt.Sprintf("hash_%d", b.number), b.blockTime, b.blockTime.Format("2006-01-02"))
			assert.Nil(t, res.Error)
		}

		// Create test operator allocations
		// First allocation: magnitude 1000 at block 100 (should round up to 2024-11-15)
		res := grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "1000", 100, 100, "tx_100", 1)
		assert.Nil(t, res.Error)

		// Increase allocation: magnitude 1500 at block 101 (should round up to 2024-11-16)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "1500", 101, 101, "tx_101", 1)
		assert.Nil(t, res.Error)

		// Decrease allocation: magnitude 500 at block 102 (should round down to 2024-11-15)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "500", 102, 102, "tx_102", 1)
		assert.Nil(t, res.Error)

		// Generate snapshots
		calculator := NewRewardsCalculator(grm, nil, cfg, l, sink, stakerOperators.NewStakerOperatorStore(grm, l))
		err := calculator.GenerateAndInsertOperatorAllocationSnapshots(snapshotDate)
		assert.Nil(t, err)

		// Verify snapshots were created
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			ORDER BY snapshot
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1).Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Expected snapshots to be generated")

		// Verify rounding behavior:
		// - First allocation (1000) at block 100 (2024-11-14 10:00) should have snapshot starting 2024-11-15
		// - Increase (1500) at block 101 (2024-11-14 15:00) should have snapshot starting 2024-11-15 (next day after 2024-11-14)
		// - Decrease (500) at block 102 (2024-11-15 12:00) should have snapshot starting 2024-11-15 (same day)

		// Find the snapshot with magnitude 1000
		var mag1000Count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '1000' AND snapshot = ?
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "2024-11-15").Scan(&mag1000Count)
		assert.Nil(t, res.Error)
		assert.True(t, mag1000Count > 0, "Expected magnitude 1000 to have snapshot on 2024-11-15 (rounded up)")
	})

	t.Cleanup(func() {
		teardownOperatorAllocationSnapshot(dbFileName, cfg, grm, l)
	})
}
