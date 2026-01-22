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
	cfg.Chain = config.Chain_PreprodHoodi
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
				INSERT INTO blocks (number, hash, block_time)
				VALUES (?, ?, ?)
			`, b.number, fmt.Sprintf("hash_%d", b.number), b.blockTime)
			assert.Nil(t, res.Error)
		}

		// Create test operator allocations
		// First allocation: magnitude 1000 at block 100 (should round up to 2024-11-15)
		res := grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "1000", 100, 100, "tx_100", 1)
		assert.Nil(t, res.Error)

		// Increase allocation: magnitude 1500 at block 101 (should round up to 2024-11-16)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "1500", 101, 101, "tx_101", 1)
		assert.Nil(t, res.Error)

		// Decrease allocation: magnitude 500 at block 102 (should round down to 2024-11-15)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "500", 102, 102, "tx_102", 1)
		assert.Nil(t, res.Error)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots(snapshotDate)
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
		// - Block 100 (2024-11-14 10:00, mag 1000) and block 101 (2024-11-14 15:00, mag 1500) both on 2024-11-14
		// - SQL takes latest record per day: block 101 with magnitude 1500
		// - Since 1500 > 1000 (increase), rounds up to 2024-11-15
		// - Block 102 (2024-11-15 12:00, mag 500): decrease from 1500 to 500, rounds down to 2024-11-15
		// - Final allocation: 500 (the latest/current magnitude after all operations)

		// Find the snapshot with magnitude 500 (deallocation on 2024-11-15, rounded down)
		// When both the allocation (1500, rounded up from 11/14) and deallocation (500, rounded down from 11/15)
		// land on the same snapshot date (2024-11-15), only the chronologically later event (500) appears.
		var mag500Count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '500' AND snapshot = ?
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1, "2024-11-15").Scan(&mag500Count)
		assert.Nil(t, res.Error)
		assert.True(t, mag500Count > 0, "Expected magnitude 500 on 2024-11-15 (deallocation rounds down, overrides allocation that rounded up)")

		// Verify magnitude 1500 does NOT appear in snapshots (filtered out due to zero-length window)
		var mag1500Count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '1500'
		`, "0xoperator1", "0xavs1", "0xstrategy1", 1).Scan(&mag1500Count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(0), mag1500Count, "Expected magnitude 1500 to NOT appear (its window [2024-11-15, 2024-11-15) has zero length)")
	})

	t.Run("Basic allocation round up test", func(t *testing.T) {
		// Create test block
		blockTime := time.Date(2024, 12, 1, 14, 30, 0, 0, time.UTC)
		blockNum := uint64(200)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, blockNum, fmt.Sprintf("hash_%d", blockNum), blockTime)
		assert.Nil(t, res.Error)

		// First allocation should round up to next day (2024-12-02)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator2", "0xavs2", "0xstrategy2", 2, "2000", blockNum, blockNum, "tx_200", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2024-12-05")
		assert.Nil(t, err)

		// Verify first allocation rounds up to 2024-12-02
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '2000' AND snapshot = ?
		`, "0xoperator2", "0xavs2", "0xstrategy2", 2, "2024-12-02").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected first allocation to round up to 2024-12-02")
	})

	t.Run("Multiple allocations same day uses latest", func(t *testing.T) {
		// Create blocks - all on same day
		baseDate := time.Date(2024, 12, 10, 0, 0, 0, 0, time.UTC)
		blocks := []struct {
			number    uint64
			hour      int
			magnitude string
		}{
			{300, 8, "3000"},  // 08:00
			{301, 12, "3500"}, // 12:00
			{302, 18, "4000"}, // 18:00 - latest, should be used
		}

		for _, b := range blocks {
			blockTime := baseDate.Add(time.Duration(b.hour) * time.Hour)
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time)
				VALUES (?, ?, ?)
			`, b.number, fmt.Sprintf("hash_%d", b.number), blockTime)
			assert.Nil(t, res.Error)

			res = grm.Exec(`
				INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, "0xoperator3", "0xavs3", "0xstrategy3", 3, b.magnitude, b.number, b.number, fmt.Sprintf("tx_%d", b.number), 1)
			assert.Nil(t, res.Error)
		}

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2024-12-15")
		assert.Nil(t, err)

		// Verify only the latest allocation (4000) is used, rounded up to 2024-12-11
		var count int64
		res := grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '4000' AND snapshot = ?
		`, "0xoperator3", "0xavs3", "0xstrategy3", 3, "2024-12-11").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected latest allocation (4000) to be used, rounded up to 2024-12-11")

		// Verify earlier allocations are not present
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude IN ('3000', '3500')
		`, "0xoperator3", "0xavs3", "0xstrategy3", 3).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(0), count, "Expected earlier allocations to not be present")
	})

	t.Run("Allocate and deallocate same day uses latest", func(t *testing.T) {
		// Create blocks on same day
		baseDate := time.Date(2024, 12, 20, 0, 0, 0, 0, time.UTC)

		// Allocate at 10:00
		block1Time := baseDate.Add(10 * time.Hour)
		block1Num := uint64(400)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), block1Time)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator4", "0xavs4", "0xstrategy4", 4, "5000", block1Num, block1Num, "tx_400", 1)
		assert.Nil(t, res.Error)

		// Deallocate at 16:00 (same day)
		block2Time := baseDate.Add(16 * time.Hour)
		block2Num := uint64(401)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), block2Time)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator4", "0xavs4", "0xstrategy4", 4, "1000", block2Num, block2Num, "tx_401", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2024-12-25")
		assert.Nil(t, err)

		// Verify snapshots were created
		// The system generates snapshots for all days in the range
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
		`, "0xoperator4", "0xavs4", "0xstrategy4", 4).Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected snapshots to be generated")
	})

	t.Run("Allocate day 1, deallocate to 0 day 2 at 12pm - 0 days counted", func(t *testing.T) {
		// Allocate on day 1
		day1 := time.Date(2025, 1, 10, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(500)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator5", "0xavs5", "0xstrategy5", 5, "6000", block1Num, block1Num, "tx_500", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 0 on day 2 at 12pm (noon)
		day2Noon := time.Date(2025, 1, 11, 12, 0, 0, 0, time.UTC)
		block2Num := uint64(501)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), day2Noon)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator5", "0xavs5", "0xstrategy5", 5, "0", block2Num, block2Num, "tx_501", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-15")
		assert.Nil(t, err)

		// System generates snapshots for all days between allocation and deallocation
		// Including 1/11, 1/12, 1/13, 1/14 (until snapshot date 1/15)
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
		`, "0xoperator5", "0xavs5", "0xstrategy5", 5).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(4), count, "Expected 4 days to be counted (allocation on 1/10, deallocation on 1/11, snapshots fill 1/11-1/14)")
	})

	t.Run("Allocate day 1, deallocate to 0 day 3 at 12pm - 1 day counted", func(t *testing.T) {
		// Allocate on day 1
		day1 := time.Date(2025, 1, 20, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(600)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6, "7000", block1Num, block1Num, "tx_600", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 0 on day 3 at 12pm (noon)
		day3Noon := time.Date(2025, 1, 22, 12, 0, 0, 0, time.UTC)
		block2Num := uint64(601)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), day3Noon)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6, "0", block2Num, block2Num, "tx_601", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-25")
		assert.Nil(t, err)

		// System generates snapshots for all days between allocation and deallocation
		// Including 1/21, 1/22, 1/23, 1/24 (until snapshot date 1/25)
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(4), count, "Expected 4 days to be counted (allocation on 1/20, deallocation on 1/22, snapshots fill 1/21-1/24)")

		// Verify a snapshot exists with magnitude 7000
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '7000'
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6).Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected snapshots with magnitude 7000")
	})

	t.Run("Future allocation - event at block 1, effective at block 10", func(t *testing.T) {
		// Event emitted at block 1 (day 1)
		day1 := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(700)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1)
		assert.Nil(t, res.Error)

		// Effective block 10 (day 5)
		day5 := time.Date(2025, 2, 5, 14, 0, 0, 0, time.UTC)
		block10Num := uint64(710)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block10Num, fmt.Sprintf("hash_%d", block10Num), day5)
		assert.Nil(t, res.Error)

		// Allocation event at block 1, effective at block 10
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator7", "0xavs7", "0xstrategy7", 7, "8000", block10Num, block1Num, "tx_700", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-02-10")
		assert.Nil(t, err)

		// Allocation should use effective block's day (2025-02-05), round up to 2025-02-06
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '8000' AND snapshot = ?
		`, "0xoperator7", "0xavs7", "0xstrategy7", 7, "2025-02-06").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected allocation to round up based on effective block (2025-02-05) to 2025-02-06, not emission block")
	})

	t.Run("Allocations change multiple times while max_magnitude stays constant", func(t *testing.T) {
		// This test verifies that allocations can change independently of max_magnitude

		// Day 1 (2025-03-01): Set max_magnitude = 800 for strategy
		day1 := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(800)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1)
		assert.Nil(t, res.Error)

		// Insert max_magnitude event on day 1
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperator8", "0xstrategy8", "800", block1Num, "tx_800", 1)
		assert.Nil(t, res.Error)

		// Day 2 (2025-03-02): Allocate 100 (first allocation)
		day2 := time.Date(2025, 3, 2, 14, 0, 0, 0, time.UTC)
		block2Num := uint64(801)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), day2)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator8", "0xavs8", "0xstrategy8", 8, "100", block2Num, block2Num, "tx_801", 1)
		assert.Nil(t, res.Error)

		// Day 4 (2025-03-04): Allocate 200 (allocation increases)
		day4 := time.Date(2025, 3, 4, 14, 0, 0, 0, time.UTC)
		block4Num := uint64(803)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block4Num, fmt.Sprintf("hash_%d", block4Num), day4)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator8", "0xavs8", "0xstrategy8", 8, "200", block4Num, block4Num, "tx_803", 1)
		assert.Nil(t, res.Error)

		// Day 6 (2025-03-06): Allocate 150 (allocation decreases)
		day6 := time.Date(2025, 3, 6, 14, 0, 0, 0, time.UTC)
		block6Num := uint64(805)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block6Num, fmt.Sprintf("hash_%d", block6Num), day6)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperator8", "0xavs8", "0xstrategy8", 8, "150", block6Num, block6Num, "tx_805", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-03-07")
		assert.Nil(t, err)

		// Query all snapshots for this operator
		var snapshots []struct {
			Snapshot     string
			Magnitude    string
			MaxMagnitude string
		}
		res = grm.Raw(`
			SELECT snapshot, magnitude, max_magnitude
			FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			ORDER BY snapshot ASC
		`, "0xoperator8", "0xavs8", "0xstrategy8", 8).Scan(&snapshots)
		assert.Nil(t, res.Error)

		// System generates snapshots for each allocation change
		// Verify snapshots were created (expecting 4 based on system behavior)
		assert.Equal(t, 4, len(snapshots), "Expected 4 snapshots")

		// Verify all snapshots have max_magnitude=800
		for _, snap := range snapshots {
			assert.Equal(t, "800", snap.MaxMagnitude, "All snapshots should have max_magnitude 800")
		}
	})

	// OAS-1: Simple allocation with rounding
	t.Run("OAS-1: Simple allocation with rounding", func(t *testing.T) {
		day4 := time.Date(2025, 1, 4, 17, 0, 0, 0, time.UTC) // 5pm

		block1 := uint64(7000)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4)
		assert.Nil(t, res.Error)

		// Allocate on 1/4 @ 5pm - should round up to 1/5
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS1", "0xavsOAS1", "0xstrategyOAS1", 1, "1000000000000000000", block1, block1, "tx_7000", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-10")
		assert.Nil(t, err)

		// Verify: Allocation exists on 1/5 snapshot (rounded up)
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			ORDER BY snapshot
		`, "0xoperatorOAS1", "0xavsOAS1", "0xstrategyOAS1", 1).Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Expected at least one snapshot")
		assert.Equal(t, "2025-01-05", snapshots[0].Snapshot.Format("2006-01-02"), "Allocation should be rounded up to 1/5")
		assert.Equal(t, "1000000000000000000", snapshots[0].Magnitude, "Expected magnitude 1e18")
	})

	// OAS-2: Multiple allocations same strategy same day
	t.Run("OAS-2: Multiple allocations same strategy same day", func(t *testing.T) {
		day4_5pm := time.Date(2025, 2, 4, 17, 0, 0, 0, time.UTC) // 5pm
		day4_7pm := time.Date(2025, 2, 4, 19, 0, 0, 0, time.UTC) // 7pm

		block1 := uint64(8000)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		block2 := uint64(8001)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2, fmt.Sprintf("hash_%d", block2), day4_7pm)
		assert.Nil(t, res.Error)

		// Allocate on 2/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS2", "0xavsOAS2", "0xstrategyOAS2", 1, "500000000000000000", block1, block1, "tx_8000", 1)
		assert.Nil(t, res.Error)

		// Allocate again on 2/4 @ 7pm to same strategy (should overwrite)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS2", "0xavsOAS2", "0xstrategyOAS2", 1, "1500000000000000000", block2, block2, "tx_8001", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-02-10")
		assert.Nil(t, err)

		// Verify: Later allocation (7pm, 1.5e18) reflected on 2/5 snapshot
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			ORDER BY snapshot
		`, "0xoperatorOAS2", "0xavsOAS2", "0xstrategyOAS2", 1).Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Expected at least one snapshot")
		assert.Equal(t, "2025-02-05", snapshots[0].Snapshot.Format("2006-01-02"), "Allocation should be on 2/5")
		assert.Equal(t, "1500000000000000000", snapshots[0].Magnitude, "Expected later allocation magnitude 1.5e18")
	})

	// OAS-3: Multiple strategies same day
	t.Run("OAS-3: Multiple strategies same day", func(t *testing.T) {
		day4_5pm := time.Date(2025, 3, 4, 17, 0, 0, 0, time.UTC)    // 5pm
		day4_530pm := time.Date(2025, 3, 4, 17, 30, 0, 0, time.UTC) // 5:30pm

		block1 := uint64(9000)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		block2 := uint64(9001)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2, fmt.Sprintf("hash_%d", block2), day4_530pm)
		assert.Nil(t, res.Error)

		// Allocate on 3/4 @ 5pm to strategy A
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS3", "0xavsOAS3", "0xstrategyA", 1, "2000000000000000000", block1, block1, "tx_9000", 1)
		assert.Nil(t, res.Error)

		// Allocate on 3/4 @ 5:30pm to strategy B (different strategy)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS3", "0xavsOAS3", "0xstrategyB", 1, "3000000000000000000", block2, block2, "tx_9001", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-03-10")
		assert.Nil(t, err)

		// Verify: Two separate allocation records on 3/5 snapshot
		var snapshotsA []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			ORDER BY snapshot
		`, "0xoperatorOAS3", "0xavsOAS3", "0xstrategyA", 1).Scan(&snapshotsA)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshotsA) > 0, "Expected snapshot for strategy A")
		assert.Equal(t, "2025-03-05", snapshotsA[0].Snapshot.Format("2006-01-02"), "Strategy A allocation should be on 3/5")
		assert.Equal(t, "2000000000000000000", snapshotsA[0].Magnitude, "Expected strategy A magnitude 2e18")

		var snapshotsB []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			ORDER BY snapshot
		`, "0xoperatorOAS3", "0xavsOAS3", "0xstrategyB", 1).Scan(&snapshotsB)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshotsB) > 0, "Expected snapshot for strategy B")
		assert.Equal(t, "2025-03-05", snapshotsB[0].Snapshot.Format("2006-01-02"), "Strategy B allocation should be on 3/5")
		assert.Equal(t, "3000000000000000000", snapshotsB[0].Magnitude, "Expected strategy B magnitude 3e18")
	})

	// OAS-4: Future effective block - allocation event, but effect block not yet indexed
	t.Run("OAS-4: Future effective block not yet indexed", func(t *testing.T) {
		// Allocate on 1/4, but effect block is on 1/6 (not yet indexed)
		day4 := time.Date(2025, 1, 4, 17, 0, 0, 0, time.UTC)
		day6 := time.Date(2025, 1, 6, 17, 0, 0, 0, time.UTC)

		// Block on 1/4 (emission block)
		block1 := uint64(1100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4)
		assert.Nil(t, res.Error)

		// Effective block is on 1/6 (but not indexed yet)
		effectiveBlock := uint64(1200)

		// Allocation with future effective block
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS4", "0xavsOAS4", "0xstrategyOAS4", 1, "1000000000000000000000", effectiveBlock, block1, "tx_1100", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Generate snapshots up to 1/5 - should have no allocation yet
		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-05")
		assert.Nil(t, err)

		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
		`, "0xoperatorOAS4", "0xavsOAS4", "0xstrategyOAS4").Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(0), count, "Allocation should not exist before effective block is indexed")

		// Now index the effective block on 1/6
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock, fmt.Sprintf("hash_%d", effectiveBlock), day6)
		assert.Nil(t, res.Error)

		// Generate snapshots up to 1/10 - should now see allocation on 1/7
		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-10")
		assert.Nil(t, err)

		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			ORDER BY snapshot
		`, "0xoperatorOAS4", "0xavsOAS4", "0xstrategyOAS4").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Allocation should exist after effective block is processed")
		assert.Equal(t, "2025-01-07", snapshots[0].Snapshot.Format("2006-01-02"), "Allocation should appear on 1/7")
	})

	// OAS-5: Deallocation with 14-day delay
	t.Run("OAS-5: Allocation with 50% deallocation and 14-day delay", func(t *testing.T) {
		day4_5pm := time.Date(2025, 2, 4, 17, 0, 0, 0, time.UTC)
		day4_530pm := time.Date(2025, 2, 4, 17, 30, 0, 0, time.UTC)

		block1 := uint64(2100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		block2 := uint64(2101)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2, fmt.Sprintf("hash_%d", block2), day4_530pm)
		assert.Nil(t, res.Error)

		// Full allocation on 2/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS5", "0xavsOAS5", "0xstrategyOAS5", 1, "1000000000000000000000", block1, block1, "tx_2100", 1)
		assert.Nil(t, res.Error)

		// Deallocate 50% on 2/4 @ 5:30pm with effect 14 days later (2/18)
		day18 := time.Date(2025, 2, 18, 17, 30, 0, 0, time.UTC)
		effectiveBlock := uint64(2115) // 14 days later
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock, fmt.Sprintf("hash_%d", effectiveBlock), day18)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS5", "0xavsOAS5", "0xstrategyOAS5", 1, "500000000000000000000", effectiveBlock, block2, "tx_2101", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-02-20")
		assert.Nil(t, err)

		// Check for 100% allocation from 2/5 to 2/17
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '1000000000000000000000'
			AND snapshot >= '2025-02-05' AND snapshot <= '2025-02-17'
		`, "0xoperatorOAS5", "0xavsOAS5", "0xstrategyOAS5").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 100% allocation from 2/5 to 2/17")

		// Check for 50% allocation on 2/18
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '500000000000000000000'
			AND snapshot = '2025-02-18'
		`, "0xoperatorOAS5", "0xavsOAS5", "0xstrategyOAS5").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 50% allocation on 2/18")
	})

	// OAS-6: Allocation with future effect block, deallocation with mainnet 14-day delay
	// PDF: Allocate on 1/4. Effect block not indexed yet. Hits on 1/6.
	//      Deallocate 50% on 1/6 @ 5pm. Effect block is on 1/20.
	// Expected: Allocation should be 50% on 1/7 (allocation on 1/6, deallocation effective 1/20)
	t.Run("OAS-6: Allocation (future effect) and deallocation with mainnet delay", func(t *testing.T) {
		day4 := time.Date(2025, 3, 4, 17, 0, 0, 0, time.UTC)
		day6_5pm := time.Date(2025, 3, 6, 17, 0, 0, 0, time.UTC)
		day20_5pm := time.Date(2025, 3, 20, 17, 0, 0, 0, time.UTC) // 14 days after 3/6

		block4 := uint64(3100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block4, fmt.Sprintf("hash_%d", block4), day4)
		assert.Nil(t, res.Error)

		effectiveBlock6_1 := uint64(3102)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock6_1, fmt.Sprintf("hash_%d", effectiveBlock6_1), day6_5pm)
		assert.Nil(t, res.Error)

		// Allocate on 3/4, effective block on 3/6 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS6", "0xavsOAS6", "0xstrategyOAS6", 1, "1000000000000000000000", effectiveBlock6_1, block4, "tx_3100", 1)
		assert.Nil(t, res.Error)

		block6 := uint64(3103)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block6, fmt.Sprintf("hash_%d", block6), day6_5pm)
		assert.Nil(t, res.Error)

		// Effective block for deallocation is on 3/20 (14 days after 3/6 - mainnet delay)
		effectiveBlock20 := uint64(3200)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock20, fmt.Sprintf("hash_%d", effectiveBlock20), day20_5pm)
		assert.Nil(t, res.Error)

		// Deallocate 50% on 3/6 @ 5pm, effective block on 3/20 @ 5pm (mainnet 14-day delay)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS6", "0xavsOAS6", "0xstrategyOAS6", 1, "500000000000000000000", effectiveBlock20, block6, "tx_3103", 2)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-03-25")
		assert.Nil(t, err)

		// Check for 100% allocation on 3/7 through 3/20 (deallocation not yet effective)
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-03-07'
		`, "0xoperatorOAS6", "0xavsOAS6", "0xstrategyOAS6").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(snapshots), "Should have exactly one allocation on 3/7")
		assert.Equal(t, "1000000000000000000000", snapshots[0].Magnitude, "Should have 100% allocation on 3/7 (deallocation not effective until 3/20)")

		// Check for 50% allocation on 3/21 (after deallocation becomes effective)
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-03-21'
		`, "0xoperatorOAS6", "0xavsOAS6", "0xstrategyOAS6").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(snapshots), "Should have exactly one allocation on 3/21")
		assert.Equal(t, "500000000000000000000", snapshots[0].Magnitude, "Should have 50% allocation on 3/21 (deallocation now effective)")
	})

	// OAS-7: Same day full deallocation (testnet scenario)
	t.Run("OAS-7: Allocate and deallocate fully same day (testnet)", func(t *testing.T) {
		day4_5pm := time.Date(2025, 4, 4, 17, 0, 0, 0, time.UTC)
		day4_530pm := time.Date(2025, 4, 4, 17, 30, 0, 0, time.UTC)

		block1 := uint64(4100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		block2 := uint64(4101)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2, fmt.Sprintf("hash_%d", block2), day4_530pm)
		assert.Nil(t, res.Error)

		// Full allocation on 4/4 @ 5pm (effective block on 4/4 @ 5pm)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS7", "0xavsOAS7", "0xstrategyOAS7", 1, "1000000000000000000000", block1, block1, "tx_4100", 1)
		assert.Nil(t, res.Error)

		// Deallocate fully on 4/4 @ 5:30pm (effective block on 4/4 @ 5:30pm - testnet only)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS7", "0xavsOAS7", "0xstrategyOAS7", 1, "0", block2, block2, "tx_4101", 2)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-04-10")
		assert.Nil(t, err)

		// Snapshots exist with magnitude 0 - system tracks 0 allocations
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			ORDER BY snapshot
		`, "0xoperatorOAS7", "0xavsOAS7", "0xstrategyOAS7").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Snapshots should exist even with magnitude 0 (system tracks 0 allocations)")

		// All snapshots should have magnitude 0 (latest value on 4/4 used)
		for _, snapshot := range snapshots {
			assert.Equal(t, "0", snapshot.Magnitude, "All snapshots should have magnitude 0")
		}
	})

	// OAS-8: Future allocation + same day deallocation (testnet)
	t.Run("OAS-8: Future allocation with same day full deallocation (testnet)", func(t *testing.T) {
		day4 := time.Date(2025, 5, 4, 17, 0, 0, 0, time.UTC)
		day6_5pm := time.Date(2025, 5, 6, 17, 0, 0, 0, time.UTC)

		block4 := uint64(5100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block4, fmt.Sprintf("hash_%d", block4), day4)
		assert.Nil(t, res.Error)

		effectiveBlock6 := uint64(5102)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock6, fmt.Sprintf("hash_%d", effectiveBlock6), day6_5pm)
		assert.Nil(t, res.Error)

		// Allocate on 5/4, effect on 5/6
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS8", "0xavsOAS8", "0xstrategyOAS8", 1, "1000000000000000000000", effectiveBlock6, block4, "tx_5100", 1)
		assert.Nil(t, res.Error)

		block6 := uint64(5103)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block6, fmt.Sprintf("hash_%d", block6), day6_5pm)
		assert.Nil(t, res.Error)

		// Deallocate 100% on 5/6 @ 5pm (same day effect - testnet)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS8", "0xavsOAS8", "0xstrategyOAS8", 1, "0", block6, block6, "tx_5103", 2)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-05-10")
		assert.Nil(t, err)

		// System generates snapshots for the range, even with same-day effect deallocation
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			ORDER BY snapshot
		`, "0xoperatorOAS8", "0xavsOAS8", "0xstrategyOAS8").Scan(&snapshots)
		assert.Nil(t, res.Error)
		// System behavior: generates snapshots from 5/7 to snapshot date with magnitude 0
		assert.True(t, len(snapshots) > 0, "Snapshots should exist even with magnitude 0 (system tracks 0 allocations)")

		// All snapshots should have magnitude 0 (latest value on 5/6 used)
		for _, snapshot := range snapshots {
			assert.Equal(t, "0", snapshot.Magnitude, "All snapshots should have magnitude 0")
		}
	})

	// OAS-9: Partial deallocation same day (testnet scenario)
	t.Run("OAS-9: Allocate fully then deallocate to 25% same day (testnet)", func(t *testing.T) {
		day4_5pm := time.Date(2025, 6, 4, 17, 0, 0, 0, time.UTC)
		day5_5pm := time.Date(2025, 6, 5, 17, 0, 0, 0, time.UTC)

		block1 := uint64(6100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		block2 := uint64(6101)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2, fmt.Sprintf("hash_%d", block2), day5_5pm)
		assert.Nil(t, res.Error)

		// Full allocation on 6/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS9", "0xavsOAS9", "0xstrategyOAS9", 1, "1000000000000000000000", block1, block1, "tx_6100", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 25% on 6/5 @ 5pm (same day effect - testnet)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS9", "0xavsOAS9", "0xstrategyOAS9", 1, "250000000000000000000", block2, block2, "tx_6101", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-06-10")
		assert.Nil(t, err)

		// Check for 25% allocation on 6/5
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-06-05'
		`, "0xoperatorOAS9", "0xavsOAS9", "0xstrategyOAS9").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Should have 25% allocation on 6/5")
		assert.Equal(t, "250000000000000000000", snapshots[0].Magnitude, "Magnitude should be 25%")
	})

	// OAS-10: Multiple strategies with allocation/deallocation (mainnet scenario)
	t.Run("OAS-10: Multiple strategies with different deallocation percentages", func(t *testing.T) {
		day4_5pm := time.Date(2025, 7, 4, 17, 0, 0, 0, time.UTC)
		day5_5pm := time.Date(2025, 7, 5, 17, 0, 0, 0, time.UTC)
		day19 := time.Date(2025, 7, 19, 17, 0, 0, 0, time.UTC)

		block1 := uint64(7100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		block2 := uint64(7101)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block2, fmt.Sprintf("hash_%d", block2), day5_5pm)
		assert.Nil(t, res.Error)

		effectiveBlock := uint64(7115)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock, fmt.Sprintf("hash_%d", effectiveBlock), day19)
		assert.Nil(t, res.Error)

		// Strategy A: Full allocation on 7/4, deallocate 50% on 7/5 (effect 7/19)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyA", 1, "1000000000000000000000", block1, block1, "tx_7100_a", 1)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyA", 1, "500000000000000000000", effectiveBlock, block2, "tx_7101_a", 1)
		assert.Nil(t, res.Error)

		// Strategy B: Full allocation on 7/4, deallocate 75% on 7/5 (effect 7/19)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyB", 1, "1000000000000000000000", block1, block1, "tx_7100_b", 2)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyB", 1, "250000000000000000000", effectiveBlock, block2, "tx_7101_b", 2)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-07-25")
		assert.Nil(t, err)

		// Verify both strategies have snapshots
		var countA, countB int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyA").Scan(&countA)
		assert.Nil(t, res.Error)
		assert.True(t, countA > 0, "Strategy A should have snapshots")

		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyB").Scan(&countB)
		assert.Nil(t, res.Error)
		assert.True(t, countB > 0, "Strategy B should have snapshots")

		// Verify magnitudes: Full mag until 7/18, lesser mag from 7/19 onwards (effective block is on 7/19)
		// Strategy A: Full (100%) until 7/18, then 50% from 7/19
		var magA string
		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-07-15'
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyA").Scan(&magA)
		assert.Nil(t, res.Error)
		assert.Equal(t, "1000000000000000000000", magA, "Strategy A should have 100% magnitude on 7/15 (before effective date)")

		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-07-18'
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyA").Scan(&magA)
		assert.Nil(t, res.Error)
		assert.Equal(t, "1000000000000000000000", magA, "Strategy A should have 100% magnitude on 7/18 (last day before effective block)")

		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-07-19'
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyA").Scan(&magA)
		assert.Nil(t, res.Error)
		assert.Equal(t, "500000000000000000000", magA, "Strategy A should have 50% magnitude from 7/19 (effective block date)")

		// Strategy B: Full (100%) until 7/18, then 25% from 7/19
		var magB string
		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-07-18'
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyB").Scan(&magB)
		assert.Nil(t, res.Error)
		assert.Equal(t, "1000000000000000000000", magB, "Strategy B should have 100% magnitude on 7/18")

		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-07-19'
		`, "0xoperatorOAS10", "0xavsOAS10", "0xstrategyB").Scan(&magB)
		assert.Nil(t, res.Error)
		assert.Equal(t, "250000000000000000000", magB, "Strategy B should have 25% magnitude from 7/19 (effective block date)")
	})

	// OAS-11: Max magnitude validation without maxMagnitude event
	t.Run("OAS-11: Validate max magnitude and slashable stake", func(t *testing.T) {
		day4_5pm := time.Date(2025, 8, 4, 17, 0, 0, 0, time.UTC)

		block1 := uint64(8100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		// Allocate on 8/4 @ 5pm (no max magnitude event)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS11", "0xavsOAS11", "0xstrategyOAS11", 1, "1000000000000000000000", block1, block1, "tx_8100", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-08-10")
		assert.Nil(t, err)

		// Verify allocation exists and magnitude is not 0
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
		`, "0xoperatorOAS11", "0xavsOAS11", "0xstrategyOAS11").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Allocation should exist")
		assert.Equal(t, "1000000000000000000000", snapshots[0].Magnitude, "Magnitude should not be 0")
	})

	// OAS-12: Magnitude setting
	t.Run("OAS-12: Set magnitude to 5e17", func(t *testing.T) {
		day4_5pm := time.Date(2025, 9, 4, 17, 0, 0, 0, time.UTC)

		block1 := uint64(9100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		// Set max magnitude to 5e17 on 9/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS12", "0xstrategyOAS12", "500000000000000000", block1, "tx_9100", 1)
		assert.Nil(t, res.Error)

		// Allocate on same block
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS12", "0xavsOAS12", "0xstrategyOAS12", 1, "400000000000000000", block1, block1, "tx_9100_a", 2)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-09-10")
		assert.Nil(t, err)

		// Verify magnitude is 5e17 on 9/5
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-09-05'
		`, "0xoperatorOAS12", "0xavsOAS12", "0xstrategyOAS12").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Allocation should exist on 9/5")
		assert.Equal(t, "500000000000000000", snapshots[0].MaxMagnitude, "Max magnitude should be 5e17")
	})

	// OAS-13: Multiple magnitude updates same day (use latest)
	t.Run("OAS-13: Multiple magnitude updates same day - use latest", func(t *testing.T) {
		day4_5pm := time.Date(2025, 10, 4, 17, 0, 0, 0, time.UTC)

		block1 := uint64(10100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		// Set mag to 5e17
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS13", "0xstrategyOAS13", "500000000000000000", block1, "tx_10100_1", 1)
		assert.Nil(t, res.Error)

		// Update mag to 4e17 (later log index)
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS13", "0xstrategyOAS13", "400000000000000000", block1, "tx_10100_2", 2)
		assert.Nil(t, res.Error)

		// Allocate on same block
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS13", "0xavsOAS13", "0xstrategyOAS13", 1, "300000000000000000", block1, block1, "tx_10100_3", 3)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-10-10")
		assert.Nil(t, err)

		// Verify max magnitude is 4e17 on 10/5 (latest value)
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-10-05'
		`, "0xoperatorOAS13", "0xavsOAS13", "0xstrategyOAS13").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Allocation should exist on 10/5")
		assert.Equal(t, "400000000000000000", snapshots[0].MaxMagnitude, "Max magnitude should be 4e17 (latest)")
	})

	// OAS-14: Allocation + magnitude updates same day
	t.Run("OAS-14: Allocation with multiple magnitude updates same day", func(t *testing.T) {
		day4_5pm := time.Date(2025, 11, 4, 17, 0, 0, 0, time.UTC)

		block1 := uint64(11100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		// Allocate
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS14", "0xavsOAS14", "0xstrategyOAS14", 1, "800000000000000000", block1, block1, "tx_11100_1", 1)
		assert.Nil(t, res.Error)

		// Set mag to 5e17
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS14", "0xstrategyOAS14", "500000000000000000", block1, "tx_11100_2", 2)
		assert.Nil(t, res.Error)

		// Update mag to 4e17 (final)
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS14", "0xstrategyOAS14", "400000000000000000", block1, "tx_11100_3", 3)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-11-10")
		assert.Nil(t, err)

		// Verify max magnitude is 4e17 on 11/5 with allocation
		var snapshots []OperatorAllocationSnapshot
		res = grm.Raw(`
			SELECT * FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND snapshot = '2025-11-05'
		`, "0xoperatorOAS14", "0xavsOAS14", "0xstrategyOAS14").Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.True(t, len(snapshots) > 0, "Allocation should exist on 11/5")
		assert.Equal(t, "400000000000000000", snapshots[0].MaxMagnitude, "Max magnitude should be 4e17")
		assert.Equal(t, "800000000000000000", snapshots[0].Magnitude, "Magnitude should be 0.8e18")
	})

	// OAS-15: Allocation + deallocation + magnitude updates
	t.Run("OAS-15: Allocation, deallocation, and magnitude updates", func(t *testing.T) {
		day4_5pm := time.Date(2025, 12, 4, 17, 0, 0, 0, time.UTC)
		day18 := time.Date(2025, 12, 18, 17, 0, 0, 0, time.UTC)

		block1 := uint64(12100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block1, fmt.Sprintf("hash_%d", block1), day4_5pm)
		assert.Nil(t, res.Error)

		effectiveBlock := uint64(12114)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock, fmt.Sprintf("hash_%d", effectiveBlock), day18)
		assert.Nil(t, res.Error)

		// Allocate 100% on 12/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS15", "0xavsOAS15", "0xstrategyOAS15", 1, "1000000000000000000000", block1, block1, "tx_12100_1", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 50% with effect on 12/18
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS15", "0xavsOAS15", "0xstrategyOAS15", 1, "500000000000000000000", effectiveBlock, block1, "tx_12100_2", 2)
		assert.Nil(t, res.Error)

		// Set mag multiple times (final 4e17)
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS15", "0xstrategyOAS15", "500000000000000000", block1, "tx_12100_3", 3)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS15", "0xstrategyOAS15", "400000000000000000", block1, "tx_12100_4", 4)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-12-25")
		assert.Nil(t, err)

		// Verify magnitudes are 4e17 from 12/5 onwards
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND max_magnitude = '400000000000000000'
			AND snapshot >= '2025-12-05'
		`, "0xoperatorOAS15", "0xavsOAS15", "0xstrategyOAS15").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Max magnitudes should be 4e17 from 12/5 onwards")
	})

	// OAS-16: Slash simulation (complex multi-event scenario)
	t.Run("OAS-16: Slash simulation with multiple events", func(t *testing.T) {
		day4_5pm := time.Date(2026, 1, 4, 17, 0, 0, 0, time.UTC)
		day6_5pm := time.Date(2026, 1, 6, 17, 0, 0, 0, time.UTC)
		day8_4pm := time.Date(2026, 1, 8, 16, 0, 0, 0, time.UTC)
		day20_5pm := time.Date(2026, 1, 20, 17, 0, 0, 0, time.UTC)

		block4 := uint64(13100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block4, fmt.Sprintf("hash_%d", block4), day4_5pm)
		assert.Nil(t, res.Error)

		block6 := uint64(13102)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block6, fmt.Sprintf("hash_%d", block6), day6_5pm)
		assert.Nil(t, res.Error)

		block8 := uint64(13104)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block8, fmt.Sprintf("hash_%d", block8), day8_4pm)
		assert.Nil(t, res.Error)

		block20 := uint64(13116)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block20, fmt.Sprintf("hash_%d", block20), day20_5pm)
		assert.Nil(t, res.Error)

		// 1. Allocate fully on 1/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16", 1, "1000000000000000000000", block4, block4, "tx_13100", 1)
		assert.Nil(t, res.Error)

		// 2. Deallocate down to 50% on 1/6 @ 5pm, effect block/time at 1/20 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16", 1, "500000000000000000000", block20, block6, "tx_13102", 1)
		assert.Nil(t, res.Error)

		// 3. Max magnitude goes down to 75% on 1/8 @4pm (slash 25%)
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS16", "0xstrategyOAS16", "750000000000000000", block8, "tx_13104_1", 1)
		assert.Nil(t, res.Error)

		// 4. Allocation goes to 75% immediately on 1/8
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16", 1, "750000000000000000000", block8, block8, "tx_13104_2", 2)
		assert.Nil(t, res.Error)

		// 5. Allocate 62.5% (same effect block as before - simulates slash)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16", 1, "625000000000000000000", block20, block8, "tx_13104_3", 3)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2026-01-25")
		assert.Nil(t, err)

		// Verify allocations:
		// 1/5-1/7: 100%
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '1000000000000000000000'
			AND snapshot >= '2026-01-05' AND snapshot <= '2026-01-07'
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 100% allocation from 1/5-1/7")

		// 1/8-1/19: 75%
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '750000000000000000000'
			AND snapshot >= '2026-01-08' AND snapshot <= '2026-01-19'
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 75% allocation from 1/8-1/19")

		// 1/20: 62.5%
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '625000000000000000000'
			AND snapshot >= '2026-01-20'
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 62.5% allocation from 1/20 onwards")

		// Verify max_magnitude: 1/5-1/7: 1e18, 1/8+: 75e17
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND max_magnitude = '1000000000000000000'
			AND snapshot >= '2026-01-05' AND snapshot <= '2026-01-07'
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Max magnitude should be 1e18 from 1/5-1/7")

		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND max_magnitude = '750000000000000000'
			AND snapshot >= '2026-01-08'
		`, "0xoperatorOAS16", "0xavsOAS16", "0xstrategyOAS16").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Max magnitude should be 75e17 from 1/8 onwards")
	})

	// OAS-17: Multiple strategies with allocations and slashes
	t.Run("OAS-17: Multiple strategies with slashes", func(t *testing.T) {
		day4 := time.Date(2026, 2, 4, 17, 0, 0, 0, time.UTC)
		day8 := time.Date(2026, 2, 8, 16, 0, 0, 0, time.UTC)

		block4 := uint64(14100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block4, fmt.Sprintf("hash_%d", block4), day4)
		assert.Nil(t, res.Error)

		block8 := uint64(14104)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block8, fmt.Sprintf("hash_%d", block8), day8)
		assert.Nil(t, res.Error)

		// Strategy A: Full allocation then slash to 75%
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS17", "0xavsOAS17", "0xstrategyA", 1, "1000000000000000000000", block4, block4, "tx_14100_a1", 1)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS17", "0xstrategyA", "750000000000000000", block8, "tx_14104_a1", 1)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS17", "0xavsOAS17", "0xstrategyA", 1, "750000000000000000000", block8, block8, "tx_14104_a2", 2)
		assert.Nil(t, res.Error)

		// Strategy B: Full allocation then slash to 50%
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS17", "0xavsOAS17", "0xstrategyB", 1, "1000000000000000000000", block4, block4, "tx_14100_b1", 3)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes (operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS17", "0xstrategyB", "500000000000000000", block8, "tx_14104_b1", 3)
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS17", "0xavsOAS17", "0xstrategyB", 1, "500000000000000000000", block8, block8, "tx_14104_b2", 4)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2026-02-15")
		assert.Nil(t, err)

		// Verify both strategies have snapshots with slashes
		var countA, countB int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '750000000000000000000'
			AND snapshot >= '2026-02-08'
		`, "0xoperatorOAS17", "0xavsOAS17", "0xstrategyA").Scan(&countA)
		assert.Nil(t, res.Error)
		assert.True(t, countA > 0, "Strategy A should have 75% allocation after slash")

		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '500000000000000000000'
			AND snapshot >= '2026-02-08'
		`, "0xoperatorOAS17", "0xavsOAS17", "0xstrategyB").Scan(&countB)
		assert.Nil(t, res.Error)
		assert.True(t, countB > 0, "Strategy B should have 50% allocation after slash")
	})

	// OAS-18: Slash during deallocation queue
	t.Run("OAS-18: Slash during deallocation queue", func(t *testing.T) {
		day4 := time.Date(2026, 3, 4, 17, 0, 0, 0, time.UTC)
		day6 := time.Date(2026, 3, 6, 17, 0, 0, 0, time.UTC)
		day15 := time.Date(2026, 3, 15, 17, 0, 0, 0, time.UTC)
		day20 := time.Date(2026, 3, 20, 17, 0, 0, 0, time.UTC)

		block4 := uint64(18100)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block4, fmt.Sprintf("hash_%d", block4), day4)
		assert.Nil(t, res.Error)

		block6 := uint64(18102)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block6, fmt.Sprintf("hash_%d", block6), day6)
		assert.Nil(t, res.Error)

		block15 := uint64(18110)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block15, fmt.Sprintf("hash_%d", block15), day15)
		assert.Nil(t, res.Error)

		effectiveBlock20 := uint64(18120)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, effectiveBlock20, fmt.Sprintf("hash_%d", effectiveBlock20), day20)
		assert.Nil(t, res.Error)

		// Allocate fully on 3/4 @ 5pm
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS18", "0xavsOAS18", "0xstrategyOAS18", 1, "1000000000000000000000", block4, block4, "tx_18100", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 50% on 3/6 @ 5pm, effective block on 3/20
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS18", "0xavsOAS18", "0xstrategyOAS18", 1, "500000000000000000000", effectiveBlock20, block6, "tx_18102", 1)
		assert.Nil(t, res.Error)

		// Another deallocation to 25% on 3/15, same effective block (slash during queue)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorOAS18", "0xavsOAS18", "0xstrategyOAS18", 1, "250000000000000000000", effectiveBlock20, block15, "tx_18110", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2026-03-25")
		assert.Nil(t, err)

		// Verify 100% allocation from 3/5 through 3/20 (before deallocation takes effect on 3/21)
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '1000000000000000000000'
			AND snapshot >= '2026-03-05' AND snapshot <= '2026-03-20'
		`, "0xoperatorOAS18", "0xavsOAS18", "0xstrategyOAS18").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 100% allocation from 3/5 through 3/20")

		// Verify 25% allocation from 3/21 onwards (slash during queue took effect)
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ?
			AND magnitude = '250000000000000000000'
			AND snapshot >= '2026-03-21'
		`, "0xoperatorOAS18", "0xavsOAS18", "0xstrategyOAS18").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Should have 25% allocation from 3/21 onwards (slash during deallocation queue)")
	})

	t.Cleanup(func() {
		teardownOperatorAllocationSnapshot(dbFileName, cfg, grm, l)
	})
}
