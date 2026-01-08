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
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, blockNum, fmt.Sprintf("hash_%d", blockNum), blockTime, blockTime.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		// First allocation should round up to next day (2024-12-02)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
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
				INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
				VALUES (?, ?, ?, ?, '', NOW(), NOW())
			`, b.number, fmt.Sprintf("hash_%d", b.number), blockTime, blockTime.Format("2006-01-02"))
			assert.Nil(t, res.Error)

			res = grm.Exec(`
				INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
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
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), block1Time, block1Time.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator4", "0xavs4", "0xstrategy4", 4, "5000", block1Num, block1Num, "tx_400", 1)
		assert.Nil(t, res.Error)

		// Deallocate at 16:00 (same day)
		block2Time := baseDate.Add(16 * time.Hour)
		block2Num := uint64(401)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), block2Time, block2Time.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator4", "0xavs4", "0xstrategy4", 4, "1000", block2Num, block2Num, "tx_401", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2024-12-25")
		assert.Nil(t, err)

		// Verify the latest record (deallocation to 1000) is used
		// Since it's a decrease, it should round down to 2024-12-20
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '1000' AND snapshot = ?
		`, "0xoperator4", "0xavs4", "0xstrategy4", 4, "2024-12-20").Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected latest deallocation (1000) to be used, rounded down to 2024-12-20")
	})

	t.Run("Allocate day 1, deallocate to 0 day 2 at 12pm - 0 days counted", func(t *testing.T) {
		// Allocate on day 1
		day1 := time.Date(2025, 1, 10, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(500)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1, day1.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator5", "0xavs5", "0xstrategy5", 5, "6000", block1Num, block1Num, "tx_500", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 0 on day 2 at 12pm (noon)
		day2Noon := time.Date(2025, 1, 11, 12, 0, 0, 0, time.UTC)
		block2Num := uint64(501)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), day2Noon, day2Noon.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator5", "0xavs5", "0xstrategy5", 5, "0", block2Num, block2Num, "tx_501", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-15")
		assert.Nil(t, err)

		// Allocation on day 1 (2025-01-10) rounds up to 2025-01-11
		// Deallocation on day 2 (2025-01-11) rounds down to 2025-01-11
		// So the range is [2025-01-11, 2025-01-11) which generates no days
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
		`, "0xoperator5", "0xavs5", "0xstrategy5", 5).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(0), count, "Expected 0 days to be counted (allocation rounds up to day 2, deallocation rounds down to day 2)")
	})

	t.Run("Allocate day 1, deallocate to 0 day 3 at 12pm - 1 day counted", func(t *testing.T) {
		// Allocate on day 1
		day1 := time.Date(2025, 1, 20, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(600)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1, day1.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6, "7000", block1Num, block1Num, "tx_600", 1)
		assert.Nil(t, res.Error)

		// Deallocate to 0 on day 3 at 12pm (noon)
		day3Noon := time.Date(2025, 1, 22, 12, 0, 0, 0, time.UTC)
		block2Num := uint64(601)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), day3Noon, day3Noon.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6, "0", block2Num, block2Num, "tx_601", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-25")
		assert.Nil(t, err)

		// Allocation on day 1 (2025-01-20) rounds up to 2025-01-21
		// Deallocation on day 3 (2025-01-22) rounds down to 2025-01-22
		// So the range is [2025-01-21, 2025-01-22) which generates 1 day (2025-01-21)
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(1), count, "Expected 1 day to be counted (2025-01-21)")

		// Verify the snapshot is on 2025-01-21 with magnitude 7000
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE operator = ? AND avs = ? AND strategy = ? AND operator_set_id = ?
			AND magnitude = '7000' AND snapshot = ?
		`, "0xoperator6", "0xavs6", "0xstrategy6", 6, "2025-01-21").Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(1), count, "Expected snapshot on 2025-01-21 with magnitude 7000")
	})

	t.Run("Future allocation - event at block 1, effective at block 10", func(t *testing.T) {
		// Event emitted at block 1 (day 1)
		day1 := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
		block1Num := uint64(700)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1, day1.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		// Effective block 10 (day 5)
		day5 := time.Date(2025, 2, 5, 14, 0, 0, 0, time.UTC)
		block10Num := uint64(710)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block10Num, fmt.Sprintf("hash_%d", block10Num), day5, day5.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		// Allocation event at block 1, effective at block 10
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
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
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block1Num, fmt.Sprintf("hash_%d", block1Num), day1, day1.Format("2006-01-02"))
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
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block2Num, fmt.Sprintf("hash_%d", block2Num), day2, day2.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator8", "0xavs8", "0xstrategy8", 8, "100", block2Num, block2Num, "tx_801", 1)
		assert.Nil(t, res.Error)

		// Day 4 (2025-03-04): Allocate 200 (allocation increases)
		day4 := time.Date(2025, 3, 4, 14, 0, 0, 0, time.UTC)
		block4Num := uint64(803)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block4Num, fmt.Sprintf("hash_%d", block4Num), day4, day4.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`, "0xoperator8", "0xavs8", "0xstrategy8", 8, "200", block4Num, block4Num, "tx_803", 1)
		assert.Nil(t, res.Error)

		// Day 6 (2025-03-06): Allocate 150 (allocation decreases)
		day6 := time.Date(2025, 3, 6, 14, 0, 0, 0, time.UTC)
		block6Num := uint64(805)
		res = grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (?, ?, ?, ?, '', NOW(), NOW())
		`, block6Num, fmt.Sprintf("hash_%d", block6Num), day6, day6.Format("2006-01-02"))
		assert.Nil(t, res.Error)

		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
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

		// Verify exactly 3 snapshots
		assert.Equal(t, 3, len(snapshots), "Expected 3 snapshots")

		// Verify each snapshot has correct magnitude and max_magnitude=800
		// Day 3: allocation 100, max_magnitude 800
		assert.Equal(t, "2025-03-03", snapshots[0].Snapshot, "First snapshot should be 2025-03-03 (day 2 allocation rounded up)")
		assert.Equal(t, "100", snapshots[0].Magnitude, "First snapshot should have magnitude 100")
		assert.Equal(t, "800", snapshots[0].MaxMagnitude, "First snapshot should have max_magnitude 800")

		// Day 5: allocation 200, max_magnitude 800
		assert.Equal(t, "2025-03-05", snapshots[1].Snapshot, "Second snapshot should be 2025-03-05 (day 4 allocation rounded up)")
		assert.Equal(t, "200", snapshots[1].Magnitude, "Second snapshot should have magnitude 200")
		assert.Equal(t, "800", snapshots[1].MaxMagnitude, "Second snapshot should have max_magnitude 800")

		// Day 6: allocation 150, max_magnitude 800 (decrease rounds down)
		assert.Equal(t, "2025-03-06", snapshots[2].Snapshot, "Third snapshot should be 2025-03-06 (day 6 allocation rounded down)")
		assert.Equal(t, "150", snapshots[2].Magnitude, "Third snapshot should have magnitude 150")
		assert.Equal(t, "800", snapshots[2].MaxMagnitude, "Third snapshot should have max_magnitude 800")
	})

	t.Cleanup(func() {
		teardownOperatorAllocationSnapshot(dbFileName, cfg, grm, l)
	})
}
