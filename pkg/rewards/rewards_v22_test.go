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

func setupRewardsV22Test() (
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

func teardownRewardsV22Test(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func Test_RewardsV22_Refunds(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV22Test()
	if err != nil {
		t.Fatal(err)
	}

	// R-1: Operator registers for opSet on 1/1, rewards sent for 1/1-1/10, no allocation
	t.Run("R-1: Full period refund - no allocation", func(t *testing.T) {
		day1 := time.Date(2025, 1, 1, 17, 0, 0, 0, time.UTC)
		day10 := time.Date(2025, 1, 10, 17, 0, 0, 0, time.UTC)

		blocks := []struct {
			number uint64
			time   time.Time
		}{
			{4000, day1},
			{4010, day10},
		}

		for _, b := range blocks {
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time)
				VALUES (?, ?, ?)
			`, b.number, fmt.Sprintf("hash_%d", b.number), b.time)
			assert.Nil(t, res.Error)
		}

		// Operator registers but doesn't allocate
		res := grm.Exec(`
			INSERT INTO operator_set_operator_registrations (operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorR1", "0xavsR1", 1, true, 4000, "tx_4000_reg", 1)
		assert.Nil(t, res.Error)

		// Rewards submission for 1/1-1/10 period
		endTimestamp := day1.Add(9 * 24 * time.Hour)
		res = grm.Exec(`
			INSERT INTO reward_submissions (avs, reward_hash, token, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, is_for_all, block_number, reward_type)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xavsR1", "0xhash1", "0xtoken1", "10000000000000000000", "0xstrat1", 0, "1000000000000000000", day1, endTimestamp, 9*24*3600, false, 4010, "operator_set")
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Process rewards
		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-15")
		assert.Nil(t, err)

		// Verify: AVS should get full refund (no allocations exist)
		var allocCount int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ?
		`, "0xavsR1", "0xoperatorR1").Scan(&allocCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(0), allocCount, "Expected no allocations, so full refund")
	})

	// R-2: Operator registers and allocates, rewards sent for 1/1-1/10
	t.Run("R-2: Partial refund - allocation starts mid-period", func(t *testing.T) {
		day1 := time.Date(2025, 1, 1, 17, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 1, 5, 17, 0, 0, 0, time.UTC)
		day10 := time.Date(2025, 1, 10, 17, 0, 0, 0, time.UTC)

		blocks := []struct {
			number uint64
			time   time.Time
		}{
			{4020, day1},
			{4025, day5},
			{4030, day10},
		}

		for _, b := range blocks {
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time)
				VALUES (?, ?, ?)
			`, b.number, fmt.Sprintf("hash_%d", b.number), b.time)
			assert.Nil(t, res.Error)
		}

		// Operator registers on 1/1
		res := grm.Exec(`
			INSERT INTO operator_set_operator_registrations (operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorR2", "0xavsR2", 2, true, 4020, "tx_4020_reg", 1)
		assert.Nil(t, res.Error)

		// Allocates on 1/5 (effective next day 1/6)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorR2", "0xavsR2", "0xstratR2", 2, "1000000000000000000", 4025, 4025, "tx_4025", 1)
		assert.Nil(t, res.Error)

		// Rewards for 1/1-1/10
		endTimestampR2 := day1.Add(9 * 24 * time.Hour)
		res = grm.Exec(`
			INSERT INTO reward_submissions (avs, reward_hash, token, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, is_for_all, block_number, reward_type)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xavsR2", "0xhashR2", "0xtokenR2", "10000000000000000000", "0xstratR2", 0, "1000000000000000000", day1, endTimestampR2, 9*24*3600, false, 4030, "operator_set")
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-15")
		assert.Nil(t, err)

		// AVS should get refund for 1/1-1/5 (5 days out of 10, so ~50% refund)
		// Allocations should start from 1/6
		var allocCount int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ? AND snapshot >= ?
		`, "0xavsR2", "0xoperatorR2", "2025-01-06").Scan(&allocCount)
		assert.Nil(t, res.Error)
		assert.True(t, allocCount > 0, "Expected allocations from 1/6 onwards")

		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ? AND snapshot < ?
		`, "0xavsR2", "0xoperatorR2", "2025-01-06").Scan(&allocCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, int64(0), allocCount, "Expected no allocations before 1/6 (refund period)")
	})

	// R-3: Operator allocates then deallocates before period ends
	t.Run("R-3: Refund for post-deallocation period", func(t *testing.T) {
		day1 := time.Date(2025, 1, 1, 17, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 1, 5, 17, 0, 0, 0, time.UTC)
		day19 := time.Date(2025, 1, 19, 17, 0, 0, 0, time.UTC)
		day30 := time.Date(2025, 1, 30, 17, 0, 0, 0, time.UTC)

		blocks := []struct {
			number uint64
			time   time.Time
		}{
			{4040, day1},
			{4045, day5},
			{4059, day19},
			{4070, day30},
		}

		for _, b := range blocks {
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time)
				VALUES (?, ?, ?)
			`, b.number, fmt.Sprintf("hash_%d", b.number), b.time)
			assert.Nil(t, res.Error)
		}

		// Operator allocates on 1/1
		res := grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorR3", "0xavsR3", "0xstratR3", 3, "1000000000000000000", 4040, 4040, "tx_4040", 1)
		assert.Nil(t, res.Error)

		// Deallocates fully on 1/5, effect at 1/19
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorR3", "0xavsR3", "0xstratR3", 3, "0", 4059, 4045, "tx_4045", 1)
		assert.Nil(t, res.Error)

		// Rewards for 1/1-1/30
		endTimestampR3 := day1.Add(29 * 24 * time.Hour)
		res = grm.Exec(`
			INSERT INTO reward_submissions (avs, reward_hash, token, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, is_for_all, block_number, reward_type)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xavsR3", "0xhashR3", "0xtokenR3", "30000000000000000000", "0xstratR3", 0, "1000000000000000000", day1, endTimestampR3, 29*24*3600, false, 4070, "operator_set")
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-02-05")
		assert.Nil(t, err)

		// AVS should get refund for 1/1 and 1/19-1/30 (deallocation goes into effect on 1/19)
		var allocCount int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ?
			AND snapshot >= '2025-01-02' AND snapshot < '2025-01-19'
		`, "0xavsR3", "0xoperatorR3").Scan(&allocCount)
		assert.Nil(t, res.Error)
		assert.True(t, allocCount > 0, "Expected allocations from 1/2 to 1/18")

		// Allocations from 1/19 onwards should have magnitude 0 (refund period)
		var zeroMagCount int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ?
			AND snapshot >= '2025-01-19'
			AND magnitude = 0
		`, "0xavsR3", "0xoperatorR3").Scan(&zeroMagCount)
		assert.Nil(t, res.Error)
		assert.True(t, zeroMagCount > 0, "Expected allocations with magnitude 0 from 1/19 onwards (refund period)")
	})

	// R-4: Multiple operators with different allocation percentages
	t.Run("R-4: Multi-operator proportional distribution", func(t *testing.T) {
		day31 := time.Date(2024, 12, 31, 17, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 1, 5, 17, 0, 0, 0, time.UTC)
		day19 := time.Date(2025, 1, 19, 17, 0, 0, 0, time.UTC)

		blocks := []struct {
			number uint64
			time   time.Time
		}{
			{4080, day31},
			{4085, day5},
			{4099, day19},
		}

		for _, b := range blocks {
			res := grm.Exec(`
				INSERT INTO blocks (number, hash, block_time)
				VALUES (?, ?, ?)
			`, b.number, fmt.Sprintf("hash_%d", b.number), b.time)
			assert.Nil(t, res.Error)
		}

		// Operator A: 100 shares, allocates on 12/31
		res := grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorA", "0xavsR4", "0xstratR4", 4, "100000000000000000", 4080, 4080, "tx_4080_a", 1)
		assert.Nil(t, res.Error)

		// Operator B: 200 shares, allocates on 12/31
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorB", "0xavsR4", "0xstratR4", 4, "200000000000000000", 4080, 4080, "tx_4080_b", 1)
		assert.Nil(t, res.Error)

		// Operator A deallocates 50% on 1/5, effect on 1/19
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "0xoperatorA", "0xavsR4", "0xstratR4", 4, "50000000000000000", 4099, 4085, "tx_4085_a", 1)
		assert.Nil(t, res.Error)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2025-01-25")
		assert.Nil(t, err)

		// Verify Operator B gets 2/3 of rewards till 1/19 (200 / (100+200))
		var opBCount int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ?
			AND snapshot >= '2025-01-01' AND snapshot < '2025-01-19'
		`, "0xavsR4", "0xoperatorB").Scan(&opBCount)
		assert.Nil(t, res.Error)
		assert.True(t, opBCount > 0, "Expected Op B allocations from 1/1 to 1/18")

		// Starting 1/19, Operator B should get 4/5 of rewards (200 / (50+200))
		var opBMag string
		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ?
			AND snapshot = '2025-01-19'
			LIMIT 1
		`, "0xavsR4", "0xoperatorB").Scan(&opBMag)
		assert.Nil(t, res.Error)
		assert.Equal(t, "200000000000000000", opBMag, "Op B magnitude should remain 200")

		var opAMag string
		res = grm.Raw(`
			SELECT magnitude FROM operator_allocation_snapshots
			WHERE avs = ? AND operator = ?
			AND snapshot = '2025-01-19'
			LIMIT 1
		`, "0xavsR4", "0xoperatorA").Scan(&opAMag)
		assert.Nil(t, res.Error)
		assert.Equal(t, "50000000000000000", opAMag, "Op A magnitude should be 50 after deallocation")
	})

	t.Cleanup(func() {
		teardownRewardsV22Test(dbFileName, cfg, grm, l)
	})
}
