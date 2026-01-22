package rewards

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func Test_RewardsV2_2(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	// Run with TEST_REWARDS_FULL=true to enable this test.
	if os.Getenv("TEST_REWARDS_FULL") != "true" {
		t.Skipf("Skipping slow full pipeline test. Set TEST_REWARDS_FULL=true to run.")
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	fmt.Printf("Using db file: %+v\n", dbFileName)

	if err != nil {
		t.Fatal(err)
	}

	// Enable v2.2 for testing
	cfg.Rewards.RewardsV2_2Enabled = true

	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)

	t.Run("Should initialize the rewards calculator with v2.2", func(t *testing.T) {
		rc, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)
		if err != nil {
			t.Fatal(err)
		}
		assert.NotNil(t, rc)

		fmt.Printf("DB Path: %+v\n", dbFileName)

		testStart := time.Now()

		// Setup all tables and source data
		_, err = hydrateRewardsV2Blocks(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorAvsStateChangesTable(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorAvsRestakedStrategies(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorShareDeltas(grm, l)
		assert.Nil(t, err)

		err = hydrateStakerDelegations(grm, l)
		assert.Nil(t, err)

		err = hydrateStakerShareDeltas(grm, l)
		assert.Nil(t, err)

		err = hydrateRewardSubmissionsTable(grm, l)
		assert.Nil(t, err)

		// RewardsV2 tables
		err = hydrateOperatorAvsSplits(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorPISplits(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorDirectedRewardSubmissionsTable(grm, l)
		assert.Nil(t, err)

		// RewardsV2_1 tables
		err = hydrateOperatorSetOperatorRegistrationsTable(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorSetStrategyRegistrationsTable(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorSetSplits(grm, l)
		assert.Nil(t, err)

		err = hydrateOperatorDirectedOperatorSetRewardSubmissionsTable(grm, l)
		assert.Nil(t, err)

		// RewardsV2_2 tables - Operator allocations for unique stake
		err = hydrateOperatorAllocations(grm, l)
		assert.Nil(t, err)

		t.Log("Hydrated tables")

		snapshotDates := []string{
			"2025-02-10", // Date after Pecos fork for v2.2
		}

		fmt.Printf("Hydration duration: %v\n", time.Since(testStart))
		testStart = time.Now()

		for _, snapshotDate := range snapshotDates {
			t.Log("-----------------------------\n")

			snapshotStartTime := time.Now()

			t.Logf("Generating rewards - snapshotDate: %s", snapshotDate)
			// Generate snapshots
			err = rc.generateSnapshotData(snapshotDate)
			assert.Nil(t, err)

			goldTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)

			fmt.Printf("Snapshot duration: %v\n", time.Since(testStart))
			testStart = time.Now()

			t.Log("Generated and inserted snapshots")
			forks, err := cfg.GetRewardsSqlForkDates()
			assert.Nil(t, err)

			fmt.Printf("Running gold_1_active_rewards\n")
			err = rc.Generate1ActiveRewards(snapshotDate)
			assert.Nil(t, err)
			rows, err := getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_1_ActiveRewards])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_1_active_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_2_staker_reward_amounts %+v\n", time.Now())
			err = rc.GenerateGold2StakerRewardAmountsTable(snapshotDate, forks)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_2_StakerRewardAmounts])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_2_staker_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_3_operator_reward_amounts\n")
			err = rc.GenerateGold3OperatorRewardAmountsTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_3_OperatorRewardAmounts])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_3_operator_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_4_rewards_for_all\n")
			err = rc.GenerateGold4RewardsForAllTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_4_RewardsForAll])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_4_rewards_for_all: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_5_rfae_stakers\n")
			err = rc.GenerateGold5RfaeStakersTable(snapshotDate, forks)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_5_RfaeStakers])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_5_rfae_stakers: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_6_rfae_operators\n")
			err = rc.GenerateGold6RfaeOperatorsTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_6_RfaeOperators])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_6_rfae_operators: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			// ------------------------------------------------------------------------
			// Rewards V2
			// ------------------------------------------------------------------------
			rewardsV2Enabled, err := cfg.IsRewardsV2EnabledForCutoffDate(snapshotDate)
			assert.Nil(t, err)

			fmt.Printf("Running gold_7_active_od_rewards\n")
			err = rc.Generate7ActiveODRewards(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_7_ActiveODRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_7_active_od_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_8_operator_od_reward_amounts\n")
			err = rc.GenerateGold8OperatorODRewardAmountsTable(snapshotDate, forks)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_8_OperatorODRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_8_operator_od_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_9_staker_od_reward_amounts\n")
			err = rc.GenerateGold9StakerODRewardAmountsTable(snapshotDate, forks)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_9_StakerODRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_9_staker_od_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_10_avs_od_reward_amounts\n")
			err = rc.GenerateGold10AvsODRewardAmountsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_10_AvsODRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_10_avs_od_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			// ------------------------------------------------------------------------
			// Rewards V2.1 (Should be skipped when v2.2 is enabled)
			// ------------------------------------------------------------------------

			rewardsV2_1Enabled, err := cfg.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
			assert.Nil(t, err)

			fmt.Printf("Running gold_11_active_od_operator_set_rewards\n")
			err = rc.GenerateGold11ActiveODOperatorSetRewards(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_11_active_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			// ------------------------------------------------------------------------
			// Rewards V2.2 (Unique Stake)
			// ------------------------------------------------------------------------

			rewardsV2_2Enabled, err := cfg.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
			assert.Nil(t, err)
			assert.True(t, rewardsV2_2Enabled, "v2.2 should be enabled for this test")

			fmt.Printf("Running gold_15_operator_od_operator_set_rewards_v2_2\n")
			err = rc.GenerateGold15OperatorOperatorSetUniqueStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_15_OperatorOperatorSetUniqueStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_15_operator_od_operator_set_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_16_staker_od_operator_set_rewards_v2_2\n")
			err = rc.GenerateGold16StakerOperatorSetUniqueStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_16_StakerOperatorSetUniqueStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_16_staker_od_operator_set_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_17_avs_od_operator_set_rewards_v2_2\n")
			err = rc.GenerateGold17AvsOperatorSetUniqueStakeRewardsTable(snapshotDate, forks)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_17_AvsOperatorSetUniqueStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_17_avs_od_operator_set_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_18_staging\n")
			err = rc.GenerateGold21StagingTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_21_GoldStaging])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_18_staging: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_final_table\n")
			err = rc.GenerateGold22FinalTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, "gold_table")
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_table: %v - [time: %v]\n", rows, time.Since(testStart))

			goldRows, err := rc.ListGoldRows()
			assert.Nil(t, err)

			t.Logf("Gold staging rows for snapshot %s: %d", snapshotDate, len(goldRows))
			for i, row := range goldRows {
				if strings.EqualFold(row.RewardHash, strings.ToLower("0xB38AB57E8E858F197C07D0CDF61F34EB07C3D0FC58390417DDAD0BF528681909")) &&
					strings.EqualFold(row.Earner, strings.ToLower("0xaFF71569D30ED876987088a62E0EA881EBc761E6")) {
					t.Logf("%d: %s %s %s %s %s", i, row.Earner, row.Snapshot.String(), row.RewardHash, row.Token, row.Amount)
				}
			}

			t.Logf("Generating staker operators table")
			err = rc.sog.GenerateStakerOperatorsTable(snapshotDate)
			assert.Nil(t, err)

			// V2.2 specific validation - ensure operator allocations snapshot was created
			operatorAllocSnapshotTable := goldTableNames[rewardsUtils.Table_OperatorAllocationSnapshots]
			rows, err = getRowCountForTable(grm, operatorAllocSnapshotTable)
			assert.Nil(t, err)
			t.Logf("Operator allocation snapshots: %v", rows)
			assert.True(t, rows > 0, "Operator allocation snapshots should be created for v2.2")

			fmt.Printf("Total duration for rewards compute %s: %v\n", snapshotDate, time.Since(snapshotStartTime))
			testStart = time.Now()
		}

		fmt.Printf("Done!\n\n")
		t.Cleanup(func() {
			// teardownRewards(dbFileName, cfg, grm, l)
		})
	})
}

// hydrateOperatorAllocations creates test data for operator allocations (unique stake)
func hydrateOperatorAllocations(grm *gorm.DB, l *zap.Logger) error {
	// Get an existing block number from the blocks table to use as reference
	var blockNumber int64
	res := grm.Raw("SELECT number FROM blocks ORDER BY number ASC LIMIT 1").Scan(&blockNumber)
	if res.Error != nil || blockNumber == 0 {
		// Fallback: insert a block if none exists
		blockNumber = 100
		grm.Exec(`INSERT INTO blocks (number, hash, block_time) VALUES (?, ?, ?) ON CONFLICT (number) DO NOTHING`,
			blockNumber, "0xblock100", "2025-01-01 00:00:00")
	}

	records := []map[string]interface{}{
		// Operator 1 allocates stake to operator set 0
		{
			"operator":         "0xa067defa8e919ebad10f3c4168a77e29a46e0b3f",
			"avs":              "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
			"strategy":         "0x93c4b944d05dfe6df7645a86cd2206016c51564d",
			"magnitude":        "1000000000000000000", // 1 ETH
			"operator_set_id":  0,
			"effective_block":  blockNumber,
			"transaction_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
			"log_index":        1,
			"block_number":     blockNumber,
		},
		// Operator 2 allocates stake to operator set 1
		{
			"operator":         "0x9e91cc6d7a6ada3e2f1f1c4eaa39e35d8e7a6c29",
			"avs":              "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
			"strategy":         "0x93c4b944d05dfe6df7645a86cd2206016c51564d",
			"magnitude":        "2000000000000000000", // 2 ETH
			"operator_set_id":  1,
			"effective_block":  blockNumber,
			"transaction_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
			"log_index":        2,
			"block_number":     blockNumber,
		},
	}

	for _, record := range records {
		res := grm.Exec(`
			INSERT INTO operator_allocations 
			(operator, avs, strategy, magnitude, operator_set_id, effective_block, transaction_hash, log_index, block_number)
			VALUES (@operator, @avs, @strategy, @magnitude, @operator_set_id, @effective_block, @transaction_hash, @log_index, @block_number)
		`,
			sql.Named("operator", record["operator"]),
			sql.Named("avs", record["avs"]),
			sql.Named("strategy", record["strategy"]),
			sql.Named("magnitude", record["magnitude"]),
			sql.Named("operator_set_id", record["operator_set_id"]),
			sql.Named("effective_block", record["effective_block"]),
			sql.Named("transaction_hash", record["transaction_hash"]),
			sql.Named("log_index", record["log_index"]),
			sql.Named("block_number", record["block_number"]),
		)
		if res.Error != nil {
			l.Sugar().Errorw("Failed to insert operator allocation", "error", res.Error)
			return res.Error
		}
	}

	return nil
}

// =============================================================================
// Refund Tests (formerly in rewards_v22_test.go)
// =============================================================================

func setupRewardsV22RefundTest() (
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

func teardownRewardsV22RefundTest(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
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

	dbFileName, cfg, grm, l, sink, err := setupRewardsV22RefundTest()
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
		teardownRewardsV22RefundTest(dbFileName, cfg, grm, l)
	})
}
