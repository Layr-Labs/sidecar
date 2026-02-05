package rewards

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/totalStakeRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/uniqueStakeRewardSubmissions"
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

		// RewardsV2_2 tables - Operator set registrations and supporting data
		err = hydrateV2_2OperatorSetRegistrations(grm, l)
		assert.Nil(t, err)

		// RewardsV2_2 tables - Stake reward submissions (unique and total)
		// Using v2.2-specific hydrate functions with correct block numbers and dates
		err = hydrateUniqueStakeRewardSubmissionsForV2_2(grm, l)
		assert.Nil(t, err)

		err = hydrateTotalStakeRewardSubmissionsForV2_2(grm, l)
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
			// Rewards V2.1 (Required for v2.2 - operator set rewards)
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

			fmt.Printf("Running gold_12_operator_od_operator_set_rewards\n")
			err = rc.GenerateGold12OperatorODOperatorSetRewardAmountsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_12_OperatorODOperatorSetRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_12_operator_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_13_staker_od_operator_set_rewards\n")
			err = rc.GenerateGold13StakerODOperatorSetRewardAmountsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_13_StakerODOperatorSetRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_13_staker_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_14_avs_od_operator_set_rewards\n")
			err = rc.GenerateGold14AvsODOperatorSetRewardAmountsTable(snapshotDate, forks)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_14_AvsODOperatorSetRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_14_avs_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			// ------------------------------------------------------------------------
			// Rewards V2.2 (Stake-based Unique/Total Stake)
			// ------------------------------------------------------------------------

			rewardsV2_2Enabled, err := cfg.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
			assert.Nil(t, err)
			assert.True(t, rewardsV2_2Enabled, "v2.2 should be enabled for this test")

			// Generate stake operator set rewards (denormalized table)
			fmt.Printf("Running stake_operator_set_rewards\n")
			err = rc.GenerateAndInsertStakeOperatorSetRewards(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, "stake_operator_set_rewards")
				assert.Nil(t, err)
				fmt.Printf("\tRows in stake_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			// Table 15: Active Unique and Total Stake Rewards (source for unique/total stake calculations)
			fmt.Printf("Running gold_15_active_unique_and_total_stake_rewards\n")
			err = rc.GenerateGold15ActiveUniqueAndTotalStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_15_ActiveUniqueAndTotalStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_15_active_unique_and_total_stake_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_16_operator_od_operator_set_rewards_v2_2\n")
			err = rc.GenerateGold16OperatorOperatorSetUniqueStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_16_OperatorOperatorSetUniqueStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_16_operator_od_operator_set_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_17_staker_od_operator_set_rewards_v2_2\n")
			err = rc.GenerateGold17StakerOperatorSetUniqueStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_17_StakerOperatorSetUniqueStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_17_staker_od_operator_set_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_18_avs_od_operator_set_rewards_v2_2\n")
			err = rc.GenerateGold18AvsOperatorSetUniqueStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_18_AvsOperatorSetUniqueStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_18_avs_od_operator_set_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			// ------------------------------------------------------------------------
			// Rewards V2.2 (Total Stake)
			// ------------------------------------------------------------------------

			fmt.Printf("Running gold_19_operator_total_stake_rewards_v2_2\n")
			err = rc.GenerateGold19OperatorOperatorSetTotalStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_19_OperatorOperatorSetTotalStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_19_operator_total_stake_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_20_staker_total_stake_rewards_v2_2\n")
			err = rc.GenerateGold20StakerOperatorSetTotalStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_20_StakerOperatorSetTotalStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_20_staker_total_stake_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_21_avs_total_stake_rewards_v2_2\n")
			err = rc.GenerateGold21AvsOperatorSetTotalStakeRewardsTable(snapshotDate)
			assert.Nil(t, err)
			if rewardsV2_2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_21_AvsOperatorSetTotalStakeRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_21_avs_total_stake_rewards_v2_2: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_22_staging\n")
			err = rc.GenerateGold22StagingTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_22_GoldStaging])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_18_staging: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_final_table\n")
			err = rc.GenerateGold23FinalTable(snapshotDate)
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
			rows, err = getRowCountForTable(grm, "operator_allocation_snapshots")
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
	// Use block numbers that exist in the test data (rewardsV2Blocks.sql)
	// Block 2923053 is from 2024-12-12 17:04:00
	records := []map[string]interface{}{
		// Operator 1 allocates stake to operator set 0
		{
			"operator":         "0xa067defa8e919ebad10f3c4168a77e29a46e0b3f",
			"avs":              "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
			"strategy":         "0x93c4b944d05dfe6df7645a86cd2206016c51564d",
			"magnitude":        "1000000000000000000", // 1 ETH
			"operator_set_id":  0,
			"effective_block":  2923050,
			"transaction_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
			"log_index":        1,
			"block_number":     2923050,
		},
		// Operator 2 allocates stake to operator set 1
		{
			"operator":         "0x9e91cc6d7a6ada3e2f1f1c4eaa39e35d8e7a6c29",
			"avs":              "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
			"strategy":         "0x93c4b944d05dfe6df7645a86cd2206016c51564d",
			"magnitude":        "2000000000000000000", // 2 ETH
			"operator_set_id":  1,
			"effective_block":  2923051,
			"transaction_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
			"log_index":        2,
			"block_number":     2923051,
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

// hydrateUniqueStakeRewardSubmissionsForV2_2 creates test data for unique stake reward submissions
// using block numbers and dates that align with the v2.2 test data (block 2923050, Dec 2024)
// and a reward period that includes the 2025-02-10 snapshot date
func hydrateUniqueStakeRewardSubmissionsForV2_2(grm *gorm.DB, l *zap.Logger) error {
	// Reward period: Dec 12, 2024 to Feb 28, 2025 (78 days)
	// This ensures rewards are active for the 2025-02-10 snapshot
	startTime := time.Date(2024, 12, 12, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	duration := uint64(endTime.Sub(startTime).Seconds()) // ~78 days in seconds

	reward := uniqueStakeRewardSubmissions.UniqueStakeRewardSubmission{
		// Use the same AVS as hydrateOperatorAllocations
		Avs:             "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
		OperatorSetId:   0, // Match operator set from hydrateOperatorAllocations
		RewardHash:      "0xunique_stake_v2_2_test_hash_0001",
		Token:           "0x94373a4919b3240d86ea41593d5eba789fef3848", // EIGEN token
		Amount:          "1000000000000000000000000",                  // 1M tokens
		Strategy:        "0x93c4b944d05dfe6df7645a86cd2206016c51564d", // Same strategy as allocations
		StrategyIndex:   0,
		Multiplier:      "1000000000000000000", // 1e18
		StartTimestamp:  &startTime,
		EndTimestamp:    &endTime,
		Duration:        duration,
		BlockNumber:     2923050, // Block that exists in rewardsV2Blocks.sql
		TransactionHash: "0xunique_stake_v2_2_tx_hash",
		LogIndex:        100,
	}

	result := grm.Create(&reward)
	if result.Error != nil {
		l.Sugar().Errorw("Failed to create unique stake reward submission for v2.2", "error", result.Error)
		return result.Error
	}
	return nil
}

// hydrateTotalStakeRewardSubmissionsForV2_2 creates test data for total stake reward submissions
// using block numbers and dates that align with the v2.2 test data
func hydrateTotalStakeRewardSubmissionsForV2_2(grm *gorm.DB, l *zap.Logger) error {
	// Reward period: Dec 12, 2024 to Feb 28, 2025
	startTime := time.Date(2024, 12, 12, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	duration := uint64(endTime.Sub(startTime).Seconds())

	reward := totalStakeRewardSubmissions.TotalStakeRewardSubmission{
		// Use the same AVS as hydrateOperatorAllocations
		Avs:             "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
		OperatorSetId:   1, // Different operator set for variety
		RewardHash:      "0xtotal_stake_v2_2_test_hash_0001",
		Token:           "0x94373a4919b3240d86ea41593d5eba789fef3848", // EIGEN token
		Amount:          "2000000000000000000000000",                  // 2M tokens
		Strategy:        "0x93c4b944d05dfe6df7645a86cd2206016c51564d", // Same strategy
		StrategyIndex:   0,
		Multiplier:      "1000000000000000000", // 1e18
		StartTimestamp:  &startTime,
		EndTimestamp:    &endTime,
		Duration:        duration,
		BlockNumber:     2923050,
		TransactionHash: "0xtotal_stake_v2_2_tx_hash",
		LogIndex:        101,
	}

	result := grm.Create(&reward)
	if result.Error != nil {
		l.Sugar().Errorw("Failed to create total stake reward submission for v2.2", "error", result.Error)
		return result.Error
	}
	return nil
}

// hydrateV2_2OperatorSetRegistrations creates operator set registrations for v2.2 test data
// This ensures operators are registered to the AVS/operator sets used in stake rewards
func hydrateV2_2OperatorSetRegistrations(grm *gorm.DB, l *zap.Logger) error {
	avs := "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0"
	operator1 := "0xa067defa8e919ebad10f3c4168a77e29a46e0b3f"
	operator2 := "0x9e91cc6d7a6ada3e2f1f1c4eaa39e35d8e7a6c29"
	strategy := "0x93c4b944d05dfe6df7645a86cd2206016c51564d"
	blockNumber := uint64(2923050)

	// Register operators to operator sets
	res := grm.Exec(`
		INSERT INTO operator_set_operator_registrations
		(operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index)
		VALUES
		(?, ?, 0, true, ?, '0xv2_2_op_reg_1', 0),
		(?, ?, 1, true, ?, '0xv2_2_op_reg_2', 1)
	`, operator1, avs, blockNumber, operator2, avs, blockNumber)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to create operator set operator registrations for v2.2", "error", res.Error)
		return res.Error
	}

	// Register strategy to operator sets
	res = grm.Exec(`
		INSERT INTO operator_set_strategy_registrations
		(strategy, avs, operator_set_id, is_active, block_number, transaction_hash, log_index)
		VALUES
		(?, ?, 0, true, ?, '0xv2_2_strat_reg_1', 0),
		(?, ?, 1, true, ?, '0xv2_2_strat_reg_2', 1)
	`, strategy, avs, blockNumber, strategy, avs, blockNumber)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to create operator set strategy registrations for v2.2", "error", res.Error)
		return res.Error
	}

	// Add operator max magnitudes
	res = grm.Exec(`
		INSERT INTO operator_max_magnitudes
		(operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
		VALUES
		(?, ?, '1000000000000000000', ?, '0xv2_2_max_mag_1', 0),
		(?, ?, '2000000000000000000', ?, '0xv2_2_max_mag_2', 1)
	`, operator1, strategy, blockNumber, operator2, strategy, blockNumber)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to create operator max magnitudes for v2.2", "error", res.Error)
		return res.Error
	}

	// Add operator share deltas
	// Block 2923050 has block_time '2024-12-12 17:03:24'
	res = grm.Exec(`
		INSERT INTO operator_share_deltas
		(operator, strategy, shares, block_number, transaction_hash, log_index, block_time, block_date, staker)
		VALUES
		(?, ?, '1000000000000000000000', ?, '0xv2_2_op_share_1', 0, '2024-12-12 17:03:24', '2024-12-12', ?),
		(?, ?, '2000000000000000000000', ?, '0xv2_2_op_share_2', 1, '2024-12-12 17:03:24', '2024-12-12', ?)
	`, operator1, strategy, blockNumber, operator1, operator2, strategy, blockNumber, operator2)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to create operator share deltas for v2.2", "error", res.Error)
		return res.Error
	}

	return nil
}
