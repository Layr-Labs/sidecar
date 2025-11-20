package rewards

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"github.com/stretchr/testify/assert"
)

func Test_RewardsV2_1(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewardsV2()
	fmt.Printf("Using db file: %+v\n", dbFileName)

	if err != nil {
		t.Fatal(err)
	}

	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)

	t.Run("Should initialize the rewards calculator", func(t *testing.T) {
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

		t.Log("Hydrated tables")

		snapshotDates := []string{
			"2025-02-05",
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
			// Rewards V2.1
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

			fmt.Printf("Running gold_15_staging\n")
			err = rc.GenerateGold21StagingTable(snapshotDate)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_21_GoldStaging])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_15_staging: %v - [time: %v]\n", rows, time.Since(testStart))
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
				// t.Logf("%d: %s %s %s %s %s", i, row.Earner, row.Snapshot.String(), row.RewardHash, row.Token, row.Amount)
			}

			t.Logf("Generating staker operators table")
			err = rc.sog.GenerateStakerOperatorsTable(snapshotDate)
			assert.Nil(t, err)

			fmt.Printf("Total duration for rewards compute %s: %v\n", snapshotDate, time.Since(snapshotStartTime))
			testStart = time.Now()
		}

		fmt.Printf("Done!\n\n")
		t.Cleanup(func() {
			// teardownRewards(dbFileName, cfg, grm, l)
		})
	})
}
