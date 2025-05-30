package rewards

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/metrics"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	postgres2 "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// const TOTAL_BLOCK_COUNT = 1229187

func rewardsTestsEnabled() bool {
	return os.Getenv("TEST_REWARDS") == "true"
}

func getRewardsTestContext() string {
	ctx := os.Getenv("REWARDS_TEST_CONTEXT")
	if ctx == "" {
		return "testnet"
	}
	return ctx
}

func getProjectRootPath() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	p, err := filepath.Abs(fmt.Sprintf("%s/../..", wd))
	if err != nil {
		panic(err)
	}
	return p
}

func getSnapshotDate() (string, error) {
	context := getRewardsTestContext()

	switch context {
	case "testnet":
		return "2024-09-01", nil
	case "testnet-reduced":
		return "2024-07-25", nil
	case "mainnet-reduced":
		return "2024-08-12", nil
	case "preprod-rewardsV2":
		return "2024-12-09", nil
	}
	return "", fmt.Errorf("Unknown context: %s", context)
}

func hydrateAllBlocksTable(grm *gorm.DB, l *zap.Logger) (int, error) {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetAllBlocksSqlFile(projectRoot)

	if err != nil {
		return 0, err
	}

	count := len(strings.Split(strings.Trim(contents, "\n"), "\n")) - 1

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return count, res.Error
	}
	return count, nil
}

func hydrateRewardsV2Blocks(grm *gorm.DB, l *zap.Logger) (int, error) {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetRewardsV2Blocks(projectRoot)

	if err != nil {
		return 0, err
	}

	count := len(strings.Split(strings.Trim(contents, "\n"), "\n")) - 1

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return count, res.Error
	}
	return count, nil
}

func getRowCountForTable(grm *gorm.DB, tableName string) (int, error) {
	query := fmt.Sprintf("select count(*) as cnt from %s", tableName)
	var count int
	res := grm.Raw(query).Scan(&count)

	if res.Error != nil {
		return 0, res.Error
	}
	return count, nil
}

func setupRewards() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	cfg := tests.GetConfig()
	cfg.Rewards.GenerateStakerOperatorsTable = true
	cfg.Rewards.ValidateRewardsRoot = true
	cfg.Chain = config.Chain_Mainnet

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func setupRewardsV2() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	cfg := tests.GetConfig()
	cfg.Rewards.GenerateStakerOperatorsTable = true
	cfg.Rewards.ValidateRewardsRoot = true
	cfg.Chain = config.Chain_Preprod

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func Test_Rewards(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewards()
	fmt.Printf("Using db file: %+v\n", dbFileName)

	if err != nil {
		t.Fatal(err)
	}

	projectRoot := getProjectRootPath()

	// snapshotDate, err := getSnapshotDate()
	// if err != nil {
	// 	t.Fatal(err)
	// }

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
		_, err = hydrateAllBlocksTable(grm, l)
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

		t.Log("Hydrated tables")

		snapshotDates := []string{
			"2024-08-02",
			"2024-08-11",
			// "2024-08-12",
			// "2024-08-19",
		}

		fmt.Printf("Hydration duration: %v\n", time.Since(testStart))
		testStart = time.Now()

		for _, snapshotDate := range snapshotDates {
			snapshotStatus, err := rc.CreateRewardSnapshotStatus(snapshotDate)
			if err != nil {
				t.Fatalf("Failed to create snapshot status: %v", err)
			}
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
			err = rc.Generate1ActiveRewards(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			rows, err := getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_1_ActiveRewards])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_1_active_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_2_staker_reward_amounts %+v\n", time.Now())
			err = rc.GenerateGold2StakerRewardAmountsTable(snapshotDate, snapshotStatus.Id, forks)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_2_StakerRewardAmounts])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_2_staker_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_3_operator_reward_amounts\n")
			err = rc.GenerateGold3OperatorRewardAmountsTable(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_3_OperatorRewardAmounts])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_3_operator_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_4_rewards_for_all\n")
			err = rc.GenerateGold4RewardsForAllTable(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_4_RewardsForAll])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_4_rewards_for_all: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_5_rfae_stakers\n")
			err = rc.GenerateGold5RfaeStakersTable(snapshotDate, snapshotStatus.Id, forks)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_5_RfaeStakers])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_5_rfae_stakers: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_6_rfae_operators\n")
			err = rc.GenerateGold6RfaeOperatorsTable(snapshotDate, snapshotStatus.Id)
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
			err = rc.Generate7ActiveODRewards(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_7_ActiveODRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_7_active_od_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_8_operator_od_reward_amounts\n")
			err = rc.GenerateGold8OperatorODRewardAmountsTable(snapshotDate, snapshotStatus.Id, forks)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_8_OperatorODRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_8_operator_od_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_9_staker_od_reward_amounts\n")
			err = rc.GenerateGold9StakerODRewardAmountsTable(snapshotDate, snapshotStatus.Id, forks)
			assert.Nil(t, err)
			if rewardsV2Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_9_StakerODRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_9_staker_od_reward_amounts: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_10_avs_od_reward_amounts\n")
			err = rc.GenerateGold10AvsODRewardAmountsTable(snapshotDate, snapshotStatus.Id)
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
			err = rc.GenerateGold11ActiveODOperatorSetRewards(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_11_active_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_12_operator_od_operator_set_rewards\n")
			err = rc.GenerateGold12OperatorODOperatorSetRewardAmountsTable(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_12_OperatorODOperatorSetRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_12_operator_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_13_staker_od_operator_set_rewards\n")
			err = rc.GenerateGold13StakerODOperatorSetRewardAmountsTable(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_13_StakerODOperatorSetRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_13_staker_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_14_avs_od_operator_set_rewards\n")
			err = rc.GenerateGold14AvsODOperatorSetRewardAmountsTable(snapshotDate, snapshotStatus.Id, forks)
			assert.Nil(t, err)
			if rewardsV2_1Enabled {
				rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_14_AvsODOperatorSetRewardAmounts])
				assert.Nil(t, err)
				fmt.Printf("\tRows in gold_14_avs_od_operator_set_rewards: %v - [time: %v]\n", rows, time.Since(testStart))
			}
			testStart = time.Now()

			fmt.Printf("Running gold_15_staging\n")
			err = rc.GenerateGold15StagingTable(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, goldTableNames[rewardsUtils.Table_15_GoldStaging])
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_15_staging: %v - [time: %v]\n", rows, time.Since(testStart))
			testStart = time.Now()

			fmt.Printf("Running gold_final_table\n")
			err = rc.GenerateGold16FinalTable(snapshotDate, snapshotStatus.Id)
			assert.Nil(t, err)
			rows, err = getRowCountForTable(grm, "gold_table")
			assert.Nil(t, err)
			fmt.Printf("\tRows in gold_table: %v - [time: %v]\n", rows, time.Since(testStart))

			goldRows, err := rc.ListGoldRows()
			assert.Nil(t, err)

			t.Logf("Gold staging rows for snapshot %s: %d", snapshotDate, len(goldRows))

			fmt.Printf("Total duration for rewards compute %s: %v\n", snapshotDate, time.Since(snapshotStartTime))
			testStart = time.Now()

			expectedRows, err := tests.GetGoldExpectedResults(projectRoot, snapshotDate)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, len(expectedRows), len(goldRows))
			t.Logf("Expected rows: %d, Gold staging rows: %d", len(expectedRows), len(goldRows))

			expectedRowsMap := make(map[string]*tests.GoldStagingExpectedResult)

			for _, row := range expectedRows {
				key := fmt.Sprintf("%s_%s_%s_%s", row.Earner, row.Snapshot, row.RewardHash, row.Token)
				if _, ok := expectedRowsMap[key]; !ok {
					expectedRowsMap[key] = row
				} else {
					t.Logf("Duplicate expected row found: %+v", row)
				}
			}

			missingRows := 0
			invalidAmounts := 0
			for i, row := range goldRows {
				key := fmt.Sprintf("%s_%s_%s_%s", row.Earner, row.Snapshot.Format(time.DateOnly), row.RewardHash, row.Token)
				foundRow, ok := expectedRowsMap[key]
				if !ok {
					missingRows++
					if missingRows < 100 {
						fmt.Printf("[%d] Row not found in expected results: %+v\n", i, row)
					}
					continue
				}
				if foundRow.Amount != row.Amount {
					invalidAmounts++
					if invalidAmounts < 100 {
						fmt.Printf("[%d] Amount mismatch: expected '%s', got '%s' for row: %+v\n", i, foundRow.Amount, row.Amount, row)
					}
				}
			}
			assert.Zero(t, missingRows)
			if missingRows > 0 {
				t.Fatalf("Missing rows: %d", missingRows)
			}

			assert.Zero(t, invalidAmounts)
			if invalidAmounts > 0 {
				t.Fatalf("Invalid amounts: %d", invalidAmounts)
			}

			t.Logf("Generating staker operators table")
			err = rc.sog.GenerateStakerOperatorsTable(snapshotDate)
			assert.Nil(t, err)

			accountTree, _, _, err := rc.MerkelizeRewardsForSnapshot(snapshotDate)
			assert.Nil(t, err)

			root := utils.ConvertBytesToString(accountTree.Root())
			t.Logf("Root: %s", root)
		}

		fmt.Printf("Done!\n\n")
		t.Cleanup(func() {
			// teardownRewards(dbFileName, cfg, grm, l)
		})
	})
}

func Test_RewardsCalculatorLock(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupRewards()
	fmt.Printf("Using db file: %+v\n", dbFileName)
	if err != nil {
		t.Fatal(err)
	}

	bs := postgres2.NewPostgresBlockStore(grm, l, cfg)

	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, bs, sog, sink, l)
	assert.Nil(t, err)

	// Setup all tables and source data
	_, err = hydrateAllBlocksTable(grm, l)
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

	t.Log("Hydrated tables")

	// --------

	go func() {
		_ = rc.CalculateRewardsForSnapshotDate("2024-08-02")
	}()
	time.Sleep(1 * time.Second)
	t.Logf("Attempting to calculate second rewards for snapshot date: 2024-08-02")
	err = rc.calculateRewardsForSnapshotDate("2024-08-02")
	assert.True(t, errors.Is(err, &ErrRewardsCalculationInProgress{}))

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbFileName, cfg, grm, l)
	})
}
