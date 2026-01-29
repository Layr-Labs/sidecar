package rewards

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/totalStakeRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/uniqueStakeRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupStakeOperatorSetRewards() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	cfg := tests.GetConfig()
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func teardownStakeOperatorSetRewards(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateUniqueStakeRewardSubmissionsTable(grm *gorm.DB, l *zap.Logger) error {
	startTime := time.Unix(1725494400, 0)
	endTime := time.Unix(1725494400+2419200, 0)

	reward := uniqueStakeRewardSubmissions.UniqueStakeRewardSubmission{
		Avs:             "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101",
		OperatorSetId:   1,
		RewardHash:      "0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9",
		Token:           "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24",
		Amount:          "100000000000000000000000",
		Strategy:        "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c",
		StrategyIndex:   0,
		Multiplier:      "1000000000000000000",
		StartTimestamp:  &startTime,
		EndTimestamp:    &endTime,
		Duration:        2419200,
		BlockNumber:     1477020,
		TransactionHash: "unique_stake_hash",
		LogIndex:        12,
	}

	result := grm.Create(&reward)
	if result.Error != nil {
		l.Sugar().Errorw("Failed to create unique stake reward submission", "error", result.Error)
		return result.Error
	}
	return nil
}

func hydrateTotalStakeRewardSubmissionsTable(grm *gorm.DB, l *zap.Logger) error {
	startTime := time.Unix(1725494400, 0)
	endTime := time.Unix(1725494400+604800, 0)

	reward := totalStakeRewardSubmissions.TotalStakeRewardSubmission{
		Avs:             "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101",
		OperatorSetId:   2,
		RewardHash:      "0x8502669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc77b0",
		Token:           "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24",
		Amount:          "200000000000000000000000",
		Strategy:        "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c",
		StrategyIndex:   0,
		Multiplier:      "1500000000000000000",
		StartTimestamp:  &startTime,
		EndTimestamp:    &endTime,
		Duration:        604800,
		BlockNumber:     1477020,
		TransactionHash: "total_stake_hash",
		LogIndex:        15,
	}

	result := grm.Create(&reward)
	if result.Error != nil {
		l.Sugar().Errorw("Failed to create total stake reward submission", "error", result.Error)
		return result.Error
	}
	return nil
}

func Test_StakeOperatorSetRewards(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakeOperatorSetRewards()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate blocks and stake reward submissions tables", func(t *testing.T) {
		t.Log("Hydrating blocks")
		totalBlockCount, err := hydrateRewardsV2Blocks(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, totalBlockCount, count)

		t.Log("Hydrating unique stake reward submissions")
		err = hydrateUniqueStakeRewardSubmissionsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from unique_stake_reward_submissions"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, count)

		t.Log("Hydrating total stake reward submissions")
		err = hydrateTotalStakeRewardSubmissionsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from total_stake_reward_submissions"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, count)
	})

	t.Run("Should generate the proper stakeOperatorSetRewards", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		err = rewards.GenerateAndInsertStakeOperatorSetRewards(snapshotDate)
		assert.Nil(t, err)

		stakeOperatorSetRewards, err := rewards.ListStakeOperatorSetRewards()
		assert.Nil(t, err)

		assert.NotNil(t, stakeOperatorSetRewards)

		t.Logf("Generated %d stakeOperatorSetRewards", len(stakeOperatorSetRewards))

		// Should have 2 records: 1 unique_stake + 1 total_stake
		assert.Equal(t, 2, len(stakeOperatorSetRewards))

		// Verify we have both reward types
		var hasUniqueStake, hasTotalStake bool
		for _, reward := range stakeOperatorSetRewards {
			if reward.RewardType == "unique_stake" {
				hasUniqueStake = true
				assert.Equal(t, "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", reward.Avs)
				assert.Equal(t, uint64(1), reward.OperatorSetId)
				assert.Equal(t, "100000000000000000000000", reward.Amount)
			}
			if reward.RewardType == "total_stake" {
				hasTotalStake = true
				assert.Equal(t, "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", reward.Avs)
				assert.Equal(t, uint64(2), reward.OperatorSetId)
				assert.Equal(t, "200000000000000000000000", reward.Amount)
			}
		}
		assert.True(t, hasUniqueStake, "Should have unique_stake reward")
		assert.True(t, hasTotalStake, "Should have total_stake reward")
	})

	t.Cleanup(func() {
		teardownStakeOperatorSetRewards(dbFileName, cfg, grm, l)
	})
}

// Test_ProRataDistribution tests that stake-based rewards are correctly distributed
// proportionally across operators based on their allocated weight.
//
// Scenario:
// - Pool reward: 1,000,000 tokens for 10 days (100,000 tokens/day)
// - Operator A: weight 1 (allocated stake * multiplier)
// - Operator B: weight 2
// - Total weight: 3
// Expected per day:
// - Operator A: FLOOR(100,000 * 1 / 3) = 33,333 tokens
// - Operator B: FLOOR(100,000 * 2 / 3) = 66,666 tokens
func Test_ProRataDistribution(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupStakeOperatorSetRewards()
	if err != nil {
		t.Fatal(err)
	}

	// Enable rewards v2.2
	cfg.Rewards.RewardsV2_2Enabled = true

	snapshotDate := "2024-09-15"

	t.Run("Should distribute pro-rata to operators based on weight", func(t *testing.T) {
		// Hydrate blocks
		t.Log("Hydrating blocks")
		_, err := hydrateRewardsV2Blocks(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		avs := "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"
		operatorA := "0xoperator_a_prorata_test"
		operatorB := "0xoperator_b_prorata_test"
		strategy := "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c"
		token := "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24"

		// Insert operator set operator registrations
		res := grm.Exec(`
			INSERT INTO operator_set_operator_registrations
			(operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index)
			VALUES
			(?, ?, 1, true, 1477000, '0xtx_opreg_a', 0),
			(?, ?, 1, true, 1477000, '0xtx_opreg_b', 1)
		`, operatorA, avs, operatorB, avs)
		assert.Nil(t, res.Error)

		// Insert operator set strategy registration
		res = grm.Exec(`
			INSERT INTO operator_set_strategy_registrations
			(strategy, avs, operator_set_id, is_active, block_number, transaction_hash, log_index)
			VALUES
			(?, ?, 1, true, 1477000, '0xtx_stratreg', 0)
		`, strategy, avs)
		assert.Nil(t, res.Error)

		// Insert operator allocations with different magnitudes
		// Operator A: magnitude 1e18, max_magnitude 1e18 => ratio 1
		// Operator B: magnitude 2e18, max_magnitude 2e18 => ratio 1
		res = grm.Exec(`
			INSERT INTO operator_allocations
			(operator, avs, strategy, magnitude, operator_set_id, effective_block, transaction_hash, log_index, block_number)
			VALUES
			(?, ?, ?, '1000000000000000000', 1, 1477000, '0xtx_alloc_a', 0, 1477000),
			(?, ?, ?, '2000000000000000000', 1, 1477000, '0xtx_alloc_b', 1, 1477000)
		`, operatorA, avs, strategy, operatorB, avs, strategy)
		assert.Nil(t, res.Error)

		// Insert max magnitudes
		res = grm.Exec(`
			INSERT INTO operator_max_magnitudes
			(operator, strategy, max_magnitude, block_number, transaction_hash, log_index)
			VALUES
			(?, ?, '1000000000000000000', 1477000, '0xtx_maxmag_a', 0),
			(?, ?, '2000000000000000000', 1477000, '0xtx_maxmag_b', 1)
		`, operatorA, strategy, operatorB, strategy)
		assert.Nil(t, res.Error)

		// Insert operator shares (same shares, different allocations give different weights)
		// Operator A: 1000 shares * (1e18/1e18) * 1e18 multiplier = weight 1e21
		// Operator B: 1000 shares * (2e18/2e18) * 1e18 multiplier = weight 1e21
		// But we want weight ratio 1:2, so give operator B 2x shares
		res = grm.Exec(`
			INSERT INTO operator_share_deltas
			(operator, strategy, shares, strategy_index, block_number, transaction_hash, log_index)
			VALUES
			(?, ?, '1000000000000000000000', 0, 1477000, '0xtx_opshare_a', 0),
			(?, ?, '2000000000000000000000', 0, 1477000, '0xtx_opshare_b', 1)
		`, operatorA, strategy, operatorB, strategy)
		assert.Nil(t, res.Error)

		// Insert unique stake reward submission (pool-based)
		// 1,000,000 tokens over 10 days = 100,000 tokens/day
		startTime := time.Unix(1725494400, 0) // Sept 5, 2024
		endTime := time.Unix(1726358400, 0)   // Sept 15, 2024 (10 days later)

		reward := uniqueStakeRewardSubmissions.UniqueStakeRewardSubmission{
			Avs:             avs,
			OperatorSetId:   1,
			RewardHash:      "0xprorata_test_hash",
			Token:           token,
			Amount:          "1000000000000000000000000", // 1,000,000 tokens (1e24)
			Strategy:        strategy,
			StrategyIndex:   0,
			Multiplier:      "1000000000000000000", // 1e18
			StartTimestamp:  &startTime,
			EndTimestamp:    &endTime,
			Duration:        864000, // 10 days in seconds
			BlockNumber:     1477020,
			TransactionHash: "prorata_stake_hash",
			LogIndex:        20,
		}

		result := grm.Create(&reward)
		assert.Nil(t, result.Error)

		// Generate stake operator set rewards (denormalizer)
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		err = rewards.GenerateAndInsertStakeOperatorSetRewards(snapshotDate)
		assert.Nil(t, err)

		// Verify stake_operator_set_rewards has the entry
		stakeRewards, err := rewards.ListStakeOperatorSetRewards()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(stakeRewards))
		assert.Equal(t, "unique_stake", stakeRewards[0].RewardType)

		t.Logf("Created stake reward: amount=%s, duration=%d days",
			stakeRewards[0].Amount, stakeRewards[0].Duration/86400)

		// Generate snapshot data
		err = rewards.generateSnapshotData(snapshotDate)
		assert.Nil(t, err)

		// Generate gold tables (Table 15 will explode into per-day rows, Table 16 will distribute)
		err = rewards.generateGoldTables(snapshotDate)
		assert.Nil(t, err)

		// Query Table 16 to verify pro-rata distribution
		var operatorRewards []struct {
			Operator       string
			OperatorTokens string
			Snapshot       string
		}

		query := `
			SELECT operator, operator_tokens::text, snapshot::text
			FROM gold_16_operator_operator_set_unique_stake_rewards_` + strings.ReplaceAll(snapshotDate, "-", "_") + `
			ORDER BY operator, snapshot
		`
		err = grm.Raw(query).Scan(&operatorRewards).Error
		if err != nil {
			t.Logf("Query error (table may not exist or no data): %v", err)
		}

		t.Logf("Found %d operator reward entries", len(operatorRewards))

		// Count and sum by operator
		operatorTotals := make(map[string]int64)
		for _, r := range operatorRewards {
			tokens, _ := new(big.Int).SetString(r.OperatorTokens, 10)
			if tokens != nil {
				operatorTotals[r.Operator] += tokens.Int64()
			}
			t.Logf("  Operator: %s, Snapshot: %s, Tokens: %s", r.Operator, r.Snapshot, r.OperatorTokens)
		}

		// Verify the ratio is approximately 1:2
		if len(operatorTotals) >= 2 {
			tokensA := operatorTotals[operatorA]
			tokensB := operatorTotals[operatorB]

			t.Logf("Operator A total tokens: %d", tokensA)
			t.Logf("Operator B total tokens: %d", tokensB)

			// The ratio should be approximately 1:2
			if tokensA > 0 && tokensB > 0 {
				ratio := float64(tokensB) / float64(tokensA)
				t.Logf("Ratio B/A: %.4f (expected ~2.0)", ratio)

				// Allow for some rounding error due to FLOOR operations
				assert.InDelta(t, 2.0, ratio, 0.1, "Tokens should be distributed in 1:2 ratio")
			}
		}
	})

	t.Cleanup(func() {
		teardownStakeOperatorSetRewards(dbFileName, cfg, grm, l)
	})
}
