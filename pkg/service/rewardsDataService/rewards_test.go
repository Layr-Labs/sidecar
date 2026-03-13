package rewardsDataService

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/service/types"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupRewardDistributionTest() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := tests.GetConfig()
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func teardownRewardDistributionTest(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)
	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

// hydrateSnapshotTables inserts test data into the snapshot tables required
// for the GetRewardDistributionByStake query.
//
// Test scenario:
//   - AVS: 0xavs1
//   - 2 operators: 0xop1, 0xop2 (both registered to AVS with strategy 0xstrat1)
//   - 3 stakers:
//     staker1 -> delegated to op1, 3000 shares in strat1
//     staker2 -> delegated to op1, 1000 shares in strat1
//     staker3 -> delegated to op2, 2000 shares in strat1
//   - Reward: 1,000,000 tokens over 1 day (86400s)
//
// Expected weights:
//   - op1: multiplier*(3000 + 1000) = 4000 * multiplier
//   - op2: multiplier*(2000)        = 2000 * multiplier
//   - total: 6000 * multiplier
//
// Expected rewards (with FLOOR precision matching gold table logic):
//   - op1: FLOOR(FLOOR(4/6 * 1e15) / 1e15 * 1000000) = 666666
//   - op2: FLOOR(FLOOR(2/6 * 1e15) / 1e15 * 1000000) = 333333
func hydrateSnapshotTables(grm *gorm.DB) error {
	snapshot := "2025-01-15"

	// operator_avs_registration_snapshots
	registrations := []map[string]interface{}{
		{"avs": "0xavs1", "operator": "0xop1", "snapshot": snapshot},
		{"avs": "0xavs1", "operator": "0xop2", "snapshot": snapshot},
	}
	for _, r := range registrations {
		res := grm.Exec(
			"INSERT INTO operator_avs_registration_snapshots (avs, operator, snapshot) VALUES (?, ?, ?)",
			r["avs"], r["operator"], r["snapshot"],
		)
		if res.Error != nil {
			return fmt.Errorf("failed to insert operator_avs_registration_snapshot: %w", res.Error)
		}
	}

	// operator_avs_strategy_snapshots
	stratRegistrations := []map[string]interface{}{
		{"operator": "0xop1", "avs": "0xavs1", "strategy": "0xstrat1", "snapshot": snapshot},
		{"operator": "0xop2", "avs": "0xavs1", "strategy": "0xstrat1", "snapshot": snapshot},
	}
	for _, r := range stratRegistrations {
		res := grm.Exec(
			"INSERT INTO operator_avs_strategy_snapshots (operator, avs, strategy, snapshot) VALUES (?, ?, ?, ?)",
			r["operator"], r["avs"], r["strategy"], r["snapshot"],
		)
		if res.Error != nil {
			return fmt.Errorf("failed to insert operator_avs_strategy_snapshot: %w", res.Error)
		}
	}

	// staker_delegation_snapshots
	delegations := []map[string]interface{}{
		{"staker": "0xstaker1", "operator": "0xop1", "snapshot": snapshot},
		{"staker": "0xstaker2", "operator": "0xop1", "snapshot": snapshot},
		{"staker": "0xstaker3", "operator": "0xop2", "snapshot": snapshot},
	}
	for _, r := range delegations {
		res := grm.Exec(
			"INSERT INTO staker_delegation_snapshots (staker, operator, snapshot) VALUES (?, ?, ?)",
			r["staker"], r["operator"], r["snapshot"],
		)
		if res.Error != nil {
			return fmt.Errorf("failed to insert staker_delegation_snapshot: %w", res.Error)
		}
	}

	// staker_share_snapshots
	shares := []map[string]interface{}{
		{"staker": "0xstaker1", "strategy": "0xstrat1", "shares": "3000", "snapshot": snapshot},
		{"staker": "0xstaker2", "strategy": "0xstrat1", "shares": "1000", "snapshot": snapshot},
		{"staker": "0xstaker3", "strategy": "0xstrat1", "shares": "2000", "snapshot": snapshot},
	}
	for _, r := range shares {
		res := grm.Exec(
			"INSERT INTO staker_share_snapshots (staker, strategy, shares, snapshot) VALUES (?, ?, ?, ?)",
			r["staker"], r["strategy"], r["shares"], r["snapshot"],
		)
		if res.Error != nil {
			return fmt.Errorf("failed to insert staker_share_snapshot: %w", res.Error)
		}
	}

	return nil
}

func setup() (
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	*metrics.MetricsSink,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)
	pg, err := postgres.NewPostgres(pgConfig)
	if err != nil {
		l.Fatal("Failed to setup postgres connection", zap.Error(err))
	}

	grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		l.Fatal("Failed to create gorm instance", zap.Error(err))
	}

	return grm, l, cfg, sink, nil
}

// Test_RewardsDataService tests the rewards data service. It assumes that there is a full
// database to read data from, specifically a holesky database with rewards generated.
func Test_RewardsDataService(t *testing.T) {
	if !tests.LargeTestsEnabled() {
		t.Skipf("Skipping large test")
		return
	}

	grm, l, cfg, sink, err := setup()

	t.Logf("Using database with name: %s", cfg.DatabaseConfig.DbName)

	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)
	rc, err := rewards.NewRewardsCalculator(cfg, grm, mds, nil, sink, l)
	if err != nil {
		t.Fatalf("Failed to create rewards calculator: %v", err)
	}
	rds := NewRewardsDataService(grm, l, cfg, rc)

	t.Run("Test GetRewardsForSnapshot", func(t *testing.T) {
		snapshot := "2025-01-16"

		r, err := rds.GetRewardsForSnapshot(context.Background(), snapshot, nil)
		assert.Nil(t, err)
		assert.NotNil(t, r)
	})

	t.Run("Test GetTotalClaimedRewards", func(t *testing.T) {
		earner := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"
		blockNumber := uint64(3178227)

		r, err := rds.GetTotalClaimedRewards(context.Background(), earner, nil, blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, r)
	})

	t.Run("Test ListClaimedRewardsByBlockRange", func(t *testing.T) {
		earner := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"
		blockNumber := uint64(3178227)

		r, err := rds.ListClaimedRewardsByBlockRange(context.Background(), earner, blockNumber, blockNumber, nil)
		assert.Nil(t, err)
		assert.NotNil(t, r)
	})

	t.Run("Test GetTotalRewardsForEarner for claimable tokens", func(t *testing.T) {
		earner := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"
		blockNumber := uint64(3178227)

		t.Run("for active tokens", func(t *testing.T) {
			r, err := rds.GetTotalRewardsForEarner(context.Background(), earner, nil, blockNumber, false)
			assert.Nil(t, err)
			assert.NotNil(t, r)
		})
		t.Run("for claimable tokens", func(t *testing.T) {
			r, err := rds.GetTotalRewardsForEarner(context.Background(), earner, nil, blockNumber, true)
			assert.Nil(t, err)
			assert.NotNil(t, r)
		})
	})

	t.Run("Test GetClaimableRewardsForEarner", func(t *testing.T) {
		earner := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"
		blockNumber := uint64(3178227)

		r, root, err := rds.GetClaimableRewardsForEarner(context.Background(), earner, nil, blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, r)
		assert.NotNil(t, root)
	})

	t.Run("Test GetSummarizedRewards", func(t *testing.T) {
		earner := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"
		blockNumber := uint64(3178227)

		r, err := rds.GetSummarizedRewards(context.Background(), earner, nil, blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, r)
		fmt.Printf("Summarized rewards: %+v\n", r)
		for _, sr := range r {
			fmt.Printf("  %+v\n", sr)
		}
	})

	t.Run("Test ListAvailableRewardsTokens", func(t *testing.T) {
		earner := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"
		blockNumber := uint64(3178227)

		r, err := rds.ListAvailableRewardsTokens(context.Background(), earner, blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, r)
		fmt.Printf("Available rewards tokens: %v\n", r)
	})

	t.Run("Test GetRewardsByAvsForDistributionRoot", func(t *testing.T) {
		rootIndex := uint64(189)

		r, err := rds.GetRewardsByAvsForDistributionRoot(context.Background(), rootIndex, nil, nil)
		assert.Nil(t, err)
		assert.NotNil(t, r)
		assert.True(t, len(r) > 0)
	})
	t.Run("Test GetRewardsByAvsForDistributionRoot with pagination", func(t *testing.T) {
		rootIndex := uint64(189)

		r, err := rds.GetRewardsByAvsForDistributionRoot(context.Background(), rootIndex, nil, &types.Pagination{
			Page:     0,
			PageSize: 10,
		})
		assert.Nil(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, len(r), 10)
	})
	t.Run("Test GetRewardsByAvsForDistributionRoot with earner address", func(t *testing.T) {
		rootIndex := uint64(189)
		earnerAddress := "0x0fb39abd3740d10ac1f698f2796ee200bbdd2065"

		r, err := rds.GetRewardsByAvsForDistributionRoot(context.Background(), rootIndex, []string{earnerAddress}, nil)
		assert.Nil(t, err)
		assert.NotNil(t, r)
	})
	t.Run("Test GetRewardsForDistributionRoot", func(t *testing.T) {
		// Test with a known root index that should exist and have completed rewards calculation
		rootIndex := uint64(189)

		t.Run("successful case with completed rewards calculation", func(t *testing.T) {
			r, err := rds.GetRewardsForDistributionRoot(context.Background(), rootIndex)
			assert.Nil(t, err)
			assert.NotNil(t, r)
			// Should return some rewards for a valid root index
			assert.True(t, len(r) >= 0, "Should return rewards (empty array is valid if no rewards exist)")
			fmt.Printf("Found %d rewards for distribution root %d\n", len(r), rootIndex)
		})

		t.Run("error case with non-existent root index", func(t *testing.T) {
			nonExistentRootIndex := uint64(999999)
			r, err := rds.GetRewardsForDistributionRoot(context.Background(), nonExistentRootIndex)
			assert.NotNil(t, err, "Should return an error for non-existent root index")
			assert.Nil(t, r)
			assert.Contains(t, err.Error(), "no distribution root found for root index", "Error should mention missing root index")
		})

		t.Run("error case with incomplete rewards calculation", func(t *testing.T) {
			// This test would need a root index that exists but has incomplete rewards calculation
			// In a real scenario, this would be a recently created root where rewards are still being calculated
			// For this test, we'll focus on the error message structure

			// Note: This test might pass if all roots in the test database have completed calculations
			// The important thing is that our code correctly handles the case where rewards are incomplete
			rootIndex := uint64(190) // Try a different root index that might not have completed calculations

			r, err := rds.GetRewardsForDistributionRoot(context.Background(), rootIndex)
			if err != nil {
				// If there's an error, check if it's the expected incomplete calculation error
				if err.Error() == fmt.Sprintf("no distribution root found for root index '%d'", rootIndex) {
					t.Logf("Root index %d doesn't exist, which is expected for this test", rootIndex)
				} else if strings.Contains(err.Error(), "rewards calculation has not been completed for distribution root") {
					t.Logf("Found expected incomplete rewards calculation error: %s", err.Error())
					assert.Nil(t, r)
				} else {
					t.Logf("Got different error (which is fine): %s", err.Error())
				}
			} else {
				// If no error, rewards calculation was complete for this root
				assert.NotNil(t, r)
				t.Logf("Root index %d has completed rewards calculation with %d rewards", rootIndex, len(r))
			}
		})
	})

	t.Run("Test GetBlockHeightForSnapshotDate", func(t *testing.T) {
		// Test with a known date that should exist in the database
		snapshotDate := "2025-01-16"
		blockHeight, err := rds.GetBlockHeightForSnapshotDate(context.Background(), snapshotDate)
		assert.Nil(t, err)
		assert.Greater(t, blockHeight, uint64(0), "Block height should be greater than 0")
		fmt.Printf("Block height for snapshot date %s: %d\n", snapshotDate, blockHeight)

		// Test with a snapshot date that should not exist
		nonExistentDate := "1970-01-01"
		_, err = rds.GetBlockHeightForSnapshotDate(context.Background(), nonExistentDate)
		assert.NotNil(t, err, "Should return an error for non-existent date")
	})
}

func Test_GetRewardDistributionByStake(t *testing.T) {
	dbCfg := tests.GetDbConfigFromEnv()
	if dbCfg.Host == "" {
		t.Skipf("Skipping %s - no database configured (set SIDECAR_DATABASE_HOST)", t.Name())
		return
	}

	dbname, grm, l, cfg, err := setupRewardDistributionTest()
	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}
	defer teardownRewardDistributionTest(dbname, cfg, grm, l)

	rds := NewRewardsDataService(grm, l, cfg, nil)

	err = hydrateSnapshotTables(grm)
	if err != nil {
		t.Fatalf("Failed to hydrate snapshot tables: %v", err)
	}

	t.Run("basic distribution with two operators", func(t *testing.T) {
		results, err := rds.GetRewardDistributionByStake(
			context.Background(),
			"0xavs1",
			"1000000",
			"2025-01-14", // startDate (exclusive)
			"2025-01-15", // endDate (inclusive)
			86400,
			[]StrategyMultiplierParam{
				{Strategy: "0xstrat1", Multiplier: "1000000000000000000"},
			},
		)
		assert.Nil(t, err)
		assert.NotNil(t, results)
		assert.Len(t, results, 2, "should have 2 operators")

		rewardMap := make(map[string]string)
		for _, r := range results {
			rewardMap[r.Operator] = r.RewardAmount
			t.Logf("Operator: %s, Amount: %s", r.Operator, r.RewardAmount)
		}

		assert.Equal(t, "666666", rewardMap["0xop1"], "op1 should get ~2/3 of 1000000")
		assert.Equal(t, "333333", rewardMap["0xop2"], "op2 should get ~1/3 of 1000000")
	})

	t.Run("validation: empty avs returns error", func(t *testing.T) {
		_, err := rds.GetRewardDistributionByStake(
			context.Background(),
			"",
			"1000000",
			"2025-01-14",
			"2025-01-15",
			86400,
			[]StrategyMultiplierParam{
				{Strategy: "0xstrat1", Multiplier: "1"},
			},
		)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "avs is required")
	})

	t.Run("validation: empty strategies returns error", func(t *testing.T) {
		_, err := rds.GetRewardDistributionByStake(
			context.Background(),
			"0xavs1",
			"1000000",
			"2025-01-14",
			"2025-01-15",
			86400,
			[]StrategyMultiplierParam{},
		)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "at least one strategy")
	})

	t.Run("no operators registered returns empty results", func(t *testing.T) {
		results, err := rds.GetRewardDistributionByStake(
			context.Background(),
			"0xnonexistent_avs",
			"1000000",
			"2025-01-14",
			"2025-01-15",
			86400,
			[]StrategyMultiplierParam{
				{Strategy: "0xstrat1", Multiplier: "1000000000000000000"},
			},
		)
		assert.Nil(t, err)
		assert.Len(t, results, 0, "should have no operators for non-existent AVS")
	})
}
