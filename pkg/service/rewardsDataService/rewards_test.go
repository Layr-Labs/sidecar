package rewardsDataService

import (
	"context"
	"fmt"
	"os"
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
