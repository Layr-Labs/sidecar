package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupCombinedRewards() (
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

func teardownCombinedRewards(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateRewardSubmissionsTable(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetCombinedRewardsSqlFile(projectRoot)

	if err != nil {
		return err
	}

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func Test_CombinedRewards(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupCombinedRewards()

	testContext := getRewardsTestContext()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should hydrate blocks and reward_submissions tables", func(t *testing.T) {
		totalBlockCount, err := hydrateAllBlocksTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, totalBlockCount, count)

		err = hydrateRewardSubmissionsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from reward_submissions"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		switch testContext {
		case "testnet":
			assert.Equal(t, 192, count)
		case "testnet-reduced":
			assert.Equal(t, 24, count)
		case "mainnet-reduced":
			assert.Equal(t, 16, count)
		default:
			t.Fatal("Unknown test context")
		}
	})
	t.Run("Should generate the proper combinedRewards", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		err = rewards.GenerateAndInsertCombinedRewards(snapshotDate)
		assert.Nil(t, err)

		combinedRewards, err := rewards.ListCombinedRewards()
		assert.Nil(t, err)

		assert.NotNil(t, combinedRewards)

		t.Logf("Generated %d combinedRewards", len(combinedRewards))

		switch testContext {
		case "testnet":
			assert.Equal(t, 192, len(combinedRewards))
		case "testnet-reduced":
			assert.Equal(t, 24, len(combinedRewards))
		case "mainnet-reduced":
			assert.Equal(t, 16, len(combinedRewards))
		default:
			t.Fatal("Unknown test context")
		}

	})
	t.Cleanup(func() {
		teardownCombinedRewards(dbFileName, cfg, grm, l)
	})
}
