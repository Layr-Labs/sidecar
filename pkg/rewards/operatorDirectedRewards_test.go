package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
)

func setupOperatorDirectedRewards() (
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

func teardownOperatorDirectedRewards(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateOperatorDirectedRewardSubmissionsTable(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorDirectedRewardsSqlFile(projectRoot)

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

func Test_OperatorDirectedRewards(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorDirectedRewards()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate blocks and operator_directed_reward_submissions tables", func(t *testing.T) {
		totalBlockCount, err := hydrateRewardsV2Blocks(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, totalBlockCount, count)

		err = hydrateOperatorDirectedRewardSubmissionsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from operator_directed_reward_submissions"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1120, count)
	})
	t.Run("Should generate the proper operatorDirectedRewards", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		err = rewards.GenerateAndInsertOperatorDirectedRewards(snapshotDate)
		assert.Nil(t, err)

		operatorDirectedRewards, err := rewards.ListOperatorDirectedRewards()
		assert.Nil(t, err)

		assert.NotNil(t, operatorDirectedRewards)

		t.Logf("Generated %d operatorDirectedRewards", len(operatorDirectedRewards))

		assert.Equal(t, 1120, len(operatorDirectedRewards))

	})
	t.Cleanup(func() {
		teardownOperatorDirectedRewards(dbFileName, cfg, grm, l)
	})
}
