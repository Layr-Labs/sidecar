package rewards

import (
	"fmt"
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

func setupOperatorAvsSplitWindows() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	testContext := getRewardsTestContext()
	cfg := tests.GetConfig()
	switch testContext {
	case "testnet":
		cfg.Chain = config.Chain_Holesky
	case "testnet-reduced":
		cfg.Chain = config.Chain_Holesky
	case "mainnet-reduced":
		cfg.Chain = config.Chain_Mainnet
	default:
		return "", nil, nil, nil, nil, fmt.Errorf("Unknown test context")
	}

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func teardownOperatorAvsSplitWindows(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateOperatorAvsSplits(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorAvsSplitsSqlFile(projectRoot)

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

func Test_OperatorAvsSplitSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	// projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupOperatorAvsSplitWindows()
	if err != nil {
		t.Fatal(err)
	}
	// testContext := getRewardsTestContext()

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		t.Log("Hydrating blocks")

		_, err := hydrateRewardsV2Blocks(grm, l)
		assert.Nil(t, err)

		t.Log("Hydrating restaked strategies")
		err = hydrateOperatorAvsSplits(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := `select count(*) from operator_avs_splits`
		var count int
		res := grm.Raw(query).Scan(&count)

		assert.Nil(t, res.Error)

		assert.Equal(t, 48, count)
	})

	t.Run("Should calculate correct operatorAvsSplit windows", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating snapshots")
		err := rewards.GenerateAndInsertOperatorAvsSplitSnapshots(snapshotDate)
		assert.Nil(t, err)

		windows, err := rewards.ListOperatorAvsSplitSnapshots()
		assert.Nil(t, err)

		t.Logf("Found %d windows", len(windows))

		assert.Equal(t, 192, len(windows))
	})
	t.Cleanup(func() {
		teardownOperatorAvsSplitWindows(dbFileName, cfg, grm, l)
	})
}
