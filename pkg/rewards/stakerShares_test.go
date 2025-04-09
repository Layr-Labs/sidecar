package rewards

import (
	"fmt"
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

func setupStakerShares() (
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

func hydrateStakerShareDeltas(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetStakerShareDeltasSqlFile(projectRoot)

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

func Test_StakerShares(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupStakerShares()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-08-20"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err = hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err = hydrateStakerShareDeltas(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate staker shares", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating staker shares")
		err := rewards.GenerateAndInsertStakerShares(snapshotDate)
		assert.Nil(t, err)

		t.Log("Generating operator shares")
		err = rewards.GenerateAndInsertOperatorShares(snapshotDate)
		assert.Nil(t, err)

		stakerShares, err := rewards.ListStakerShares()
		assert.Nil(t, err)

		t.Log("Getting expected results")
		expectedResults, err := tests.GetStakerSharesExpectedResults(projectRoot)
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(stakerShares))

		t.Log("Comparing results")
		mappedExpectedResults := make(map[string]string)
		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s_%d_%d", expectedResult.Staker, expectedResult.Strategy, expectedResult.BlockNumber, expectedResult.LogIndex)
			mappedExpectedResults[slotId] = expectedResult.Shares
		}

		if len(expectedResults) != len(stakerShares) {
			t.Errorf("Expected %d stakerShares, got %d", len(expectedResults), len(stakerShares))

			lacksExpectedResult := make([]*StakerShares, 0)

			for _, stakerShare := range stakerShares {
				slotId := fmt.Sprintf("%s_%s_%d_%d", stakerShare.Staker, stakerShare.Strategy, stakerShare.BlockNumber, stakerShare.LogIndex)

				found, ok := mappedExpectedResults[slotId]
				if !ok {
					lacksExpectedResult = append(lacksExpectedResult, stakerShare)
					continue
				}
				if found != stakerShare.Shares {
					t.Logf("Record found, but shares dont match. Expected %s, got %+v", found, stakerShare)
					lacksExpectedResult = append(lacksExpectedResult, stakerShare)
				}
			}
			assert.Equal(t, 0, len(lacksExpectedResult))

			if len(lacksExpectedResult) > 0 {
				for i, window := range lacksExpectedResult {
					fmt.Printf("%d - Snapshot: %+v\n", i, window)
				}
			}
		}
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbFileName, cfg, grm, l)
	})
}
