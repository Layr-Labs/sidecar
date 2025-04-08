package rewards

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"slices"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupStakerDelegationSnapshot() (
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

func teardownStakerDelegationSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateStakerDelegations(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetStakerDelegationsSqlFile(projectRoot)

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

func Test_StakerDelegationSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupStakerDelegationSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err := hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err := hydrateStakerDelegations(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate staker share snapshots", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating staker delegation snapshots")
		err = rewards.GenerateAndInsertStakerDelegationSnapshots(snapshotDate)
		assert.Nil(t, err)

		snapshots, err := rewards.ListStakerDelegationSnapshots()
		assert.Nil(t, err)

		t.Log("Getting expected results")
		expectedResults, err := tests.GetStakerDelegationExpectedResults(projectRoot)
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(snapshots))

		mappedExpectedResults := make(map[string][]string)
		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s", expectedResult.Staker, expectedResult.Operator)
			if _, ok := mappedExpectedResults[slotId]; !ok {
				mappedExpectedResults[slotId] = make([]string, 0)
			}
			mappedExpectedResults[slotId] = append(mappedExpectedResults[slotId], expectedResult.Snapshot)
		}

		if len(expectedResults) != len(snapshots) {
			t.Errorf("Expected %d snapshots, got %d", len(expectedResults), len(snapshots))

			lacksExpectedResult := make([]*StakerDelegationSnapshot, 0)
			// Go line-by-line in the snapshot results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, snapshot := range snapshots {
				slotId := fmt.Sprintf("%s_%s", snapshot.Staker, snapshot.Operator)
				found, ok := mappedExpectedResults[slotId]
				if !ok {
					t.Logf("Staker/operator not found in results: %+v\n", snapshot)
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
				} else {
					if !slices.Contains(found, snapshot.Snapshot.Format(time.DateOnly)) {
						t.Logf("Found staker operator, but no snapshot: %+v - %+v\n", snapshot, found)
						lacksExpectedResult = append(lacksExpectedResult, snapshot)
					}
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
		teardownStakerDelegationSnapshot(dbFileName, cfg, grm, l)
	})
}
