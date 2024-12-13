package rewards

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
	"time"
)

func setupStakerShareSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
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
		return "", nil, nil, nil, fmt.Errorf("Unknown test context")
	}

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, nil
}

func teardownStakerShareSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateStakerShares(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetStakerSharesSqlFile(projectRoot)

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

func Test_StakerShareSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, err := setupStakerShareSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err = hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err = hydrateStakerShares(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate staker share snapshots", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, l)

		t.Log("Generating staker share snapshots")
		err := rewards.GenerateAndInsertStakerShareSnapshots(snapshotDate)
		assert.Nil(t, err)

		snapshots, err := rewards.ListStakerShareSnapshots()
		assert.Nil(t, err)

		t.Log("Getting expected results")
		expectedResults, err := tests.GetStakerSharesSnapshotsExpectedResults(projectRoot)
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(snapshots))

		t.Log("Comparing results")
		mappedExpectedResults := make(map[string]string)
		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s_%s", expectedResult.Staker, expectedResult.Strategy, expectedResult.Snapshot)
			mappedExpectedResults[slotId] = expectedResult.Shares
		}

		if len(expectedResults) != len(snapshots) {
			t.Errorf("Expected %d snapshots, got %d", len(expectedResults), len(snapshots))

			lacksExpectedResult := make([]*StakerShareSnapshot, 0)
			// Go line-by-line in the snapshot results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, snapshot := range snapshots {
				slotId := fmt.Sprintf("%s_%s_%s", snapshot.Staker, snapshot.Strategy, snapshot.Snapshot.Format(time.DateOnly))

				found, ok := mappedExpectedResults[slotId]
				if !ok {
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
					continue
				}
				if found != snapshot.Shares {
					t.Logf("Record found, but shares dont match. Expected %s, got %+v", found, snapshot)
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
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
		teardownStakerShareSnapshot(dbFileName, cfg, grm, l)
	})
}
