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
	"slices"
	"testing"
	"time"
)

func setupOperatorAvsRegistrationSnapshot() (
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

func teardownOperatorAvsRegistrationSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateOperatorAvsStateChangesTable(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorAvsRegistrationsSqlFile(projectRoot)

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

func Test_OperatorAvsRegistrationSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, err := setupOperatorAvsRegistrationSnapshot()
	testContext := getRewardsTestContext()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should hydrate blocks and operatorAvsStateChanges tables", func(t *testing.T) {
		totalBlockCount, err := hydrateAllBlocksTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, totalBlockCount, count)

		err = hydrateOperatorAvsStateChangesTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from avs_operator_state_changes"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		switch testContext {
		case "testnet":
			assert.Equal(t, 20442, count)
		case "testnet-reduced":
			assert.Equal(t, 16042, count)
		case "mainnet-reduced":
			assert.Equal(t, 1752, count)
		default:
			t.Fatal("Unknown test context")
		}
	})
	t.Run("Should generate the proper operatorAvsRegistrationWindows", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, l)

		err := rewards.GenerateAndInsertOperatorAvsRegistrationSnapshots(snapshotDate)
		assert.Nil(t, err)

		snapshots, err := rewards.ListOperatorAvsRegistrationSnapshots()
		assert.Nil(t, err)
		assert.NotNil(t, snapshots)

		t.Logf("Generated %d snapshots", len(snapshots))

		expectedResults, err := tests.GetExpectedOperatorAvsSnapshotResults(projectRoot)
		assert.Nil(t, err)

		t.Logf("Expected %d snapshots", len(expectedResults))
		assert.Equal(t, len(expectedResults), len(snapshots))

		lacksExpectedResult := make([]*OperatorAvsRegistrationSnapshots, 0)

		mappedExpectedResults := make(map[string][]string)
		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s", expectedResult.Operator, expectedResult.Avs)
			if _, ok := mappedExpectedResults[slotId]; !ok {
				mappedExpectedResults[slotId] = make([]string, 0)
			}
			mappedExpectedResults[slotId] = append(mappedExpectedResults[slotId], expectedResult.Snapshot)
		}

		// If the two result sets are different lengths, we need to find out why.
		if len(expectedResults) != len(snapshots) {
			// Go line-by-line in the window results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, window := range snapshots {
				slotId := fmt.Sprintf("%s_%s", window.Operator, window.Avs)
				found, ok := mappedExpectedResults[slotId]
				if !ok {
					t.Logf("Operator/AVS not found in results: %+v\n", window)
					lacksExpectedResult = append(lacksExpectedResult, window)
				} else {
					if !slices.Contains(found, window.Snapshot.Format(time.DateOnly)) {
						t.Logf("Found operator/AVS, but no snapshot: %+v - %+v\n", window, found)
						lacksExpectedResult = append(lacksExpectedResult, window)
					}
				}
			}
			assert.Equal(t, 0, len(lacksExpectedResult))

			if len(lacksExpectedResult) > 0 {
				for i, window := range lacksExpectedResult {
					fmt.Printf("%d - Window: %+v\n", i, window)
				}
			}
		}
	})
	t.Cleanup(func() {
		teardownOperatorAvsRegistrationSnapshot(dbFileName, cfg, grm, l)
	})
}
