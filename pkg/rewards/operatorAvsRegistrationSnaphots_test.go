package rewards

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/internal/tests/sqlite"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"slices"
	"testing"
)

func setupOperatorAvsRegistrationSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbFileName, db, err := sqlite.GetFileBasedSqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return dbFileName, cfg, db, l, err
}

func teardownOperatorAvsRegistrationSnapshot(grm *gorm.DB) {
	queries := []string{
		`delete from avs_operator_state_changes`,
		`delete from blocks`,
	}
	for _, query := range queries {
		if res := grm.Exec(query); res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
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
			assert.Equal(t, 1922, count)
		default:
			t.Fatal("Unknown test context")
		}
	})
	t.Run("Should generate the proper operatorAvsRegistrationWindows", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, grm, cfg)

		snapshots, err := rewards.GenerateOperatorAvsRegistrationSnapshots(snapshotDate)
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
					if !slices.Contains(found, window.Snapshot) {
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
		tests.DeleteTestSqliteDB(dbFileName)
		teardownOperatorAvsRegistrationSnapshot(grm)
	})
}
