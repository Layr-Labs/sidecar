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
	"testing"
)

func setupOperatorShareSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbFileName, db, err := sqlite.GetFileBasedSqliteDatabaseConnection(l, "")
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return dbFileName, cfg, db, l, err
}

func teardownOperatorShareSnapshot(grm *gorm.DB) {
	queries := []string{
		`delete from operator_shares`,
		`delete from blocks`,
	}
	for _, query := range queries {
		if res := grm.Exec(query); res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
	}
}

func hydrateOperatorShares(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorSharesSqlFile(projectRoot)

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

func Test_OperatorShareSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, err := setupOperatorShareSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate, err := getSnapshotDate()
	if err != nil {
		t.Fatal(err)
	}

	startDate := "1970-01-01"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err = hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err = hydrateOperatorShares(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate operator share snapshots", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, grm, cfg)

		t.Log("Generating operator share snapshots")
		snapshots, err := rewards.GenerateOperatorShareSnapshots(startDate, snapshotDate)
		assert.Nil(t, err)

		t.Log("Loading expected results")
		expectedResults, err := tests.GetOperatorSharesExpectedResults(projectRoot)
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(snapshots))

		mappedExpectedResults := make(map[string]string)

		for _, expectedResult := range expectedResults {
			slotId := fmt.Sprintf("%s_%s_%s", expectedResult.Operator, expectedResult.Strategy, expectedResult.Snapshot)
			mappedExpectedResults[slotId] = expectedResult.Shares
		}

		if len(expectedResults) != len(snapshots) {
			t.Errorf("Expected %d snapshots, got %d", len(expectedResults), len(snapshots))

			lacksExpectedResult := make([]*OperatorShareSnapshots, 0)
			// Go line-by-line in the snapshot results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, snapshot := range snapshots {

				slotId := fmt.Sprintf("%s_%s_%s", snapshot.Operator, snapshot.Strategy, snapshot.Snapshot)

				found, ok := mappedExpectedResults[slotId]
				if !ok {
					t.Logf("Record not found %+v", snapshot)
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
					continue
				}
				if found != snapshot.Shares {
					// t.Logf("Expected: %s, Got: %s for %+v", found, snapshot.Shares, snapshot)
					lacksExpectedResult = append(lacksExpectedResult, snapshot)
				}
			}
			assert.Equal(t, 0, len(lacksExpectedResult))
			// if len(lacksExpectedResult) > 0 {
			// 	for i, window := range lacksExpectedResult {
			// 		fmt.Printf("%d - Snapshot: %+v\n", i, window)
			// 	}
			// }
		}
	})
	t.Cleanup(func() {
		teardownOperatorShareSnapshot(grm)
		tests.DeleteTestSqliteDB(dbFileName)
	})
}
