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

func setupStakerShareSnapshot() (
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

func teardownStakerShareSnapshot(grm *gorm.DB) {
	queries := []string{
		`delete from staker_shares`,
		`delete from blocks`,
	}
	for _, query := range queries {
		if res := grm.Exec(query); res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
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
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error), zap.String("query", contents))
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

	startDate := "1970-01-01"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if _, err = hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if err = hydrateStakerShares(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate staker share snapshots", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, grm, cfg)

		t.Log("Generating staker share snapshots")
		snapshots, err := rewards.GenerateStakerShareSnapshots(startDate, snapshotDate)
		assert.Nil(t, err)

		t.Log("Getting expected results")
		expectedResults, err := tests.GetStakerSharesExpectedResults(projectRoot)
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
				slotId := fmt.Sprintf("%s_%s_%s", snapshot.Staker, snapshot.Strategy, snapshot.Snapshot)

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
		teardownStakerShareSnapshot(grm)
		tests.DeleteTestSqliteDB(dbFileName)
	})
}
