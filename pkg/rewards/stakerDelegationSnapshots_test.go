package rewards

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"slices"
	"testing"
)

func setupStakerDelegationSnapshot() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	db, err := tests.GetSqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return cfg, db, l, err
}

func teardownStakerDelegationSnapshot(grm *gorm.DB) {
	queries := []string{
		`delete from staker_delegation_changes`,
		`delete from blocks`,
	}
	for _, query := range queries {
		if res := grm.Exec(query); res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
	}
}

func hydrateStakerDelegations(grm *gorm.DB, l *zap.Logger) (int, error) {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetStakerDelegationsSqlFile(projectRoot)

	if err != nil {
		return 0, err
	}

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return 0, res.Error
	}
	return len(contents), err
}

func Test_StakerDelegationSnapshots(t *testing.T) {
	projectRoot := getProjectRootPath()
	cfg, grm, l, err := setupStakerDelegationSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-09-01"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		if err := hydrateAllBlocksTable(grm, l); err != nil {
			t.Error(err)
		}
		if _, err := hydrateStakerDelegations(grm, l); err != nil {
			t.Error(err)
		}
	})
	t.Run("Should generate staker share snapshots", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, nil, grm, cfg)

		t.Log("Generating staker delegation snapshots")
		snapshots, err := rewards.GenerateStakerDelegationSnapshots(snapshotDate)
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
					if !slices.Contains(found, snapshot.Snapshot) {
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
		teardownStakerDelegationSnapshot(grm)
	})
}
