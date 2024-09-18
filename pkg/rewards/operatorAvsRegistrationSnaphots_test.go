package rewards

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
)

func setupOperatorAvsRegistrationSnapshot() (
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

func hydrateOperatorAvsStateChangesTable(grm *gorm.DB, l *zap.Logger) (int, error) {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorAvsRegistrationsSqlFile(projectRoot)

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

func Test_OperatorAvsRegistrationSnapshots(t *testing.T) {
	projectRoot := getProjectRootPath()
	cfg, grm, l, err := setupOperatorAvsRegistrationSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-09-01"

	t.Run("Should hydrate blocks and operatorAvsStateChanges tables", func(t *testing.T) {
		err := hydrateAllBlocksTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, TOTAL_BLOCK_COUNT, count)

		_, err = hydrateOperatorAvsStateChangesTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from avs_operator_state_changes"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 20442, count)
	})
	t.Run("Should generate the proper operatorAvsRegistrationWindows", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, nil, grm, cfg)

		snapshots, err := rewards.GenerateOperatorAvsRegistrationSnapshots(snapshotDate)
		assert.Nil(t, err)
		assert.NotNil(t, snapshots)

		t.Logf("Generated %d snapshots", len(snapshots))

		expectedResults, err := tests.GetExpectedOperatorAvsSnapshotResults(projectRoot)
		assert.Nil(t, err)

		t.Logf("Expected %d snapshots", len(expectedResults))
		assert.Equal(t, len(expectedResults), len(snapshots))

		lacksExpectedResult := make([]*OperatorAvsRegistrationSnapshots, 0)

		// If the two result sets are different lengths, we need to find out why.
		if len(expectedResults) != len(snapshots) {
			// Go line-by-line in the window results and find the corresponding line in the expected results.
			// If one doesnt exist, add it to the missing list.
			for _, window := range snapshots {
				match := utils.Find(expectedResults, func(expected *tests.ExpectedOperatorAvsRegistrationSnapshot) bool {
					if expected.Operator == window.Operator && expected.Avs == window.Avs && expected.Snapshot == window.Snapshot {
						return true
					}
					return false
				})
				if match == nil {
					lacksExpectedResult = append(lacksExpectedResult, window)
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
		teardownOperatorAvsRegistrationSnapshot(grm)
	})
}
