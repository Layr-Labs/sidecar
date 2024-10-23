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
	"strings"
	"testing"
)

func setupOperatorAvsStrategyWindows() (
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

func teardownOperatorAvsStrategyWindows(grm *gorm.DB) {
	queries := []string{
		`delete from operator_avs_strategy_snapshots`,
	}
	for _, query := range queries {
		grm.Exec(query)
	}
}

func hydrateOperatorAvsRestakedStrategies(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetOperatorAvsRestakedStrategiesSqlFile(projectRoot)

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

func Test_OperatorAvsStrategySnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, err := setupOperatorAvsStrategyWindows()
	if err != nil {
		t.Fatal(err)
	}
	testContext := getRewardsTestContext()

	snapshotDate, err := getSnapshotDate()
	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal(err)
	}

	startDate := "1970-01-01"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		t.Log("Hydrating restaked strategies")
		err := hydrateOperatorAvsRestakedStrategies(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := `select count(*) from operator_restaked_strategies`
		var count int
		res := grm.Raw(query).Scan(&count)

		assert.Nil(t, res.Error)

		switch testContext {
		case "testnet":
			assert.Equal(t, 3144978, count)
		case "testnet-reduced":
			assert.Equal(t, 1591921, count)
		case "mainnet-reduced":
			assert.Equal(t, 2497286, count)
		default:
			t.Fatal("Unknown test context")
		}
	})

	t.Run("Should calculate correct operatorAvsStrategy windows", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, grm, cfg)

		t.Log("Generating snapshots")
		windows, err := rewards.GenerateOperatorAvsStrategySnapshots(startDate, snapshotDate)
		assert.Nil(t, err)

		t.Log("Getting expected results")
		expectedResults, err := tests.GetExpectedOperatorAvsSnapshots(projectRoot)
		assert.Nil(t, err)
		t.Logf("Loaded %d expected results", len(expectedResults))

		assert.Equal(t, len(expectedResults), len(windows))

		// Memoize to make lookups faster rather than n^2
		mappedExpectedResults := make(map[string][]string)
		for _, r := range expectedResults {
			slotId := strings.ToLower(fmt.Sprintf("%s_%s_%s", r.Operator, r.Avs, r.Strategy))
			val, ok := mappedExpectedResults[slotId]
			if !ok {
				mappedExpectedResults[slotId] = make([]string, 0)
			}
			mappedExpectedResults[slotId] = append(val, r.Snapshot)
		}

		lacksExpectedResult := make([]*OperatorAvsStrategySnapshot, 0)
		// Go line-by-line in the window results and find the corresponding line in the expected results.
		// If one doesnt exist, add it to the missing list.
		for _, window := range windows {
			slotId := strings.ToLower(fmt.Sprintf("%s_%s_%s", window.Operator, window.Avs, window.Strategy))

			found, ok := mappedExpectedResults[slotId]
			if !ok {
				lacksExpectedResult = append(lacksExpectedResult, window)
				t.Logf("Could not find expected result for %+v", window)
				continue
			}
			if !slices.Contains(found, window.Snapshot) {
				t.Logf("Found result, but snapshot doesnt match: %+v - %v", window, found)
				lacksExpectedResult = append(lacksExpectedResult, window)
			}
		}
		assert.Equal(t, 0, len(lacksExpectedResult))
	})
	t.Cleanup(func() {
		teardownOperatorAvsStrategyWindows(grm)
		tests.DeleteTestSqliteDB(dbFileName)
	})
}
