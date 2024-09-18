package rewards

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
)

var previousEnv = make(map[string]string)

func setupOperatorAvsStrategyWindows() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	tests.ReplaceEnv(map[string]string{
		"SIDECAR_ENVIRONMENT":           "testnet",
		"SIDECAR_NETWORK":               "holesky",
		"SIDECAR_ETHEREUM_RPC_BASE_URL": "http://34.229.43.36:8545",
		"SIDECAR_ETHERSCAN_API_KEYS":    "some key",
		"SIDECAR_STATSD_URL":            "localhost:8125",
		"SIDECAR_DEBUG":                 "true",
	}, &previousEnv)

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

func teardownOperatorAvsStrategyWindows(grm *gorm.DB) {
	queries := []string{
		`delete from operator_avs_strategy_snapshots`,
	}
	for _, query := range queries {
		grm.Exec(query)
	}
}

func hydrateOperatorAvsRestakedStrategies(grm *gorm.DB, l *zap.Logger) (int, error) {
	contents, err := tests.GetOperatorAvsRestakedStrategiesSqlFile()

	if err != nil {
		return 0, err
	}

	_, err = sqlite.WrapTxAndCommit[interface{}](func(tx *gorm.DB) (interface{}, error) {
		for i, content := range contents {
			res := grm.Exec(content)
			if res.Error != nil {
				l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error), zap.String("query", content), zap.Int("lineNumber", i))
				return nil, res.Error
			}
		}
		return nil, nil
	}, grm, nil)
	return len(contents), err
}

func Test_OperatorAvsStrategySnapshots(t *testing.T) {
	cfg, grm, l, err := setupOperatorAvsStrategyWindows()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		inserted, err := hydrateOperatorAvsRestakedStrategies(grm, l)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("Inserted %d\n", inserted)
	})

	t.Run("Should calculate correct operatorAvsStrategy windows", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, nil, grm, cfg)

		windows, err := rewards.GenerateOperatorAvsStrategySnapshots("2024-09-01")
		assert.Nil(t, err)

		expectedResults, err := tests.GetExpectedOperatorAvsSnapshots()
		assert.Nil(t, err)

		assert.Equal(t, len(expectedResults), len(windows))

		lacksExpectedResult := make([]*OperatorAvsStrategySnapshot, 0)
		// Go line-by-line in the window results and find the corresponding line in the expected results.
		// If one doesnt exist, add it to the missing list.
		for _, window := range windows {
			match := utils.Find(expectedResults, func(expected *tests.ExpectedOperatorAvsSnapshot) bool {
				if expected.Operator == window.Operator &&
					expected.Avs == window.Avs &&
					expected.Strategy == window.Strategy &&
					expected.Snapshot == window.Snapshot {
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
				fmt.Printf("%d - Snapshot: %+v\n", i, window)
			}
		}
	})
	t.Cleanup(func() {
		teardownOperatorAvsStrategyWindows(grm)
		tests.RestoreEnv(previousEnv)
	})
}
