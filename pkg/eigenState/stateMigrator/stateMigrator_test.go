package stateMigrator

import (
	"encoding/hex"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetOperatorRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetStrategyRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setup() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Preprod
	//cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.Debug = true
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func loadTestSqlFile(projectRoot string, filePath string) string {
	p, err := filepath.Abs(fmt.Sprintf("%s/pkg/eigenState/stateMigrator/testdata/%s", projectRoot, filePath))
	if err != nil {
		panic(err)
	}
	fileContents, err := os.ReadFile(p)
	if err != nil {
		panic(err)
	}

	return strings.Trim(string(fileContents), "\n")
}

func loadBlocks(grm *gorm.DB, projectRoot string) {
	blocksContent := loadTestSqlFile(projectRoot, "blocks.sql")

	res := grm.Exec(blocksContent)
	if res.Error != nil {
		panic(res.Error)
	}
}

func loadTransactionLogs(grm *gorm.DB, projectRoot string) {
	txLogsContent := loadTestSqlFile(projectRoot, "transactionLogs.sql")

	res := grm.Exec(txLogsContent)
	if res.Error != nil {
		panic(res.Error)
	}
}

func Test_StateMigrator(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	projectRoot := tests.GetProjectRoot()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new StateMigrator", func(t *testing.T) {
		sm, err := NewStateMigrator(grm, cfg, l)
		if err != nil {
			t.Fatal(err)
		}
		if sm == nil {
			t.Fatal("StateMigrator is nil")
		}
	})

	t.Run("Should run no migrations for a block that has no migrations", func(t *testing.T) {
		sm, err := NewStateMigrator(grm, cfg, l)
		if err != nil {
			t.Fatal(err)
		}
		if sm == nil {
			t.Fatal("StateMigrator is nil")
		}
		migrations := sm.migrations.GetMigrationsForBlock(1000)
		assert.Equal(t, len(migrations), 0)
	})

	t.Run("Should run migration for preprod block", func(t *testing.T) {
		loadBlocks(grm, projectRoot)
		loadTransactionLogs(grm, projectRoot)

		sm, err := NewStateMigrator(grm, cfg, l)
		if err != nil {
			t.Fatal(err)
		}
		if sm == nil {
			t.Fatal("StateMigrator is nil")
		}
		forks, err := cfg.GetRewardsSqlForkDates()
		assert.Nil(t, err)

		blockNumber := forks[config.RewardsFork_Mississippi].BlockNumber

		migrations := sm.migrations.GetMigrationsForBlock(blockNumber)
		assert.Equal(t, len(migrations), 1)

		root, committedStates, err := sm.RunMigrationsForBlock(blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, root)
		assert.NotNil(t, committedStates)
		assert.True(t, len(root) > 0)
		hexRoot := hex.EncodeToString(root)
		fmt.Printf("Root: %s\n", hexRoot)

		assert.True(t, len(committedStates) > 0)

		var opsetRegistrations []operatorSetOperatorRegistrations.OperatorSetOperatorRegistration
		res := grm.Model(&operatorSetOperatorRegistrations.OperatorSetOperatorRegistration{}).Find(&opsetRegistrations)
		assert.Nil(t, res.Error)
		assert.True(t, len(opsetRegistrations) > 0)

		var opsetStrategyRegistrations []operatorSetStrategyRegistrations.OperatorSetStrategyRegistration
		res = grm.Model(&operatorSetStrategyRegistrations.OperatorSetStrategyRegistration{}).Find(&opsetStrategyRegistrations)
		assert.Nil(t, res.Error)
		assert.True(t, len(opsetStrategyRegistrations) > 0)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
