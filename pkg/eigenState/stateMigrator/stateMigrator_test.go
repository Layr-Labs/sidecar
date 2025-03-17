package stateMigrator

import (
	"encoding/hex"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/encumberedMagnitudes"
	operatorAllocationDelayDelays "github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAllocationDelays"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAllocations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorMaxMagnitudes"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetOperatorRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetStrategyRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSets"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperators"
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
	cfg.Debug = false
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

func loadBlocks(prefix string, grm *gorm.DB, projectRoot string) {
	blocksContent := loadTestSqlFile(projectRoot, fmt.Sprintf("%s/blocks.sql", prefix))

	res := grm.Exec(blocksContent)
	if res.Error != nil {
		panic(res.Error)
	}
}

func loadTransactionLogs(prefix string, grm *gorm.DB, projectRoot string) {
	txLogsContent := loadTestSqlFile(projectRoot, fmt.Sprintf("%s/transactionLogs.sql", prefix))

	res := grm.Exec(txLogsContent)
	if res.Error != nil {
		panic(res.Error)
	}
}

func Test_StateMigrator(t *testing.T) {
	projectRoot := tests.GetProjectRoot()

	t.Run("Should create a new StateMigrator", func(t *testing.T) {
		dbName, grm, l, cfg, err := setup()
		if err != nil {
			t.Fatal(err)
		}
		defer postgres.TeardownTestDatabase(dbName, cfg, grm, l)

		sm, err := NewStateMigrator(grm, cfg, l)
		if err != nil {
			t.Fatal(err)
		}
		if sm == nil {
			t.Fatal("StateMigrator is nil")
		}

	})

	t.Run("Should run no migrations for a block that has no migrations", func(t *testing.T) {
		dbName, grm, l, cfg, err := setup()
		if err != nil {
			t.Fatal(err)
		}
		defer postgres.TeardownTestDatabase(dbName, cfg, grm, l)

		sm, err := NewStateMigrator(grm, cfg, l)
		if err != nil {
			t.Fatal(err)
		}
		if sm == nil {
			t.Fatal("StateMigrator is nil")
			return
		}
		migrations := sm.migrations.GetMigrationsForBlock(1000)
		assert.Equal(t, len(migrations), 0)
	})

	t.Run("Should run migration for preprod block for rewards v2.1 migration", func(t *testing.T) {
		dbName, grm, l, cfg, err := setup()
		if err != nil {
			t.Fatal(err)
		}
		defer postgres.TeardownTestDatabase(dbName, cfg, grm, l)

		loadBlocks("rewardsV2-1", grm, projectRoot)
		loadTransactionLogs("rewardsV2-1", grm, projectRoot)

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
	t.Run("Should run migration for preprod block for slashing migration", func(t *testing.T) {
		dbName, grm, l, cfg, err := setup()
		if err != nil {
			t.Fatal(err)
		}
		defer postgres.TeardownTestDatabase(dbName, cfg, grm, l)

		loadBlocks("slashing", grm, projectRoot)
		loadTransactionLogs("slashing", grm, projectRoot)

		sm, err := NewStateMigrator(grm, cfg, l)
		if err != nil {
			t.Fatal(err)
		}
		if sm == nil {
			t.Fatal("StateMigrator is nil")
		}
		forks, err := cfg.GetRewardsSqlForkDates()
		assert.Nil(t, err)

		blockNumber := forks[config.RewardsFork_Brazos].BlockNumber

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

		var opsetResults []operatorSets.OperatorSet
		res := grm.Model(&operatorSets.OperatorSet{}).Find(&opsetResults)
		assert.Nil(t, res.Error)
		assert.True(t, len(opsetResults) > 0)

		var operatorAllocationresults []operatorAllocations.OperatorAllocation
		res = grm.Model(&operatorAllocations.OperatorAllocation{}).Find(&operatorAllocationresults)
		assert.Nil(t, res.Error)
		assert.True(t, len(operatorAllocationresults) > 0)

		var slashedOperatorResults []slashedOperators.SlashedOperator
		res = grm.Model(&slashedOperators.SlashedOperator{}).Find(&slashedOperatorResults)
		assert.Nil(t, res.Error)
		assert.True(t, len(slashedOperatorResults) > 0)

		var encumberedMagResults []encumberedMagnitudes.EncumberedMagnitude
		res = grm.Model(&encumberedMagnitudes.EncumberedMagnitude{}).Find(&encumberedMagResults)
		assert.Nil(t, res.Error)
		assert.True(t, len(encumberedMagResults) > 0)

		var operatorMaxMagResults []operatorMaxMagnitudes.OperatorMaxMagnitude
		res = grm.Model(&operatorMaxMagnitudes.OperatorMaxMagnitude{}).Find(&operatorMaxMagResults)
		assert.Nil(t, res.Error)
		assert.True(t, len(operatorMaxMagResults) > 0)

		var slashedOperatorSharesResults []slashedOperatorShares.SlashedOperatorShares
		res = grm.Model(&slashedOperatorShares.SlashedOperatorShares{}).Find(&slashedOperatorSharesResults)
		assert.Nil(t, res.Error)
		assert.True(t, len(slashedOperatorSharesResults) == 0)

		var operatorAllocationDelayResults []operatorAllocationDelayDelays.OperatorAllocationDelay
		res = grm.Model(&operatorAllocationDelayDelays.OperatorAllocationDelay{}).Find(&operatorAllocationDelayResults)
		assert.Nil(t, res.Error)
		assert.True(t, len(operatorAllocationDelayResults) > 0)
	})
}
