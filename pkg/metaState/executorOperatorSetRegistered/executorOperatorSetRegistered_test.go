package executorOperatorSetRegistered

import (
	"os"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func Test_ExecutorOperatorSetRegistered(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)

	executorOperatorSetRegisteredModel, err := NewExecutorOperatorSetRegisteredModel(grm, l, cfg, msm)
	assert.Nil(t, err)

	t.Run("Should insert an ExecutorOperatorSetRegistered event with isRegistered = true", func(t *testing.T) {
		block := &storage.Block{
			Number:    20535299,
			Hash:      "",
			BlockTime: time.Time{},
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		log := &storage.TransactionLog{
			TransactionHash:  "0x767e002f6f3a7942b22e38f2434ecd460fb2111b7ea584d16adb71692b856801",
			TransactionIndex: 77,
			Address:          "0x0000000000000000000000000000000000000000", // TaskManager address
			Arguments:        `[{"Name": "caller", "Type": "address", "Value": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x1234567890abcdef1234567890abcdef12345678", "Indexed": true}, {"Name": "executorOperatorSetId", "Type": "uint32", "Value": 123, "Indexed": true}]`,
			EventName:        "ExecutorOperatorSetRegistered",
			OutputData:       `{"executorOperatorSetId": 123, "isRegistered": true}`,
			LogIndex:         270,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := executorOperatorSetRegisteredModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := executorOperatorSetRegisteredModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := executorOperatorSetRegisteredModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.ExecutorOperatorSetRegistered)
		assert.Equal(t, "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", typedState.Caller)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", typedState.Avs)
		assert.Equal(t, uint32(123), typedState.ExecutorOperatorSetId)
		assert.Equal(t, true, typedState.IsRegistered)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = executorOperatorSetRegisteredModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the ExecutorOperatorSetRegistered event was inserted
		var executorOperatorSetRegistered types.ExecutorOperatorSetRegistered
		res = grm.Model(&types.ExecutorOperatorSetRegistered{}).Where("block_number = ?", block.Number).First(&executorOperatorSetRegistered)
		assert.Nil(t, res.Error)

		err = executorOperatorSetRegisteredModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Run("Should insert an ExecutorOperatorSetRegistered event with isRegistered = false", func(t *testing.T) {
		block := &storage.Block{
			Number:    20535300,
			Hash:      "",
			BlockTime: time.Time{},
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		log := &storage.TransactionLog{
			TransactionHash:  "0x767e002f6f3a7942b22e38f2434ecd460fb2111b7ea584d16adb71692b856802",
			TransactionIndex: 78,
			Address:          "0x0000000000000000000000000000000000000000", // TaskManager address
			Arguments:        `[{"Name": "caller", "Type": "address", "Value": "0x1111111111111111111111111111111111111111", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x2222222222222222222222222222222222222222", "Indexed": true}, {"Name": "executorOperatorSetId", "Type": "uint32", "Value": 456, "Indexed": true}]`,
			EventName:        "ExecutorOperatorSetRegistered",
			OutputData:       `{"executorOperatorSetId": 456, "isRegistered": false}`,
			LogIndex:         271,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := executorOperatorSetRegisteredModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := executorOperatorSetRegisteredModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := executorOperatorSetRegisteredModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.ExecutorOperatorSetRegistered)
		assert.Equal(t, "0x1111111111111111111111111111111111111111", typedState.Caller)
		assert.Equal(t, "0x2222222222222222222222222222222222222222", typedState.Avs)
		assert.Equal(t, uint32(456), typedState.ExecutorOperatorSetId)
		assert.Equal(t, false, typedState.IsRegistered)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = executorOperatorSetRegisteredModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the ExecutorOperatorSetRegistered event was inserted
		var executorOperatorSetRegistered types.ExecutorOperatorSetRegistered
		res = grm.Model(&types.ExecutorOperatorSetRegistered{}).Where("block_number = ?", block.Number).First(&executorOperatorSetRegistered)
		assert.Nil(t, res.Error)

		err = executorOperatorSetRegisteredModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
