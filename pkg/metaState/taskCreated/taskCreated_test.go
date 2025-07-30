package taskCreated

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

func Test_TaskCreated(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)

	taskCreatedModel, err := NewTaskCreatedModel(grm, l, cfg, msm)
	assert.Nil(t, err)

	t.Run("Should insert a TaskCreated event", func(t *testing.T) {
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
			Address:          "0x0000000000000000000000000000000000000000", // TaskMailbox address
			Arguments:        `[{"Name": "creator", "Type": "address", "Value": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "Indexed": true}, {"Name": "taskHash", "Type": "bytes32", "Value": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x1234567890abcdef1234567890abcdef12345678", "Indexed": true}]`,
			EventName:        "TaskCreated",
			OutputData:       `{"executorOperatorSetId": 123, "refundCollector": "0x9876543210fedcba9876543210fedcba98765432", "avsFee": 1000000000000000000, "taskDeadline": 1640995200, "payload": "0x48656c6c6f20576f726c64"}`,
			LogIndex:         270,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := taskCreatedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := taskCreatedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := taskCreatedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.TaskCreated)
		assert.Equal(t, "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", typedState.Creator)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", typedState.TaskHash)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", typedState.Avs)
		assert.Equal(t, uint32(123), typedState.ExecutorOperatorSetId)
		assert.Equal(t, "0x9876543210fedcba9876543210fedcba98765432", typedState.RefundCollector)
		assert.Equal(t, "1000000000000000000", typedState.AvsFee)
		assert.Equal(t, uint64(1640995200), typedState.TaskDeadline)
		assert.Equal(t, []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64}, typedState.Payload)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = taskCreatedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the TaskCreated event was inserted
		var taskCreated types.TaskCreated
		res = grm.Model(&types.TaskCreated{}).Where("block_number = ?", block.Number).First(&taskCreated)
		assert.Nil(t, res.Error)

		err = taskCreatedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Run("Should insert a TaskCreated event with empty payload", func(t *testing.T) {
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
			Address:          "0x0000000000000000000000000000000000000000", // TaskMailbox address
			Arguments:        `[{"Name": "creator", "Type": "address", "Value": "0x1111111111111111111111111111111111111111", "Indexed": true}, {"Name": "taskHash", "Type": "bytes32", "Value": "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x2222222222222222222222222222222222222222", "Indexed": true}]`,
			EventName:        "TaskCreated",
			OutputData:       `{"executorOperatorSetId": 456, "refundCollector": "0x3333333333333333333333333333333333333333", "avsFee": 500000000000000000, "taskDeadline": 1640999999, "payload": "0x"}`,
			LogIndex:         271,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := taskCreatedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := taskCreatedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := taskCreatedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.TaskCreated)
		assert.Equal(t, "0x1111111111111111111111111111111111111111", typedState.Creator)
		assert.Equal(t, "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321", typedState.TaskHash)
		assert.Equal(t, "0x2222222222222222222222222222222222222222", typedState.Avs)
		assert.Equal(t, uint32(456), typedState.ExecutorOperatorSetId)
		assert.Equal(t, "0x3333333333333333333333333333333333333333", typedState.RefundCollector)
		assert.Equal(t, "500000000000000000", typedState.AvsFee)
		assert.Equal(t, uint64(1640999999), typedState.TaskDeadline)
		assert.Equal(t, []byte{}, typedState.Payload)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = taskCreatedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the TaskCreated event was inserted
		var taskCreated types.TaskCreated
		res = grm.Model(&types.TaskCreated{}).Where("block_number = ?", block.Number).First(&taskCreated)
		assert.Nil(t, res.Error)

		err = taskCreatedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
