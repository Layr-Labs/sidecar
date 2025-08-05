package taskVerified

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
	cfg.Chain = config.Chain_Sepolia
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func Test_TaskVerified(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)

	taskVerifiedModel, err := NewTaskVerifiedModel(grm, l, cfg, msm)
	assert.Nil(t, err)

	t.Run("Should insert a TaskVerified event with executorCert and result", func(t *testing.T) {
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
			Address:          "0xb99cc53e8db7018f557606c2a5b066527bf96b26", // TaskMailbox address (Sepolia)
			Arguments:        `[{"Name": "aggregator", "Type": "address", "Value": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "Indexed": true}, {"Name": "taskHash", "Type": "bytes32", "Value": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x1234567890abcdef1234567890abcdef12345678", "Indexed": true}]`,
			EventName:        "TaskVerified",
			OutputData:       `{"executorOperatorSetId": 123, "executorCert": [84, 101, 115, 116, 32, 99, 101, 114, 116, 105, 102, 105, 99, 97, 116, 101, 32, 100, 97, 116, 97], "result": [84, 101, 115, 116, 32, 114, 101, 115, 117, 108, 116, 32, 100, 97, 116, 97]}`,
			LogIndex:         270,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := taskVerifiedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := taskVerifiedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := taskVerifiedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.TaskVerified)
		assert.Equal(t, "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", typedState.Aggregator)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", typedState.TaskHash)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", typedState.Avs)
		assert.Equal(t, uint32(123), typedState.ExecutorOperatorSetId)
		assert.Equal(t, "0x546573742063657274696669636174652064617461", typedState.ExecutorCert)
		assert.Equal(t, "0x5465737420726573756c742064617461", typedState.Result)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = taskVerifiedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the TaskVerified event was inserted
		var taskVerified types.TaskVerified
		res = grm.Model(&types.TaskVerified{}).Where("block_number = ?", block.Number).First(&taskVerified)
		assert.Nil(t, res.Error)

		err = taskVerifiedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Run("Should insert a TaskVerified event with empty executorCert and result", func(t *testing.T) {
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
			Address:          "0xb99cc53e8db7018f557606c2a5b066527bf96b26", // TaskMailbox address (Sepolia)
			Arguments:        `[{"Name": "aggregator", "Type": "address", "Value": "0x1111111111111111111111111111111111111111", "Indexed": true}, {"Name": "taskHash", "Type": "bytes32", "Value": "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x2222222222222222222222222222222222222222", "Indexed": true}]`,
			EventName:        "TaskVerified",
			OutputData:       `{"executorOperatorSetId": 456, "executorCert": [], "result": []}`,
			LogIndex:         271,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := taskVerifiedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := taskVerifiedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := taskVerifiedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.TaskVerified)
		assert.Equal(t, "0x1111111111111111111111111111111111111111", typedState.Aggregator)
		assert.Equal(t, "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321", typedState.TaskHash)
		assert.Equal(t, "0x2222222222222222222222222222222222222222", typedState.Avs)
		assert.Equal(t, uint32(456), typedState.ExecutorOperatorSetId)
		assert.Equal(t, "0x", typedState.ExecutorCert)
		assert.Equal(t, "0x", typedState.Result)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = taskVerifiedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the TaskVerified event was inserted
		var taskVerified types.TaskVerified
		res = grm.Model(&types.TaskVerified{}).Where("block_number = ?", block.Number).First(&taskVerified)
		assert.Nil(t, res.Error)

		err = taskVerifiedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Run("Should insert a TaskVerified event with large executorCert and result", func(t *testing.T) {
		block := &storage.Block{
			Number:    20535301,
			Hash:      "",
			BlockTime: time.Time{},
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		log := &storage.TransactionLog{
			TransactionHash:  "0x767e002f6f3a7942b22e38f2434ecd460fb2111b7ea584d16adb71692b856803",
			TransactionIndex: 79,
			Address:          "0xb99cc53e8db7018f557606c2a5b066527bf96b26", // TaskMailbox address (Sepolia)
			Arguments:        `[{"Name": "aggregator", "Type": "address", "Value": "0x5555555555555555555555555555555555555555", "Indexed": true}, {"Name": "taskHash", "Type": "bytes32", "Value": "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "Indexed": true}, {"Name": "avs", "Type": "address", "Value": "0x6666666666666666666666666666666666666666", "Indexed": true}]`,
			EventName:        "TaskVerified",
			OutputData:       `{"executorOperatorSetId": 789, "executorCert": [76, 111, 110, 103, 32, 101, 120, 101, 99, 117, 116, 111, 114, 32, 99, 101, 114, 116, 105, 102, 105, 99, 97, 116, 101, 32, 100, 97, 116, 97, 32, 119, 105, 116, 104, 32, 97, 32, 118, 101, 114, 121, 32, 108, 111, 110, 103, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 32, 116, 111, 32, 116, 101, 115, 116, 32, 108, 97, 114, 103, 101, 32, 100, 97, 116, 97, 32, 104, 97, 110, 100, 108, 105, 110, 103], "result": [76, 111, 110, 103, 32, 116, 97, 115, 107, 32, 114, 101, 115, 117, 108, 116, 32, 100, 97, 116, 97, 32, 119, 105, 116, 104, 32, 97, 32, 118, 101, 114, 121, 32, 108, 111, 110, 103, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 32, 116, 111, 32, 116, 101, 115, 116, 32, 108, 97, 114, 103, 101, 32, 100, 97, 116, 97, 32, 104, 97, 110, 100, 108, 105, 110, 103]}`,
			LogIndex:         272,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := taskVerifiedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := taskVerifiedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := taskVerifiedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.TaskVerified)
		assert.Equal(t, "0x5555555555555555555555555555555555555555", typedState.Aggregator)
		assert.Equal(t, "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", typedState.TaskHash)
		assert.Equal(t, "0x6666666666666666666666666666666666666666", typedState.Avs)
		assert.Equal(t, uint32(789), typedState.ExecutorOperatorSetId)
		// Long executor certificate and result data as hex strings
		expectedCert := "0x4c6f6e67206578656375746f722063657274696669636174652064617461207769746820612076657279206c6f6e67206465736372697074696f6e20746f2074657374206c6172676520646174612068616e646c696e67"
		expectedResult := "0x4c6f6e67207461736b20726573756c742064617461207769746820612076657279206c6f6e67206465736372697074696f6e20746f2074657374206c6172676520646174612068616e646c696e67"
		assert.Equal(t, expectedCert, typedState.ExecutorCert)
		assert.Equal(t, expectedResult, typedState.Result)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = taskVerifiedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the TaskVerified event was inserted
		var taskVerified types.TaskVerified
		res = grm.Model(&types.TaskVerified{}).Where("block_number = ?", block.Number).First(&taskVerified)
		assert.Nil(t, res.Error)

		err = taskVerifiedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
