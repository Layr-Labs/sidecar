package generationReservationRemoved

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

func Test_GenerationReservationRemoved(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)

	generationReservationRemovedModel, err := NewGenerationReservationRemovedModel(grm, l, cfg, msm)
	assert.Nil(t, err)

	t.Run("Should insert a generationReservationRemoved event", func(t *testing.T) {
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
			Address:          "0x287381b1570d9048c4b4c7ec94d21ddb8aa1352a", // CrossChainRegistry address (Sepolia)
			Arguments:        `[{"Name": "operatorSet", "Type": "tuple", "Value": {"avs": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "id": 12345}, "Indexed": false}]`,
			EventName:        "GenerationReservationRemoved",
			OutputData:       `{"operatorSet": {"avs": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "id": 12345}}`,
			LogIndex:         270,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := generationReservationRemovedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := generationReservationRemovedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := generationReservationRemovedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.GenerationReservationRemoved)
		assert.Equal(t, "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", typedState.Avs)
		assert.Equal(t, uint64(12345), typedState.OperatorSetId)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = generationReservationRemovedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the generationReservationRemoved event was inserted
		var generationReservationRemoved types.GenerationReservationRemoved
		res = grm.Model(&types.GenerationReservationRemoved{}).Where("block_number = ?", block.Number).First(&generationReservationRemoved)
		assert.Nil(t, res.Error)

		err = generationReservationRemovedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)

	})

	t.Run("Should insert another generationReservationRemoved event with different operator set", func(t *testing.T) {
		block := &storage.Block{
			Number:    20535362,
			Hash:      "",
			BlockTime: time.Time{},
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		log := &storage.TransactionLog{
			TransactionHash:  "0x767e002f6f3a7942b22e38f2434ecd460fb2111b7ea584d16adb71692b856802",
			TransactionIndex: 42,
			Address:          "0x287381b1570d9048c4b4c7ec94d21ddb8aa1352a", // CrossChainRegistry address (Sepolia)
			Arguments:        `[{"Name": "operatorSet", "Type": "tuple", "Value": {"avs": "0x769e73da377876dd688b23d51ed01b7c7b154c65", "id": 67890}, "Indexed": false}]`,
			EventName:        "GenerationReservationRemoved",
			OutputData:       `{"operatorSet": {"avs": "0x769e73da377876dd688b23d51ed01b7c7b154c65", "id": 67890}}`,
			LogIndex:         200,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := generationReservationRemovedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := generationReservationRemovedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := generationReservationRemovedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.GenerationReservationRemoved)
		assert.Equal(t, "0x769e73da377876dd688b23d51ed01b7c7b154c65", typedState.Avs)
		assert.Equal(t, uint64(67890), typedState.OperatorSetId)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = generationReservationRemovedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the generationReservationRemoved event was inserted
		var generationReservationRemoved types.GenerationReservationRemoved
		res = grm.Model(&types.GenerationReservationRemoved{}).Where("block_number = ?", block.Number).First(&generationReservationRemoved)
		assert.Nil(t, res.Error)

		err = generationReservationRemovedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
