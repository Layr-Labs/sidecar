package operatorShares

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/eigenState"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/storage"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"math/big"
	"testing"
	"time"
)

func setup() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*eigenState.EigenStateManager,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	_, grm, err := tests.GetDatabaseConnection(cfg)

	eigenState := eigenState.NewEigenStateManager(l)

	return cfg, grm, l, eigenState, err
}

func teardown(model *OperatorSharesModel) {
	model.Db.Exec("truncate table operator_share_changes cascade")
	model.Db.Exec("truncate table operator_shares cascade")
}

func Test_OperatorSharesState(t *testing.T) {
	cfg, grm, l, esm, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new OperatorSharesState", func(t *testing.T) {
		model, err := NewOperatorSharesModel(esm, grm, cfg.Network, cfg.Environment, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, model)
	})
	t.Run("Should register OperatorSharesState", func(t *testing.T) {
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			BlockSequenceId:  big.NewInt(300).Uint64(),
			Address:          cfg.GetContractsMapForEnvAndNetwork().DelegationManager,
			Arguments:        `[{"Value": "0xdb9afbdcfeca94dfb25790c900c527969e78bd3c"}]`,
			EventName:        "OperatorSharesIncreased",
			LogIndex:         big.NewInt(400).Uint64(),
			OutputData:       `{"shares": "100", "strategy": "0x93c4b944d05dfe6df7645a86cd2206016c51564d"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		model, err := NewOperatorSharesModel(esm, grm, cfg.Network, cfg.Environment, l, cfg)

		change, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		teardown(model)
	})
	t.Run("Should register AvsOperatorState and generate the table for the block", func(t *testing.T) {
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			BlockSequenceId:  big.NewInt(300).Uint64(),
			Address:          cfg.GetContractsMapForEnvAndNetwork().DelegationManager,
			Arguments:        `[{"Value": "0xdb9afbdcfeca94dfb25790c900c527969e78bd3c"}]`,
			EventName:        "OperatorSharesIncreased",
			LogIndex:         big.NewInt(400).Uint64(),
			OutputData:       `{"shares": "100", "strategy": "0x93c4b944d05dfe6df7645a86cd2206016c51564d"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		model, err := NewOperatorSharesModel(esm, grm, cfg.Network, cfg.Environment, l, cfg)
		assert.Nil(t, err)

		stateChange, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, stateChange)

		err = model.WriteFinalState(blockNumber)
		assert.Nil(t, err)

		states := []OperatorShares{}
		statesRes := model.Db.
			Model(&OperatorShares{}).
			Raw("select * from operator_shares where block_number = @blockNumber", sql.Named("blockNumber", blockNumber)).
			Scan(&states)

		if statesRes.Error != nil {
			t.Fatalf("Failed to fetch operator_shares: %v", statesRes.Error)
		}
		assert.Equal(t, 1, len(states))

		assert.Equal(t, "100", states[0].Shares)

		stateRoot, err := model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		fmt.Printf("StateRoot: %s\n", stateRoot)

		teardown(model)
	})
}