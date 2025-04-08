package operatorSetStrategyRegistrations

import (
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

func teardown(model *OperatorSetStrategyRegistrationModel) {
	queries := []string{
		`truncate table operator_set_strategy_registrations`,
		`truncate table blocks cascade`,
	}
	for _, query := range queries {
		res := model.DB.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
	}
}

func createBlock(model *OperatorSetStrategyRegistrationModel, blockNumber uint64) error {
	block := &storage.Block{
		Number:    blockNumber,
		Hash:      "some hash",
		BlockTime: time.Now().Add(time.Hour * time.Duration(blockNumber)),
	}
	res := model.DB.Model(&storage.Block{}).Create(block)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func Test_OperatorSetStrategyRegistration(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewOperatorSetStrategyRegistrationModel(esm, grm, l, cfg)

		t.Run("Handle a strategy added to an operator set", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operatorSet", "Type": "tuple", "Value": {"avs": "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "id": 1}, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": false}]`,
				EventName:        "StrategyAddedToOperatorSet",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operatorSet": {"avs": "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "id": 1}, "strategy": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategyRegistration := change.(*OperatorSetStrategyRegistration)

			assert.Equal(t, strings.ToLower("0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"), strings.ToLower(strategyRegistration.Strategy))
			assert.Equal(t, strings.ToLower("0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1"), strings.ToLower(strategyRegistration.Avs))
			assert.Equal(t, uint64(1), strategyRegistration.OperatorSetId)
			assert.Equal(t, true, strategyRegistration.IsActive)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			strategyRegistrations := make([]*OperatorSetStrategyRegistration, 0)
			query := `select * from operator_set_strategy_registrations where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&strategyRegistrations)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(strategyRegistrations))

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Run("Handle a strategy removed from an operator set", func(t *testing.T) {
			blockNumber := uint64(103)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operatorSet", "Type": "tuple", "Value": {"avs": "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "id": 1}, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": false}]`,
				EventName:        "StrategyRemovedFromOperatorSet",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operatorSet": {"avs": "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "id": 1}, "strategy": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategyRegistration := change.(*OperatorSetStrategyRegistration)

			assert.Equal(t, strings.ToLower("0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"), strings.ToLower(strategyRegistration.Strategy))
			assert.Equal(t, strings.ToLower("0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1"), strings.ToLower(strategyRegistration.Avs))
			assert.Equal(t, uint64(1), strategyRegistration.OperatorSetId)
			assert.Equal(t, false, strategyRegistration.IsActive)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			strategyRegistrations := make([]*OperatorSetStrategyRegistration, 0)
			query := `select * from operator_set_strategy_registrations where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&strategyRegistrations)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(strategyRegistrations))

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Cleanup(func() {
			teardown(model)
		})
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
