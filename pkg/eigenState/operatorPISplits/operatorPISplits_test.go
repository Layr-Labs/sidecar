package operatorPISplits

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
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
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func teardown(model *OperatorPISplitModel) {
	queries := []string{
		`truncate table operator_pi_splits`,
		`truncate table blocks cascade`,
	}
	for _, query := range queries {
		res := model.DB.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
	}
}

func createBlock(model *OperatorPISplitModel, blockNumber uint64) error {
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

func Test_OperatorPISplit(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)

		model, err := NewOperatorPISplitModel(esm, grm, l, cfg)

		t.Run("Handle an operator pi split", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "caller", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "operator", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "activatedAt", "Type": "uint32", "Value": 1725494400, "Indexed": false}, {"Name": "oldOperatorAVSSplitBips", "Type": "uint16", "Value": 1000, "Indexed": false}, {"Name": "newOperatorAVSSplitBips", "Type": "uint16", "Value": 2000, "Indexed": false}]`,
				EventName:        "OperatorPISplitBipsSet",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"activatedAt": 1725494400, "oldOperatorAVSSplitBips": 1000, "newOperatorAVSSplitBips": 2000}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			split := change.(*OperatorPISplit)

			assert.Equal(t, strings.ToLower("0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"), strings.ToLower(split.Operator))
			assert.Equal(t, int64(1725494400), split.ActivatedAt.Unix())
			assert.Equal(t, uint64(1000), split.OldOperatorAVSSplitBips)
			assert.Equal(t, uint64(2000), split.NewOperatorAVSSplitBips)

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			splits := make([]*OperatorPISplit, 0)
			query := `select * from operator_pi_splits where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&splits)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(splits))

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
