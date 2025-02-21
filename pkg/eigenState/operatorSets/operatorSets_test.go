package operatorSets

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"
)

/*
[
  {
    "transaction_hash": "0xacd5dc7381749340b9f3c97edf72941cca3f879f28b6595bb1d572e719c7a52a",
    "address": "0xfdd5749e11977d60850e06bf5b13221ad95eb6b4",
    "arguments": [{"Name": "operatorSet", "Type": "(address,uint32)", "Value": null, "Indexed": false}],
    "event_name": "OperatorSetCreated",
    "log_index": 19,
    "created_at": "2024-12-21 00:16:08.248295 +00:00",
    "updated_at": "2024-12-21 00:16:08.248295 +00:00",
    "deleted_at": "0001-01-01 00:00:00.000000 +00:00",
    "block_number": 2978149,
    "transaction_index": 10,
    "block_sequence_id": 2025524,
    "output_data": {"operatorSet": {"id": 0, "avs": "0x99ee5cb4fd535f1bc9ca0f10da5078fe1f9fc866"}}
  }
]
*/

func setup() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func createBlock(model *OperatorSetModel, blockNumber uint64) error {
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

func Test_OperatorSets(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewOperatorSetModel(esm, grm, l, cfg)

		t.Run("Handle an operator set", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operatorSet", "Type": "(address,uint32)", "Value": null, "Indexed": false}]`,
				EventName:        "OperatorSetCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operatorSet": {"id": 0, "avs": "0x99ee5cb4fd535f1bc9ca0f10da5078fe1f9fc866"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			split := change.(*OperatorSet)

			assert.Equal(t, uint64(0), split.OperatorSetId)
			assert.Equal(t, strings.ToLower("0x99ee5cb4fd535f1bc9ca0f10da5078fe1f9fc866"), strings.ToLower(split.Avs))

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			splits := make([]*OperatorSet, 0)
			query := `select * from operator_sets where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&splits)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(splits))

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)
		})
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
