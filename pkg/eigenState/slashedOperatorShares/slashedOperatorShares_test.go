package slashedOperatorShares

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
	"testing"
	"time"
)

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

func createBlock(model *SlashedOperatorSharesModel, blockNumber uint64) error {
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

func Test_SlashedOperatorShares(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewSlashedOperatorSharesModel(esm, grm, l, cfg)

		t.Run("Handle an SlashedOperatorShareUpdated", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().DelegationManager,
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": "0x6355ea4fb1fca0c794950fff5efd6e2ee28acc14", "Indexed": true}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "totalSharesSlashed", "Type": "uint64", "Value": null, "Indexed": false}]`,
				EventName:        "OperatorSharesSlashed",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"strategy": "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "totalSlashedShares": 875000000000000000}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*SlashedOperatorShares)

			assert.Equal(t, "0x6355ea4fb1fca0c794950fff5efd6e2ee28acc14", record.Operator)
			assert.Equal(t, "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", record.Strategy)
			assert.Equal(t, "875000000000000000", record.TotalSlashedShares)

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			results := make([]*SlashedOperatorShares, 0)
			query := `select * from slashed_operator_shares where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&results)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(results))

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
