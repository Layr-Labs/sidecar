package operatorAllocations

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

func createBlock(model *OperatorAllocationModel, blockNumber uint64) error {
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

func Test_OperatorAllocations(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewOperatorAllocationModel(esm, grm, l, cfg)

		t.Run("Handle an allocationUpdated", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": null, "Indexed": false}, {"Name": "operatorSet", "Type": "(address,uint32)", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "magnitude", "Type": "uint64", "Value": null, "Indexed": false}, {"Name": "effectBlock", "Type": "uint32", "Value": null, "Indexed": false}]`,
				EventName:        "AllocationUpdated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operator": "0x93a797473810c125ece22f25a2087b6ceb8ce886", "strategy": "0x947e522010e22856071f8fb03e735fedfccd6e9f", "magnitude": 400000000000000000, "effectBlock": 2937866, "operatorSet": {"id": 5, "avs": "0x69aa865947f6c9191b02954b1dd1a44131541226"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*OperatorAllocation)

			assert.Equal(t, strings.ToLower("0x93a797473810c125ece22f25a2087b6ceb8ce886"), record.Operator)
			assert.Equal(t, strings.ToLower("0x947e522010e22856071f8fb03e735fedfccd6e9f"), record.Strategy)
			assert.Equal(t, "400000000000000000", record.Magnitude)
			assert.Equal(t, uint64(2937866), record.EffectiveBlock)
			assert.Equal(t, uint64(5), record.OperatorSetId)
			assert.Equal(t, strings.ToLower("0x69aa865947f6c9191b02954b1dd1a44131541226"), record.Avs)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			results := make([]*OperatorAllocation, 0)
			query := `select * from operator_allocations where block_number = ?`
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
