package slashedOperators

import (
	"fmt"
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

func createBlock(model *SlashedOperatorModel, blockNumber uint64) error {
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

func Test_SlashedOperators(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewSlashedOperatorModel(esm, grm, l, cfg)

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
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": null, "Indexed": false}, {"Name": "operatorSet", "Type": "(address,uint32)", "Value": null, "Indexed": false}, {"Name": "strategies", "Type": "address[]", "Value": null, "Indexed": false}, {"Name": "wadSlashed", "Type": "uint256[]", "Value": null, "Indexed": false}, {"Name": "description", "Type": "string", "Value": null, "Indexed": false}]`,
				EventName:        "OperatorSlashed",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operator": "0x93a797473810c125ece22f25a2087b6ceb8ce886", "strategies": ["0x1bc0b67cccd43aaeb380c56a3a17200f3d1cf81b", "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "0x947e522010e22856071f8fb03e735fedfccd6e9f", "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "0xee2eeaba1458f1c8419993f098f9a82fe92954bc"], "wadSlashed": [194526931019878002, 79778140132002167, 141181003474358656, 142693720193774487, 65356369960153455], "description": "Liveness Fault", "operatorSet": {"id": 5, "avs": "0x69aa865947f6c9191b02954b1dd1a44131541226"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.([]*SlashedOperator)
			fmt.Printf("Record: %+v\n", record)

			assert.Equal(t, 5, len(record))

			strategies := []string{"0x1bc0b67cccd43aaeb380c56a3a17200f3d1cf81b", "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "0x947e522010e22856071f8fb03e735fedfccd6e9f", "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "0xee2eeaba1458f1c8419993f098f9a82fe92954bc"}
			wadSlashed := []string{"194526931019878002", "79778140132002167", "141181003474358656", "142693720193774487", "65356369960153455"}

			for i, r := range record {
				assert.Equal(t, "0x93a797473810c125ece22f25a2087b6ceb8ce886", r.Operator)
				assert.Equal(t, wadSlashed[i], r.WadSlashed)
				assert.Equal(t, strategies[i], r.Strategy)
				assert.Equal(t, "Liveness Fault", r.Description)
				assert.Equal(t, uint64(5), r.OperatorSetId)
				assert.Equal(t, strings.ToLower("0x69aa865947f6c9191b02954b1dd1a44131541226"), r.Avs)
			}

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			foundRecords := make([]*SlashedOperator, 0)
			query := `select * from slashed_operators where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&foundRecords)
			assert.Nil(t, res.Error)
			assert.Equal(t, 5, len(foundRecords))

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
