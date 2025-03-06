package disabledDistributionRoots

import (
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
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
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func teardown(model *DisabledDistributionRootsModel) {
	queries := []string{
		`truncate table disabled_distribution_roots cascade`,
	}
	for _, query := range queries {
		model.DB.Raw(query)
	}
}

func Test_DisabledDistributionRoots(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	esm := stateManager.NewEigenStateManager(nil, l, grm)
	model, err := NewDisabledDistributionRootsModel(esm, grm, l, cfg)

	t.Run("Parse a disabled distribution root", func(t *testing.T) {
		blockNumber := uint64(100)
		block := &storage.Block{
			Number:    blockNumber,
			Hash:      "",
			BlockTime: time.Unix(1726063248, 0),
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		assert.Nil(t, res.Error)

		log := &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "rootIndex", "Type": "uint32", "Value": 8, "Indexed": true}]`,
			EventName:        "DistributionRootDisabled",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		isInteresting := model.IsInterestingLog(log)
		assert.True(t, isInteresting)

		change, err := model.HandleStateChange(log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange := change.(*types.DisabledDistributionRoot)
		assert.Equal(t, uint64(8), typedChange.RootIndex)
		assert.Equal(t, blockNumber, typedChange.BlockNumber)

		err = model.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `SELECT * FROM disabled_distribution_roots WHERE block_number = ?`
		var roots []*types.DisabledDistributionRoot
		res = model.DB.Raw(query, blockNumber).Scan(&roots)

		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(roots))

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
