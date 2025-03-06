package submittedDistributionRoots

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

func teardown(model *SubmittedDistributionRootsModel) {
	queries := []string{
		`truncate table submitted_distribution_roots cascade`,
	}
	for _, query := range queries {
		model.DB.Raw(query)
	}
}

func Test_SubmittedDistributionRoots(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	esm := stateManager.NewEigenStateManager(nil, l, grm)
	model, err := NewSubmittedDistributionRootsModel(esm, grm, l, cfg)

	insertedRoots := make([]*types.SubmittedDistributionRoot, 0)

	t.Run("Parse a submitted distribution root with an index of 0x000...", func(t *testing.T) {
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
			Arguments:        `[{"Name": "rootIndex", "Type": "uint32", "Value": "0x0000000000000000000000000000000000000000"}, {"Name": "root", "Type": "bytes32", "Value": "0x169AaC3F9464C0468C99Aa875a30306037f24927"}, {"Name": "paymentCalculationEndTimestamp", "Type": "uint32", "Value": "0x00000000000000000000000000000000663EB500"}, {"Name": "activatedAt", "Type": "uint32", "Value": ""}]`,
			EventName:        "DistributionRootSubmitted",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"activatedAt": 1715626776}`,
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

		typedChange := change.(*types.SubmittedDistributionRoot)
		assert.Equal(t, uint64(0), typedChange.RootIndex)
		assert.Equal(t, "0x169AaC3F9464C0468C99Aa875a30306037f24927", typedChange.Root)
		assert.Equal(t, time.Unix(1715626776, 0), typedChange.ActivatedAt)
		assert.Equal(t, "timestamp", typedChange.ActivatedAtUnit)
		assert.Equal(t, time.Unix(1715385600, 0), typedChange.RewardsCalculationEnd)
		assert.Equal(t, "snapshot", typedChange.RewardsCalculationEndUnit)
		assert.Equal(t, blockNumber, typedChange.CreatedAtBlockNumber)
		assert.Equal(t, uint64(100), typedChange.BlockNumber)

		err = model.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `SELECT * FROM submitted_distribution_roots WHERE block_number = ?`
		var roots []*types.SubmittedDistributionRoot
		res = model.DB.Raw(query, blockNumber).Scan(&roots)

		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(roots))

		insertedRoots = append(insertedRoots, roots[0])

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Run("Parse a submitted distribution root with numeric arguments", func(t *testing.T) {
		blockNumber := uint64(101)
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
			Arguments:        `[{"Name": "rootIndex", "Type": "uint32", "Value": 43, "Indexed": true}, {"Name": "root", "Type": "bytes32", "Value": "0xa40e58b05ab9cc79321f85cbe6a4c1df9fa8f04f80bb9c1c77b464b1dc4c5bd3", "Indexed": true}, {"Name": "rewardsCalculationEndTimestamp", "Type": "uint32", "Value": 1719964800, "Indexed": true}, {"Name": "activatedAt", "Type": "uint32", "Value": null, "Indexed": false}]`,
			EventName:        "DistributionRootSubmitted",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"activatedAt": 1720099932}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		isInteresting := model.IsInterestingLog(log)
		assert.True(t, isInteresting)

		change, err := model.HandleStateChange(log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange := change.(*types.SubmittedDistributionRoot)
		assert.Equal(t, uint64(43), typedChange.RootIndex)
		assert.Equal(t, "0xa40e58b05ab9cc79321f85cbe6a4c1df9fa8f04f80bb9c1c77b464b1dc4c5bd3", typedChange.Root)
		assert.Equal(t, time.Unix(1720099932, 0), typedChange.ActivatedAt)
		assert.Equal(t, "timestamp", typedChange.ActivatedAtUnit)
		assert.Equal(t, time.Unix(1719964800, 0), typedChange.RewardsCalculationEnd)
		assert.Equal(t, "snapshot", typedChange.RewardsCalculationEndUnit)
		assert.Equal(t, blockNumber, typedChange.CreatedAtBlockNumber)
		assert.Equal(t, uint64(101), typedChange.BlockNumber)

		err = model.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `SELECT * FROM submitted_distribution_roots WHERE block_number = ?`
		var roots []*types.SubmittedDistributionRoot
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
