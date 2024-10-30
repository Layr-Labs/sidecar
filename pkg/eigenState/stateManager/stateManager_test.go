package stateManager

import (
	"github.com/Layr-Labs/go-sidecar/pkg/postgres"
	"testing"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/types"
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

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func Test_StateManager(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	insertedStateRoots := make([]*StateRoot, 0)

	t.Run("Should create a new EigenStateManager", func(t *testing.T) {
		esm := NewEigenStateManager(l, grm)
		assert.NotNil(t, esm)
	})
	t.Run("Should write a state root to the db", func(t *testing.T) {
		esm := NewEigenStateManager(l, grm)

		blockNumber := uint64(200)
		blockHash := "0x123"
		stateRoot := types.StateRoot("0x456")

		root, err := esm.WriteStateRoot(blockNumber, blockHash, stateRoot)
		assert.Nil(t, err)
		assert.Equal(t, blockNumber, root.EthBlockNumber)
		assert.Equal(t, blockHash, root.EthBlockHash)
		assert.Equal(t, string(stateRoot), root.StateRoot)
		insertedStateRoots = append(insertedStateRoots, root)
	})
	t.Run("Should read a state root from the db", func(t *testing.T) {
		esm := NewEigenStateManager(l, grm)

		blockNumber := insertedStateRoots[0].EthBlockNumber

		root, err := esm.GetStateRootForBlock(blockNumber)
		assert.Nil(t, err)

		assert.Equal(t, insertedStateRoots[0].EthBlockNumber, root.EthBlockNumber)
		assert.Equal(t, insertedStateRoots[0].EthBlockHash, root.EthBlockHash)
		assert.Equal(t, insertedStateRoots[0].StateRoot, root.StateRoot)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
