package stateManager

import (
	"testing"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	db, err := tests.GetSqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return cfg, db, l, err
}

func teardown(grm *gorm.DB) {
	grm.Exec("delete from state_roots")
}

func Test_StateManager(t *testing.T) {
	_, grm, l, err := setup()

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

	teardown(grm)
}
