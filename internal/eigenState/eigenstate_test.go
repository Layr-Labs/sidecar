package eigenState

import (
	"testing"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/avsOperators"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/operatorShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/internal/tests/sqlite"
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

	db, err := sqlite.GetInMemorySqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return cfg, db, l, err
}

func teardown(grm *gorm.DB) {
	grm.Exec("delete from avs_operator_changes")
	grm.Exec("delete from registered_avs_operators")
}

func Test_EigenStateManager(t *testing.T) {
	cfg, grm, l, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new EigenStateManager", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		assert.NotNil(t, esm)
	})
	t.Run("Should create a state root with states from models", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		avsOperatorsModel, err := avsOperators.NewAvsOperatorsModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, avsOperatorsModel)

		operatorSharesModel, err := operatorShares.NewOperatorSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, operatorSharesModel)

		indexes := esm.GetSortedModelIndexes()
		assert.Equal(t, 2, len(indexes))
		assert.Equal(t, 0, indexes[0])
		assert.Equal(t, 1, indexes[1])

		err = esm.InitProcessingForBlock(200)
		assert.Nil(t, err)

		root, err := esm.GenerateStateRoot(200, "0x123")
		assert.Nil(t, err)
		assert.True(t, len(root) > 0)
	})
	teardown(grm)
}
