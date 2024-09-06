package eigenState

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/internal/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
)

func setup() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	_, grm, err := tests.GetDatabaseConnection(cfg)

	return cfg, grm, l, err
}

func teardown(grm *gorm.DB) {
	grm.Exec("truncate table avs_operator_changes cascade")
	grm.Exec("truncate table registered_avs_operators cascade")
}

func Test_EigenStateManager(t *testing.T) {
	cfg, grm, l, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new EigenStateManager", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l)
		assert.NotNil(t, esm)
	})
	t.Run("Should create a state root with states from models", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l)
		avsOperatorsModel, err := avsOperators.NewAvsOperators(esm, grm, cfg.Network, cfg.Environment, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, avsOperatorsModel)

		operatorSharesModel, err := operatorShares.NewOperatorSharesModel(esm, grm, cfg.Network, cfg.Environment, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, operatorSharesModel)

		indexes := esm.GetSortedModelIndexes()
		assert.Equal(t, 2, len(indexes))
		assert.Equal(t, 0, indexes[0])
		assert.Equal(t, 1, indexes[1])

		root, err := esm.GenerateStateRoot(200)
		assert.Nil(t, err)
		assert.True(t, len(root) > 0)
		fmt.Printf("Root: %+v\n", root)
	})
	teardown(grm)
}