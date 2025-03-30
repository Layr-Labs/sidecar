package runtime

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"testing"
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

func insertStateRoot(grm *gorm.DB, blockNumber int) error {
	root, _ := uuid.NewRandom()
	stateRoot := &stateManager.StateRoot{
		EthBlockNumber: uint64(blockNumber),
		EthBlockHash:   fmt.Sprintf("0xSomething-%d", blockNumber),
		StateRoot:      root.String(),
	}

	block := &storage.Block{
		Number: uint64(blockNumber),
		Hash:   "something",
	}

	res := grm.Model(&storage.Block{}).Create(&block)
	if res.Error != nil {
		return res.Error
	}

	res = grm.Model(&stateManager.StateRoot{}).Create(&stateRoot)
	return res.Error
}

func Test_SidecarRuntime(t *testing.T) {
	dbName, grm, l, cfg, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	rtime := NewSidecarRuntime(grm, cfg, l)

	t.Run("Should insert a new version when there isnt one", func(t *testing.T) {
		version := "v1.0.0"
		blockNumber := 1

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})
	t.Run("Should fail due to the version being older", func(t *testing.T) {
		version := "v0.1.0"
		blockNumber := 2

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.NotNil(t, err)
	})
	t.Run("Should upgrade the version to a minor release", func(t *testing.T) {
		version := "v1.1.0"
		blockNumber := 3

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})
	t.Run("Should upgrade the version to a patch release", func(t *testing.T) {
		version := "v1.1.1"
		blockNumber := 4

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})
	t.Run("Should upgrade the version to a  major release", func(t *testing.T) {
		version := "v2.0.0"
		blockNumber := 5

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})
	t.Run("Should upgrade the version with a commit suffix", func(t *testing.T) {
		version := "v2.0.0+abc123"
		blockNumber := 6

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})
	t.Run("Should upgrade the version with an RC suffix", func(t *testing.T) {
		version := "v2.0.1-rc.1"
		blockNumber := 7

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})
	t.Run("Should upgrade the version with an RC suffix and commit suffix", func(t *testing.T) {
		version := "v2.0.1-rc.1+abc123"
		blockNumber := 8

		err = insertStateRoot(grm, blockNumber)
		assert.Nil(t, err)

		err := rtime.ValidateAndUpdateSidecarVersion(version)
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
