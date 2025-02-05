package protocolDataService

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"testing"
)

func setup() (
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)
	pg, err := postgres.NewPostgres(pgConfig)
	if err != nil {
		l.Fatal("Failed to setup postgres connection", zap.Error(err))
	}

	grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		l.Fatal("Failed to create gorm instance", zap.Error(err))
	}

	return grm, l, cfg, nil
}

func Test_ProtocolDataService(t *testing.T) {
	if !tests.LargeTestsEnabled() {
		t.Skipf("Skipping large test")
		return
	}

	grm, l, cfg, err := setup()

	t.Logf("Using database with name: %s", cfg.DatabaseConfig.DbName)

	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}
	smig, err := stateMigrator.NewStateMigrator(grm, cfg, l)
	if err != nil {
		t.Fatalf("Failed to create state migrator: %v", err)
	}
	sm := stateManager.NewEigenStateManager(smig, l, grm)

	pds := NewProtocolDataService(sm, grm, l, cfg)

	t.Run("Test ListRegisteredAVSsForOperator", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		blockNumber := uint64(3204393)

		avss, err := pds.ListRegisteredAVSsForOperator(context.Background(), operator, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(avss) > 0)
	})

	t.Run("Test ListDelegatedStrategiesForOperator", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		blockNumber := uint64(3204393)

		strategies, err := pds.ListDelegatedStrategiesForOperator(context.Background(), operator, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(strategies) > 0)
	})

	t.Run("Test GetOperatorDelegatedStake", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		strategy := "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"
		blockNumber := uint64(3204393)

		stake, err := pds.GetOperatorDelegatedStake(context.Background(), operator, strategy, blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stake)
	})

	t.Run("Test ListDelegatedStakersForOperator", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		blockNumber := uint64(3204393)

		stakers, err := pds.ListDelegatedStakersForOperator(context.Background(), operator, blockNumber, nil)
		assert.Nil(t, err)
		assert.True(t, len(stakers) > 0)
	})

	t.Run("Test ListStakerShares", func(t *testing.T) {
		staker := "0x130c646e1224d979ff23523308abb6012ce04b0a"
		blockNumber := uint64(3204391)

		shares, err := pds.ListStakerShares(context.Background(), staker, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(shares) > 0)
	})
}
