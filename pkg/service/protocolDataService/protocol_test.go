package protocolDataService

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup(dataSourceName string) (
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	projectRoot := tests.GetProjectRoot()
	datasources, err := tests.GetTestDataSources(projectRoot)
	if err != nil {
		log.Fatalf("Failed to get datasources: %v", err)
	}
	dsTestnet := datasources.GetDataSourceByName(dataSourceName)

	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()
	cfg.DatabaseConfig.DbName = dsTestnet.DbName

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

	grm, l, cfg, err := setup("testnetFull")

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
		operator := "0x7dcdae42c07ae52d6c5c9554e7c5109f19b2613d"
		blockNumber := uint64(3240224)

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
		assert.Equal(t, stake.Shares, "999960761744418521836")
		assert.Equal(t, stake.AvsAddresses[0], "0xd9b1da8159cf83ccc55ad5757bea33e6f0ce34be")

	})

	t.Run("Test ListDelegatedStakersForOperator", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		blockNumber := uint64(3204393)

		stakers, err := pds.ListDelegatedStakersForOperator(context.Background(), operator, blockNumber, nil)
		assert.Nil(t, err)
		assert.True(t, len(stakers) > 0)
	})

	t.Run("Test ListStakerShares", func(t *testing.T) {
		t.Run("Should return an empty array of AVSs", func(t *testing.T) {
			staker := "0x130c646e1224d979ff23523308abb6012ce04b0a"
			blockNumber := uint64(3204391)

			shares, err := pds.ListStakerShares(context.Background(), staker, "", blockNumber, false, 0, 0)
			assert.Nil(t, err)
			assert.True(t, len(shares) > 0)
			for _, share := range shares {
				assert.True(t, len(share.AvsAddresses) == 0)
			}
		})
		t.Run("Should return an array of AVSs", func(t *testing.T) {
			staker := "0xbc9dec48f305167bb8ee593e44893acf65ad3f36"
			blockNumber := uint64(3240224)

			shares, err := pds.ListStakerShares(context.Background(), staker, "", blockNumber, false, 0, 0)
			assert.Nil(t, err)
			assert.True(t, len(shares) > 0)
			for _, share := range shares {
				assert.True(t, len(share.AvsAddresses) > 0)
			}
		})
	})
}
