package slashingDataService

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"log"
	"os"
	"testing"
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

func Test_SlashingDataService(t *testing.T) {
	if !tests.LargeTestsEnabled() {
		t.Skipf("Skipping large test")
		return
	}

	grm, l, cfg, err := setup("preprodSlim")

	t.Logf("Using database with name: %s", cfg.DatabaseConfig.DbName)

	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}

	sds := NewSlashingDataService(grm, l, cfg)

	t.Run("Test ListStakerSlashingHistory", func(t *testing.T) {
		staker := "0x9c8154b9c2486bc85daada3853e57cd4fa7e4307"

		history, err := sds.ListStakerSlashingHistory(context.Background(), staker, 2943099)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(history))
		assert.Equal(t, uint64(2942800), history[0].BlockNumber)
		assert.Equal(t, 1, len(history[0].Strategies))
		assert.Equal(t, "-12053572999999999", history[0].Strategies[0].TotalSharesSlashed)
		assert.Equal(t, "0x69aa865947f6c9191b02954b1dd1a44131541226", history[0].OperatorSet.Avs)
		assert.Equal(t, uint64(5), history[0].OperatorSet.Id)
	})

	t.Run("Test ListOperatorSlashingHistory", func(t *testing.T) {
		operator := "0x6355ea4fb1fca0c794950fff5efd6e2ee28acc14"

		history, err := sds.ListOperatorSlashingHistory(context.Background(), operator, 2943099)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(history))
	})

	t.Run("Test ListAvsSlashingHistory", func(t *testing.T) {
		avs := "0x69aa865947f6c9191b02954b1dd1a44131541226"

		history, err := sds.ListAvsSlashingHistory(context.Background(), avs, 2943099)
		assert.Nil(t, err)
		assert.Equal(t, 8, len(history))
	})

	t.Run("Test ListAvsOperatorSetSlashingHistory", func(t *testing.T) {
		avs := "0x69aa865947f6c9191b02954b1dd1a44131541226"
		operatorSetId := uint64(5)

		history, err := sds.ListAvsOperatorSetSlashingHistory(context.Background(), avs, operatorSetId, 2943099)
		assert.Nil(t, err)
		assert.Equal(t, 8, len(history))
	})

}
