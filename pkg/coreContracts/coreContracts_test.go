package coreContracts

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
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
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

type TestContractMigration struct{}

func (m *TestContractMigration) Up(db *gorm.DB, cm contractManager.ContractManager, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	return &types.MigrationResult{
		CoreContractsAdded: []types.CoreContractAdded{
			{
				Address:         "0x1234567890",
				IndexStartBlock: 100,
			},
		},
	}, nil
}

func (m *TestContractMigration) GetName() string {
	return "202503191547_initializeAllNewContracts"
}

func Test_CoreContractManager(t *testing.T) {
	dbname, grm, l, cfg, err := setup()
	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}

	ccm := NewCoreContractManager(grm, cfg, l)
	if ccm == nil {
		t.Fatalf("Failed to create CoreContractManager")
	}

	t.Run("Test MigrateCoreContracts", func(t *testing.T) {
		newCoreContracts, err := ccm.MigrateCoreContracts([]types.ICoreContractMigration{
			&TestContractMigration{},
		})
		assert.Nil(t, err)
		fmt.Printf("New core contracts: %v\n", newCoreContracts)
		assert.Equal(t, 1, len(newCoreContracts))
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbname, cfg, grm, l)
	})
}
