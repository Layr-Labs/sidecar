package coreContracts

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	_02503191547_initializeAllNewContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202503191547_initializeAllNewContracts"
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
	contractStore.ContractStore,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	cs := postgresContractStore.NewPostgresContractStore(grm, l, cfg)

	return dbname, grm, l, cfg, cs, nil
}

type TestContractMigration struct{}

func (m *TestContractMigration) Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	return &types.MigrationResult{
		CoreContractsAdded: []types.CoreContractAdded{
			{
				Address:         "0x1234567890",
				IndexStartBlock: 100,
				Backfill:        true,
			},
		},
	}, nil
}

func (m *TestContractMigration) GetName() string {
	return "testContractMigration"
}

func Test_CoreContractManager(t *testing.T) {
	dbname, grm, l, cfg, cs, err := setup()
	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}

	ccm := NewCoreContractManager(grm, cfg, cs, l)
	if ccm == nil {
		t.Fatalf("Failed to create CoreContractManager")
	}

	t.Run("Test MigrateCoreContracts", func(t *testing.T) {
		newCoreContracts, err := ccm.MigrateCoreContracts([]types.ICoreContractMigration{
			&TestContractMigration{},
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(newCoreContracts))
	})

	t.Run("Test migration for existing mainnet contracts", func(t *testing.T) {
		migration := &_02503191547_initializeAllNewContracts.ContractMigration{}
		newCoreContracts, err := ccm.MigrateCoreContracts([]types.ICoreContractMigration{
			migration,
		})
		assert.Nil(t, err)
		assert.Equal(t, 0, len(newCoreContracts))

		var migrationRecord *CoreContractMigrations
		res := grm.Debug().Model(&CoreContractMigrations{}).Where("name = ?", migration.GetName()).First(&migrationRecord)
		assert.Nil(t, res.Error)

		coreContractsAdded, _ := migrationRecord.Metadata.Get("CoreContractsAdded")
		assert.Equal(t, 5, len(coreContractsAdded.([]interface{})))

		implementationContractsAdded, _ := migrationRecord.Metadata.Get("ImplementationContractsAdded")
		assert.Equal(t, 11, len(implementationContractsAdded.([]interface{})))

	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbname, cfg, grm, l)
	})
}
