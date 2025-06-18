// Package coreContracts provides functionality for managing core smart contracts.
package coreContracts

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

// Metadata represents a map of string keys to arbitrary values used to store
// additional information about core contracts.
type Metadata map[string]interface{}

// Value implements the driver.Valuer interface for database serialization.
// Returns the JSON representation of the metadata or an empty JSON object if nil.
func (m *Metadata) Value() (driver.Value, error) {
	if m == nil {
		return "{}", nil
	}
	return json.Marshal(m)
}

// Scan implements the sql.Scanner interface for database deserialization.
// Unmarshals JSON data into the Metadata map structure.
func (m *Metadata) Scan(value interface{}) error {
	if value == nil || value == "" {
		*m = Metadata{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSONB value: %v", value)
	}

	return json.Unmarshal(bytes, m)
}

// Get retrieves a value from the Metadata map by its key.
//
// Parameters:
//   - key: The key to look up in the metadata
//
// Returns:
//   - interface{}: The value associated with the key, or nil if not found
//   - bool: Whether the key was found in the map
func (m *Metadata) Get(key string) (interface{}, bool) {
	if m == nil {
		return nil, false
	}
	v, ok := (*m)[key]
	return v, ok
}

// CoreContractMigrations represents a database record of migrations that have been applied
// to core contracts.
type CoreContractMigrations struct {
	Id        uint64 `gorm:"serial"`
	Name      string
	Metadata  *Metadata `gorm:"type:jsonb"`
	CreatedAt time.Time
}

// CoreContractManager handles the initialization and management of core contracts.
type CoreContractManager struct {
	db            *gorm.DB
	globalConfig  *config.Config
	contractStore contractStore.ContractStore
	logger        *zap.Logger
}

// NewCoreContractManager creates a new instance of CoreContractManager.
//
// Parameters:
//   - db: Database connection
//   - globalConfig: Application configuration
//   - contractStore: Store for contract data
//   - logger: Logger instance
//
// Returns:
//   - *CoreContractManager: A new core contract manager instance
func NewCoreContractManager(
	db *gorm.DB,
	globalConfig *config.Config,
	contractStore contractStore.ContractStore,
	logger *zap.Logger,
) *CoreContractManager {
	return &CoreContractManager{
		db:            db,
		globalConfig:  globalConfig,
		contractStore: contractStore,
		logger:        logger,
	}
}

// getMigrationByName retrieves a core contract migration record by name.
//
// Parameters:
//   - name: The name of the migration to find
//
// Returns:
//   - *CoreContractMigrations: The migration record if found, nil if not found
//   - error: Any error that occurred during the database query
func (ccm *CoreContractManager) getMigrationByName(name string) (*CoreContractMigrations, error) {
	migration := &CoreContractMigrations{}
	res := ccm.db.Where("name = ?", name).First(migration)
	if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return nil, res.Error
	}
	if errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	return migration, nil
}

// resultToMetadata converts a MigrationResult to a Metadata structure.
//
// Parameters:
//   - res: The migration result to convert
//
// Returns:
//   - *Metadata: The converted metadata
//   - error: Any error that occurred during conversion
func resultToMetadata(res *types.MigrationResult) (*Metadata, error) {
	j, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	var m *Metadata
	err = json.Unmarshal(j, &m)
	return m, err
}

// MigrateCoreContracts applies a series of migrations to core contracts.
// It tracks which migrations have been applied in the database to avoid duplication.
//
// Parameters:
//   - migrations: A slice of migrations to apply
//
// Returns:
//   - []types.CoreContractAdded: A list of core contracts that were added and require backfilling
//   - error: Any error that occurred during migration
func (ccm *CoreContractManager) MigrateCoreContracts(migrations []types.ICoreContractMigration) ([]types.CoreContractAdded, error) {
	addedCoreContracts := make([]types.CoreContractAdded, 0)

	for _, migration := range migrations {
		m, err := ccm.getMigrationByName(migration.GetName())
		if err != nil {
			return nil, err
		}
		if m != nil {
			ccm.logger.Sugar().Infof("Migration '%s' already applied", migration.GetName())
			continue
		}
		migrationRes, err := migration.Up(ccm.db, ccm.contractStore, ccm.logger, ccm.globalConfig)
		if err != nil {
			ccm.logger.Sugar().Errorf("Failed to apply migration '%s': %v", migration.GetName(), err)
			return nil, err
		}
		var metadata *Metadata
		if migrationRes != nil {
			metadata, err = resultToMetadata(migrationRes)
			if err != nil {
				return nil, err
			}
		}

		res := ccm.db.Model(&CoreContractMigrations{}).Create(&CoreContractMigrations{
			Name:     migration.GetName(),
			Metadata: metadata,
		})
		if res.Error != nil {
			ccm.logger.Sugar().Errorf("Failed to create migration record '%s': %v", migration.GetName(), res.Error)
			return nil, res.Error
		}
		if migrationRes != nil && len(migrationRes.CoreContractsAdded) > 0 {
			for _, c := range migrationRes.CoreContractsAdded {
				if c.Backfill {
					addedCoreContracts = append(addedCoreContracts, c)
				}
			}
		}
	}
	return addedCoreContracts, nil
}
