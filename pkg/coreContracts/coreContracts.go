package coreContracts

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

type CoreContractManager struct {
	db              *gorm.DB
	globalConfig    *config.Config
	contractManager contractManager.ContractManager
	logger          *zap.Logger
}

type Metadata map[string]interface{}

func (m *Metadata) Value() (driver.Value, error) {
	if m == nil {
		return "{}", nil
	}
	return json.Marshal(m)
}

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

type CoreContractMigrations struct {
	Id        uint64 `gorm:"serial"`
	Name      string
	Metadata  *Metadata `gorm:"type:jsonb"`
	CreatedAt time.Time
}

func NewCoreContractManager(
	db *gorm.DB,
	globalConfig *config.Config,
	logger *zap.Logger,
) *CoreContractManager {
	return &CoreContractManager{
		db:           db,
		globalConfig: globalConfig,
		logger:       logger,
	}
}

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

func resultToMetadata(res *types.MigrationResult) (*Metadata, error) {
	j, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	var m *Metadata
	err = json.Unmarshal(j, &m)
	return m, err
}

func (ccm *CoreContractManager) MigrateCoreContracts(migrations []types.ICoreContractMigration) ([]types.CoreContractAdded, error) {
	addedCoreContracts := make([]types.CoreContractAdded, 0)

	for _, migration := range migrations {
		m, err := ccm.getMigrationByName(migration.GetName())
		if err != nil {
			return nil, err
		}
		if m != nil {
			ccm.logger.Sugar().Infof("Migration '%s' already applied", migration.GetName())
		}
		migrationRes, err := migration.Up(ccm.db, ccm.contractManager, ccm.logger, ccm.globalConfig)
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

		fmt.Printf("Metadata: %+v\n", metadata)
		res := ccm.db.Model(&CoreContractMigrations{}).Create(&CoreContractMigrations{
			Name:     migration.GetName(),
			Metadata: metadata,
		})
		if res.Error != nil {
			ccm.logger.Sugar().Errorf("Failed to create migration record '%s': %v", migration.GetName(), res.Error)
			return nil, res.Error
		}
		if migrationRes != nil && len(migrationRes.CoreContractsAdded) > 0 {
			addedCoreContracts = append(addedCoreContracts, migrationRes.CoreContractsAdded...)
		}
	}
	return addedCoreContracts, nil
}
