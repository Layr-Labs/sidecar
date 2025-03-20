package _02503191547_initializeAllNewContracts

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ContractMigration struct{}

func (m *ContractMigration) Up(db *gorm.DB, cm contractManager.ContractManager, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	return nil, nil
}

func (m *ContractMigration) GetName() string {
	return "202503191547_initializeAllNewContracts"
}
