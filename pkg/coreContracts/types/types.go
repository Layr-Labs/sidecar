package types

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ICoreContractMigration interface {
	GetName() string
	// returns any new top-level core contracts that were added
	Up(db *gorm.DB, cm contractManager.ContractManager, l *zap.Logger, cfg *config.Config) (*MigrationResult, error)
}

type CoreContractAdded struct {
	Address         string
	IndexStartBlock uint64
}

type MigrationResult struct {
	CoreContractsAdded           []CoreContractAdded
	ImplementationContractsAdded []string
}
