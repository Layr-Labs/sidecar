package types

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ICoreContractMigration interface {
	GetName() string
	// returns any new top-level core contracts that were added
	Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*MigrationResult, error)
}

type CoreContractAdded struct {
	Address         string
	IndexStartBlock uint64
	Backfill        bool
}

type MigrationResult struct {
	CoreContractsAdded           []CoreContractAdded
	ImplementationContractsAdded []string
}
