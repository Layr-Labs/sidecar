// Package types defines interfaces and types used for core contract migrations.
package types

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ICoreContractMigration defines the interface for core contract migrations.
// Implementations of this interface can be used to add or update core contracts
// in the system.
type ICoreContractMigration interface {
	// GetName returns the unique name of the migration
	GetName() string

	// Up applies the migration to the system, adding or updating core contracts.
	// Returns any new top-level core contracts that were added
	Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*MigrationResult, error)
}

// CoreContractAdded represents a core contract that was added during a migration.
type CoreContractAdded struct {
	// Address is the Ethereum address of the added contract
	Address string

	// IndexStartBlock is the block number from which to start indexing events
	IndexStartBlock uint64

	// Backfill indicates whether the contract's events should be backfilled
	Backfill bool
}

// MigrationResult contains information about the contracts that were added
// during a migration.
type MigrationResult struct {
	// CoreContractsAdded is a list of top-level core contracts that were added
	CoreContractsAdded []CoreContractAdded

	// ImplementationContractsAdded is a list of addresses of implementation contracts
	// that were added
	ImplementationContractsAdded []string
}
