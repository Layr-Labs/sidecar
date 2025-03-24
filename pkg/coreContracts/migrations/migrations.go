package migrations

import (
	_02503191547_initializeAllNewContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202503191547_initializeAllNewContracts"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
)

func GetCoreContractMigrations() []types.ICoreContractMigration {
	return []types.ICoreContractMigration{
		&_02503191547_initializeAllNewContracts.ContractMigration{},
	}
}
