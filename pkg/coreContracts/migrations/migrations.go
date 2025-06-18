package migrations

import (
	_202503191547_initializeAllNewContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202503191547_initializeAllNewContracts"
	_202506181424_updateSepoliaAddresses "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202506181424_updateSepoliaAddresses"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
)

func GetCoreContractMigrations() []types.ICoreContractMigration {
	return []types.ICoreContractMigration{
		&_202503191547_initializeAllNewContracts.ContractMigration{},
		&_202506181424_updateSepoliaAddresses.ContractMigration{},
	}
}
