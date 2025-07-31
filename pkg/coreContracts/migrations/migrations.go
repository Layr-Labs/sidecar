package migrations

import (
	_02503191547_initializeAllNewContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202503191547_initializeAllNewContracts"
	_202506181424_updateSepoliaAddresses "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202506181424_updateSepoliaAddresses"
	_202507030921_holeskyRedistribution "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507030921_holeskyRedistribution"
	_202507281314_taskMailboxContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507281314_taskMailboxContracts"
	_202507291037_crossChainRegistryContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507291037_crossChainRegistryContracts"
	_202507311116_verificationAndTableContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507311116_verificationAndTableContracts"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
)

func GetCoreContractMigrations() []types.ICoreContractMigration {
	return []types.ICoreContractMigration{
		&_02503191547_initializeAllNewContracts.ContractMigration{},
		&_202506181424_updateSepoliaAddresses.ContractMigration{},
		&_202507030921_holeskyRedistribution.ContractMigration{},
		&_202507291037_crossChainRegistryContracts.ContractMigration{},
		&_202507281314_taskMailboxContracts.ContractMigration{},
		&_202507311116_verificationAndTableContracts.ContractMigration{},
	}
}
