package migrations

import (
	_02503191547_initializeAllNewContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202503191547_initializeAllNewContracts"
	_202506181424_updateSepoliaAddresses "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202506181424_updateSepoliaAddresses"
	_202507030921_holeskyRedistribution "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507030921_holeskyRedistribution"
	_202507281314_taskMailboxContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507281314_taskMailboxContracts"
	_202507291037_crossChainRegistryContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507291037_crossChainRegistryContracts"
	_202507311116_verificationAndTableContracts "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202507311116_verificationAndTableContracts"
	_202509162116_upgradeAllocationManagerImpl "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202509162116_upgradeAllocationManagerImpl"
	_202509241754_newMainnetAddresses "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202509241754_newMainnetAddresses"
	_202509251838_updateStrategyAndDelegationImpl "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202509251838_updateStrategyAndDelegationImpl"
	_202511101200_preprodHoodiDeployment "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202511101200_preprodHoodiDeployment"
	_202511170958_preprodHoodiMultichain "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202511170958_preprodHoodiMultichain"
	_202511211117_upgradePreprodHoodi "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations/202511211117_upgradePreprodHoodi"
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
		&_202509162116_upgradeAllocationManagerImpl.ContractMigration{},
		&_202509241754_newMainnetAddresses.ContractMigration{},
		&_202509251838_updateStrategyAndDelegationImpl.ContractMigration{},
		&_202511101200_preprodHoodiDeployment.ContractMigration{},
		&_202511170958_preprodHoodiMultichain.ContractMigration{},
		&_202511211117_upgradePreprodHoodi.ContractMigration{},
	}
}
