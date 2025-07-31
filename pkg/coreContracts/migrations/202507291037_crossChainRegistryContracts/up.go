package _202507291037_crossChainRegistryContracts

import (
	"fmt"
	"slices"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/helpers"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ContractMigration struct{}

func (m *ContractMigration) Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	if !slices.Contains([]config.Chain{config.Chain_Preprod, config.Chain_Sepolia}, cfg.Chain) {
		return nil, nil
	}

	contractsMap := cfg.GetContractsMapForChain()
	if contractsMap == nil {
		l.Sugar().Errorw("No contracts map found for chain", zap.String("chain", cfg.Chain.String()))
		return nil, fmt.Errorf("no contracts map found for chain: %s", cfg.Chain.String())
	}

	// Implementation addresses are different per chain
	crossChainRegistryImplementationAddresses := map[config.Chain]string{
		config.Chain_Preprod: "0xf63787a0fa1f500c67934aa1b37594106306f31d",
		config.Chain_Sepolia: "0x20715838d4cf054577cd2b89648d9cfc8994f48b",
	}

	implementationAddress, exists := crossChainRegistryImplementationAddresses[cfg.Chain]
	if !exists {
		l.Sugar().Infow("No CrossChainRegistry implementation address found for chain, skipping migration", zap.String("chain", cfg.Chain.String()))
		return nil, nil
	}

	contractsToImport := &helpers.ContractsToImport{
		CoreContracts: []*helpers.ContractImport{
			{
				Address:      contractsMap.CrossChainRegistry,
				Abi:          CrossChainRegistryABI,
				BytecodeHash: "2f5d85e59c89f9e7a88793e866c37204021c17239e22acf35ec3768fe72c56ba",
			},
		},
		ImplementationContracts: []*helpers.ImplementationContractImport{
			{
				ProxyContractAddress: contractsMap.CrossChainRegistry,
				ImplementationContracts: []*helpers.ImplementationContract{
					{
						Contract: helpers.ContractImport{
							Address:      implementationAddress,
							Abi:          ImplementationABI,
							BytecodeHash: "2f5d85e59c89f9e7a88793e866c37204021c17239e22acf35ec3768fe72c56ba",
						},
					},
				},
			},
		},
	}

	coreContracts, err := helpers.ImportCoreContracts(contractsToImport.CoreContracts, cs)
	if err != nil {
		return nil, err
	}
	l.Sugar().Infow("Imported core contracts", zap.Int("count", len(coreContracts)))

	implementationContracts, err := helpers.ImportImplementationContracts(contractsToImport.ImplementationContracts, cs, l)
	if err != nil {
		return nil, err
	}
	l.Sugar().Infow("Imported implementation contracts", zap.Int("count", len(implementationContracts)))

	return &types.MigrationResult{
		CoreContractsAdded: utils.Map(contractsToImport.CoreContracts, func(c *helpers.ContractImport, i uint64) types.CoreContractAdded {
			return types.CoreContractAdded{
				Address:         c.Address,
				IndexStartBlock: cfg.GetGenesisBlockNumber(),
				Backfill:        true,
			}
		}),
		ImplementationContractsAdded: utils.Reduce(contractsToImport.ImplementationContracts, func(acc []string, ic *helpers.ImplementationContractImport) []string {
			return append(acc, utils.Map(ic.ImplementationContracts, func(i *helpers.ImplementationContract, j uint64) string {
				return i.Contract.Address
			})...)
		}, make([]string, 0)),
	}, nil
}

func (m *ContractMigration) GetName() string {
	return "202507291037_crossChainRegistryContracts"
}

const CrossChainRegistryABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_logic\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"admin_\",\"type\":\"address\"},{\"internalType\":\"bytes\",\"name\":\"_data\",\"type\":\"bytes\"}],\"stateMutability\":\"payable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"previousAdmin\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"address\",\"name\":\"newAdmin\",\"type\":\"address\"}],\"name\":\"AdminChanged\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beacon\",\"type\":\"address\"}],\"name\":\"BeaconUpgraded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"implementation\",\"type\":\"address\"}],\"name\":\"Upgraded\",\"type\":\"event\"},{\"stateMutability\":\"payable\",\"type\":\"fallback\"},{\"stateMutability\":\"payable\",\"type\":\"receive\"}]"
const ImplementationABI = "[{\"inputs\":[{\"internalType\":\"contract IAllocationManager\",\"name\":\"_allocationManager\",\"type\":\"address\"},{\"internalType\":\"contract IKeyRegistrar\",\"name\":\"_keyRegistrar\",\"type\":\"address\"},{\"internalType\":\"contract IPermissionController\",\"name\":\"_permissionController\",\"type\":\"address\"},{\"internalType\":\"contract IPauserRegistry\",\"name\":\"_pauserRegistry\",\"type\":\"address\"},{\"internalType\":\"string\",\"name\":\"_version\",\"type\":\"string\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[],\"name\":\"ArrayLengthMismatch\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ChainIDAlreadyWhitelisted\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ChainIDNotWhitelisted\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"CurrentlyPaused\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"EmptyChainIDsArray\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"GenerationReservationAlreadyExists\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"GenerationReservationDoesNotExist\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InputAddressZero\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidChainId\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidNewPausedStatus\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidOperatorSet\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidPermissions\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidShortString\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidStalenessPeriod\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidTableUpdateCadence\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"OnlyPauser\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"OnlyUnpauser\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"string\",\"name\":\"str\",\"type\":\"string\"}],\"name\":\"StringTooLong\",\"type\":\"error\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"chainID\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"address\",\"name\":\"operatorTableUpdater\",\"type\":\"address\"}],\"name\":\"ChainIDAddedToWhitelist\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"chainID\",\"type\":\"uint256\"}],\"name\":\"ChainIDRemovedFromWhitelist\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"GenerationReservationCreated\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"GenerationReservationRemoved\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"uint8\",\"name\":\"version\",\"type\":\"uint8\"}],\"name\":\"Initialized\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"OperatorSetConfigRemoved\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"maxStalenessPeriod\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct ICrossChainRegistryTypes.OperatorSetConfig\",\"name\":\"config\",\"type\":\"tuple\"}],\"name\":\"OperatorSetConfigSet\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"OperatorTableCalculatorRemoved\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"indexed\":false,\"internalType\":\"contract IOperatorTableCalculator\",\"name\":\"operatorTableCalculator\",\"type\":\"address\"}],\"name\":\"OperatorTableCalculatorSet\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"previousOwner\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"OwnershipTransferred\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"newPausedStatus\",\"type\":\"uint256\"}],\"name\":\"Paused\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"uint32\",\"name\":\"tableUpdateCadence\",\"type\":\"uint32\"}],\"name\":\"TableUpdateCadenceSet\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"newPausedStatus\",\"type\":\"uint256\"}],\"name\":\"Unpaused\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"uint256[]\",\"name\":\"chainIDs\",\"type\":\"uint256[]\"},{\"internalType\":\"address[]\",\"name\":\"operatorTableUpdaters\",\"type\":\"address[]\"}],\"name\":\"addChainIDsToWhitelist\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"allocationManager\",\"outputs\":[{\"internalType\":\"contract IAllocationManager\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"calculateOperatorTableBytes\",\"outputs\":[{\"internalType\":\"bytes\",\"name\":\"\",\"type\":\"bytes\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"contract IOperatorTableCalculator\",\"name\":\"operatorTableCalculator\",\"type\":\"address\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"maxStalenessPeriod\",\"type\":\"uint32\"}],\"internalType\":\"struct ICrossChainRegistryTypes.OperatorSetConfig\",\"name\":\"config\",\"type\":\"tuple\"}],\"name\":\"createGenerationReservation\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getActiveGenerationReservations\",\"outputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet[]\",\"name\":\"\",\"type\":\"tuple[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"getOperatorSetConfig\",\"outputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"maxStalenessPeriod\",\"type\":\"uint32\"}],\"internalType\":\"struct ICrossChainRegistryTypes.OperatorSetConfig\",\"name\":\"\",\"type\":\"tuple\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"getOperatorTableCalculator\",\"outputs\":[{\"internalType\":\"contract IOperatorTableCalculator\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getSupportedChains\",\"outputs\":[{\"internalType\":\"uint256[]\",\"name\":\"\",\"type\":\"uint256[]\"},{\"internalType\":\"address[]\",\"name\":\"\",\"type\":\"address[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getTableUpdateCadence\",\"outputs\":[{\"internalType\":\"uint32\",\"name\":\"\",\"type\":\"uint32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"initialOwner\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"initialTableUpdateCadence\",\"type\":\"uint32\"},{\"internalType\":\"uint256\",\"name\":\"initialPausedStatus\",\"type\":\"uint256\"}],\"name\":\"initialize\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"keyRegistrar\",\"outputs\":[{\"internalType\":\"contract IKeyRegistrar\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newPausedStatus\",\"type\":\"uint256\"}],\"name\":\"pause\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"pauseAll\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint8\",\"name\":\"index\",\"type\":\"uint8\"}],\"name\":\"paused\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"paused\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"pauserRegistry\",\"outputs\":[{\"internalType\":\"contract IPauserRegistry\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"permissionController\",\"outputs\":[{\"internalType\":\"contract IPermissionController\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256[]\",\"name\":\"chainIDs\",\"type\":\"uint256[]\"}],\"name\":\"removeChainIDsFromWhitelist\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"removeGenerationReservation\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"renounceOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"maxStalenessPeriod\",\"type\":\"uint32\"}],\"internalType\":\"struct ICrossChainRegistryTypes.OperatorSetConfig\",\"name\":\"config\",\"type\":\"tuple\"}],\"name\":\"setOperatorSetConfig\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"contract IOperatorTableCalculator\",\"name\":\"operatorTableCalculator\",\"type\":\"address\"}],\"name\":\"setOperatorTableCalculator\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint32\",\"name\":\"tableUpdateCadence\",\"type\":\"uint32\"}],\"name\":\"setTableUpdateCadence\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"transferOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newPausedStatus\",\"type\":\"uint256\"}],\"name\":\"unpause\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"version\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"
