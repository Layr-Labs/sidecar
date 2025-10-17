package _202510171046_keyRegistrar

import (
	"fmt"

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
	contractsMap := cfg.GetContractsMapForChain()
	if contractsMap == nil {
		l.Sugar().Errorw("No contracts map found for chain", zap.String("chain", cfg.Chain.String()))
		return nil, fmt.Errorf("no contracts map found for chain: %s", cfg.Chain.String())
	}

	keyRegistrarImplementationAddresses := map[config.Chain]string{
		config.Chain_Preprod: "0xa491972d3b9ba3da8a4e5a44f84d3d5b0268375e",
		config.Chain_Holesky: "0xb836a75529c8bce8542b237d11a4ba174ce567a5",
		config.Chain_Sepolia: "0xd3b7f85e3f289c601fb808b0d7d366e2dfff21c3",
		config.Chain_Hoodi:   "0x345d8b662dd7e8655a80420cc6e2e8e106875c3b",
		config.Chain_Mainnet: "0x047bec3d8c19d70ba81d61a48bf9dc63a3e9136b",
	}

	implementationAddress, exists := keyRegistrarImplementationAddresses[cfg.Chain]
	if !exists {
		l.Sugar().Infow("No KeyRegistrar implementation address found for chain, skipping migration", zap.String("chain", cfg.Chain.String()))
		return nil, nil
	}

	contractsToImport := &helpers.ContractsToImport{
		CoreContracts: []*helpers.ContractImport{
			{
				Address:      contractsMap.KeyRegistrar,
				Abi:          KeyRegistrarABI,
				BytecodeHash: "79eee62b15dafce1e71900b12be7a530c557d8bea08f90c8c7d85dc272d08ef2",
			},
		},
		ImplementationContracts: []*helpers.ImplementationContractImport{
			{
				ProxyContractAddress: contractsMap.KeyRegistrar,
				ImplementationContracts: []*helpers.ImplementationContract{
					{
						Contract: helpers.ContractImport{
							Address:      implementationAddress,
							Abi:          ImplementationABI,
							BytecodeHash: "79eee62b15dafce1e71900b12be7a530c557d8bea08f90c8c7d85dc272d08ef2",
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
	return "202510171046_keyRegistrar"
}

const KeyRegistrarABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_logic\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"admin_\",\"type\":\"address\"},{\"internalType\":\"bytes\",\"name\":\"_data\",\"type\":\"bytes\"}],\"stateMutability\":\"payable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"previousAdmin\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"address\",\"name\":\"newAdmin\",\"type\":\"address\"}],\"name\":\"AdminChanged\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beacon\",\"type\":\"address\"}],\"name\":\"BeaconUpgraded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"implementation\",\"type\":\"address\"}],\"name\":\"Upgraded\",\"type\":\"event\"},{\"stateMutability\":\"payable\",\"type\":\"fallback\"},{\"stateMutability\":\"payable\",\"type\":\"receive\"}]"
const ImplementationABI = "[{\"inputs\":[{\"internalType\":\"contract IPermissionController\",\"name\":\"_permissionController\",\"type\":\"address\"},{\"internalType\":\"contract IAllocationManager\",\"name\":\"_allocationManager\",\"type\":\"address\"},{\"internalType\":\"string\",\"name\":\"_version\",\"type\":\"string\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[],\"name\":\"ConfigurationAlreadySet\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ECAddFailed\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ECMulFailed\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ECPairingFailed\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ExpModFailed\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidCurveType\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidKeyFormat\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidKeypair\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidPermissions\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidShortString\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"InvalidSignature\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"KeyAlreadyRegistered\",\"type\":\"error\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"KeyNotFound\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"OperatorAlreadyRegistered\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"OperatorSetNotConfigured\",\"type\":\"error\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"OperatorStillSlashable\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"SignatureExpired\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"string\",\"name\":\"str\",\"type\":\"string\"}],\"name\":\"StringTooLong\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ZeroAddress\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ZeroPubkey\",\"type\":\"error\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"X\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"Y\",\"type\":\"uint256\"}],\"indexed\":false,\"internalType\":\"struct BN254.G1Point\",\"name\":\"newAggregateKey\",\"type\":\"tuple\"}],\"name\":\"AggregateBN254KeyUpdated\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"enum IKeyRegistrarTypes.CurveType\",\"name\":\"curveType\",\"type\":\"uint8\"}],\"name\":\"KeyDeregistered\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"enum IKeyRegistrarTypes.CurveType\",\"name\":\"curveType\",\"type\":\"uint8\"},{\"indexed\":false,\"internalType\":\"bytes\",\"name\":\"pubkey\",\"type\":\"bytes\"}],\"name\":\"KeyRegistered\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"indexed\":false,\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"indexed\":false,\"internalType\":\"enum IKeyRegistrarTypes.CurveType\",\"name\":\"curveType\",\"type\":\"uint8\"}],\"name\":\"OperatorSetConfigured\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"BN254_KEY_REGISTRATION_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"ECDSA_KEY_REGISTRATION_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"allocationManager\",\"outputs\":[{\"internalType\":\"contract IAllocationManager\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"enum IKeyRegistrarTypes.CurveType\",\"name\":\"curveType\",\"type\":\"uint8\"}],\"name\":\"configureOperatorSet\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"deregisterKey\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"domainSeparator\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"X\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"Y\",\"type\":\"uint256\"}],\"internalType\":\"struct BN254.G1Point\",\"name\":\"g1Point\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256[2]\",\"name\":\"X\",\"type\":\"uint256[2]\"},{\"internalType\":\"uint256[2]\",\"name\":\"Y\",\"type\":\"uint256[2]\"}],\"internalType\":\"struct BN254.G2Point\",\"name\":\"g2Point\",\"type\":\"tuple\"}],\"name\":\"encodeBN254KeyData\",\"outputs\":[{\"internalType\":\"bytes\",\"name\":\"\",\"type\":\"bytes\"}],\"stateMutability\":\"pure\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"getBN254Key\",\"outputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"X\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"Y\",\"type\":\"uint256\"}],\"internalType\":\"struct BN254.G1Point\",\"name\":\"g1Point\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256[2]\",\"name\":\"X\",\"type\":\"uint256[2]\"},{\"internalType\":\"uint256[2]\",\"name\":\"Y\",\"type\":\"uint256[2]\"}],\"internalType\":\"struct BN254.G2Point\",\"name\":\"g2Point\",\"type\":\"tuple\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"bytes\",\"name\":\"keyData\",\"type\":\"bytes\"}],\"name\":\"getBN254KeyRegistrationMessageHash\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"getECDSAAddress\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"getECDSAKey\",\"outputs\":[{\"internalType\":\"bytes\",\"name\":\"\",\"type\":\"bytes\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"keyAddress\",\"type\":\"address\"}],\"name\":\"getECDSAKeyRegistrationMessageHash\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"getKeyHash\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"bytes\",\"name\":\"keyData\",\"type\":\"bytes\"}],\"name\":\"getOperatorFromSigningKey\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"},{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"}],\"name\":\"getOperatorSetCurveType\",\"outputs\":[{\"internalType\":\"enum IKeyRegistrarTypes.CurveType\",\"name\":\"\",\"type\":\"uint8\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"keyHash\",\"type\":\"bytes32\"}],\"name\":\"isKeyGloballyRegistered\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"}],\"name\":\"isRegistered\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"permissionController\",\"outputs\":[{\"internalType\":\"contract IPermissionController\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"operator\",\"type\":\"address\"},{\"components\":[{\"internalType\":\"address\",\"name\":\"avs\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"id\",\"type\":\"uint32\"}],\"internalType\":\"struct OperatorSet\",\"name\":\"operatorSet\",\"type\":\"tuple\"},{\"internalType\":\"bytes\",\"name\":\"keyData\",\"type\":\"bytes\"},{\"internalType\":\"bytes\",\"name\":\"signature\",\"type\":\"bytes\"}],\"name\":\"registerKey\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"version\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"
