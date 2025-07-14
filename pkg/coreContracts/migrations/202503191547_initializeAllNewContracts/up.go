package _02503191547_initializeAllNewContracts

import (
	"embed"
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/helpers"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

//go:embed coreContracts
var CoreContracts embed.FS

type CoreContract struct {
	ContractAddress string `json:"contract_address"`
	ContractAbi     string `json:"contract_abi"`
	BytecodeHash    string `json:"bytecode_hash"`
}

type CoreProxyContract struct {
	ContractAddress      string `json:"contract_address"`
	ProxyContractAddress string `json:"proxy_contract_address"`
	BlockNumber          uint64 `json:"block_number"`
}

type CoreContractsData struct {
	CoreContracts  []CoreContract      `json:"core_contracts"`
	ProxyContracts []CoreProxyContract `json:"proxy_contracts"`
}

func loadContractData(globalConfig *config.Config) (*CoreContractsData, error) {
	var filename string
	switch globalConfig.Chain {
	case config.Chain_Mainnet:
		filename = "mainnet.json"
	case config.Chain_Holesky:
		filename = "testnet.json"
	case config.Chain_Preprod:
		filename = "preprod.json"
	case config.Chain_Sepolia:
		filename = "sepolia.json"
	case config.Chain_Hoodi:
		filename = "hoodi.json"
	default:
		return nil, fmt.Errorf("unknown environment")
	}
	jsonData, err := CoreContracts.ReadFile(fmt.Sprintf("coreContracts/%s", filename))
	if err != nil {
		return nil, fmt.Errorf("failed to open core contracts file: %w", err)
	}

	// read entire file and marshal it into a CoreContractsData struct
	data := &CoreContractsData{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode core contracts data: %w", err)
	}
	return data, nil
}

type SingleImplementationContract struct {
	ProxyContractAddress   string
	ImplementationContract *helpers.ImplementationContract
}

func massageContractData(data *CoreContractsData) *helpers.ContractsToImport {
	proxyContracts := make(map[string]*helpers.ContractImport)

	// implementationAddress -> SingleImplementationContract & proxy
	implementationContracts := make(map[string]*SingleImplementationContract, 0)

	for _, p := range data.ProxyContracts {
		// memoize all core proxy contracts
		// always take the most recent one if there are duplicates (which there arent)
		proxyContracts[p.ContractAddress] = &helpers.ContractImport{
			Address:      p.ContractAddress,
			Abi:          "",
			BytecodeHash: "",
		}

		// memoize all implementation contracts
		// always take the most recent one if there are duplicates (which there are)
		implementationContracts[p.ProxyContractAddress] = &SingleImplementationContract{
			ProxyContractAddress: p.ContractAddress,
			ImplementationContract: &helpers.ImplementationContract{
				Contract: helpers.ContractImport{
					Address:      p.ProxyContractAddress,
					Abi:          "",
					BytecodeHash: "",
				},
				BlockNumber: p.BlockNumber,
			},
		}
	}

	for _, c := range data.CoreContracts {
		// take each contract abi and pair it to the correct core or implementation contract
		if _, ok := proxyContracts[c.ContractAddress]; ok {
			proxyContracts[c.ContractAddress].Abi = c.ContractAbi
			proxyContracts[c.ContractAddress].BytecodeHash = c.BytecodeHash
			continue
		}
		if _, ok := implementationContracts[c.ContractAddress]; ok {
			implementationContracts[c.ContractAddress].ImplementationContract.Contract.Abi = c.ContractAbi
			implementationContracts[c.ContractAddress].ImplementationContract.Contract.BytecodeHash = c.BytecodeHash
			continue
		}
	}

	// compile everything into the expected results
	return &helpers.ContractsToImport{
		CoreContracts: utils.Values(proxyContracts),
		ImplementationContracts: utils.Map(utils.Values(proxyContracts), func(cc *helpers.ContractImport, i uint64) *helpers.ImplementationContractImport {
			return &helpers.ImplementationContractImport{
				ProxyContractAddress: cc.Address,
				ImplementationContracts: utils.Reduce(utils.Values(implementationContracts), func(acc []*helpers.ImplementationContract, ic *SingleImplementationContract) []*helpers.ImplementationContract {
					if ic.ProxyContractAddress == cc.Address {
						return append(acc, ic.ImplementationContract)
					}
					return acc
				}, make([]*helpers.ImplementationContract, 0)),
			}
		}),
	}
}

type ContractMigration struct{}

func (m *ContractMigration) Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	data, err := loadContractData(cfg)
	if err != nil {
		return nil, err
	}

	contracts := massageContractData(data)

	for _, c := range contracts.CoreContracts {
		l.Sugar().Debugw("Core contract", zap.String("address", c.Address))
	}
	for _, c := range contracts.ImplementationContracts {
		l.Sugar().Debugw("Implementation contract", zap.String("proxy", c.ProxyContractAddress))
		for _, ic := range c.ImplementationContracts {
			l.Sugar().Debugw("  - Implementation contract", zap.String("address", ic.Contract.Address))
		}
	}

	importedCoreContracts, err := helpers.ImportCoreContracts(contracts.CoreContracts, cs)
	if err != nil {
		return nil, err
	}
	l.Sugar().Infow("Imported core contracts", zap.Int("count", len(importedCoreContracts)))

	importedImplementationContracts, err := helpers.ImportImplementationContracts(contracts.ImplementationContracts, cs, l)
	if err != nil {
		return nil, err
	}

	l.Sugar().Infow("Imported implementation contracts", zap.Int("count", len(importedImplementationContracts)))

	return &types.MigrationResult{
		CoreContractsAdded: utils.Map(contracts.CoreContracts, func(c *helpers.ContractImport, i uint64) types.CoreContractAdded {
			return types.CoreContractAdded{
				Address:         c.Address,
				IndexStartBlock: 0,
				Backfill:        false,
			}
		}),
		ImplementationContractsAdded: utils.Reduce(contracts.ImplementationContracts, func(acc []string, ic *helpers.ImplementationContractImport) []string {
			return append(acc, utils.Map(ic.ImplementationContracts, func(i *helpers.ImplementationContract, j uint64) string {
				return i.Contract.Address
			})...)
		}, make([]string, 0)),
	}, nil
}

func (m *ContractMigration) GetName() string {
	return "202503191547_initializeAllNewContracts"
}
