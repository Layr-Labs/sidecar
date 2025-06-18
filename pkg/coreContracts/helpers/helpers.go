// Package helpers provides utility functions for importing and managing core contracts.
package helpers

import (
	"strings"

	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/utils"
)

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

// ContractImport represents a contract to be imported into the system.
type ContractImport struct {
	// Address is the Ethereum address of the contract
	Address string

	// Abi is the Application Binary Interface (ABI) of the contract
	Abi string

	// BytecodeHash is the hash of the contract's bytecode
	BytecodeHash string
}

// ImplementationContract represents an implementation contract for a proxy.
type ImplementationContract struct {
	// Contract is the implementation contract to create
	Contract ContractImport

	// BlockNumber is the block number at which the implementation was created
	BlockNumber uint64
}

// ImplementationContractImport represents a set of implementation contracts
// associated with a proxy contract.
type ImplementationContractImport struct {
	// ProxyContractAddress is the address of the top-level proxy contract
	ProxyContractAddress string

	// ImplementationContracts is a list of implementation contracts for the proxy
	ImplementationContracts []*ImplementationContract
}

// ContractsToImport represents a collection of contracts to be imported into the system.
type ContractsToImport struct {
	// CoreContracts is a list of top-level core contracts to import
	CoreContracts []*ContractImport

	// ImplementationContracts is a list of implementation contracts and their proxies
	ImplementationContracts []*ImplementationContractImport
}

// ImportCoreContracts imports a list of core contracts into the contract store.
//
// Parameters:
//   - coreContracts: A list of core contracts to import
//   - cs: The contract store to import into
//
// Returns:
//   - []string: A list of addresses of contracts that were created
//   - error: Any error that occurred during import
func ImportCoreContracts(coreContracts []*ContractImport, cs contractStore.ContractStore) ([]string, error) {
	createdContracts := make([]string, 0)
	for _, c := range coreContracts {
		contract, found, err := cs.FindOrCreateContract(
			strings.ToLower(c.Address),
			c.Abi,
			true,
			c.BytecodeHash,
			"",
			true,
			contractStore.ContractType_Core,
		)
		if err != nil {
			return nil, err
		}
		if found {
			continue
		}
		createdContracts = append(createdContracts, contract.ContractAddress)
	}
	return createdContracts, nil
}

// ImportImplementationContracts imports a list of implementation contracts and associates them
// with their respective proxy contracts.
//
// Parameters:
//   - implementationContracts: A list of implementation contracts to import
//   - cs: The contract store to import into
//
// Returns:
//   - []string: A list of addresses of implementation contracts that were imported
//   - error: Any error that occurred during import
func ImportImplementationContracts(
	implementationContracts []*ImplementationContractImport,
	cs contractStore.ContractStore,
) ([]string, error) {
	importedImplementationContracts := make([]string, 0)
	for _, i := range implementationContracts {
		for _, c := range i.ImplementationContracts {
			contract, found, err := cs.FindOrCreateContract(
				strings.ToLower(c.Contract.Address),
				c.Contract.Abi,
				true,
				c.Contract.BytecodeHash,
				i.ProxyContractAddress,
				true,
				contractStore.ContractType_Core,
			)
			if err != nil {
				return nil, err
			}
			if !found {
				importedImplementationContracts = append(importedImplementationContracts, contract.ContractAddress)
			}

			_, err = cs.SetContractCheckedForProxy(contract.ContractAddress)
			if err != nil {
				return nil, err
			}

			_, _, err = cs.FindOrCreateProxyContract(c.BlockNumber, i.ProxyContractAddress, contract.ContractAddress)
			if err != nil {
				return nil, err
			}
		}
	}
	return importedImplementationContracts, nil
}

type SingleImplementationContract struct {
	ProxyContractAddress   string
	ImplementationContract *ImplementationContract
}

func MassageContractData(data *CoreContractsData) *ContractsToImport {
	proxyContracts := make(map[string]*ContractImport)

	// implementationAddress -> SingleImplementationContract & proxy
	implementationContracts := make(map[string]*SingleImplementationContract, 0)

	for _, p := range data.ProxyContracts {
		// memoize all core proxy contracts
		// always take the most recent one if there are duplicates (which there arent)
		proxyContracts[p.ContractAddress] = &ContractImport{
			Address:      p.ContractAddress,
			Abi:          "",
			BytecodeHash: "",
		}

		// memoize all implementation contracts
		// always take the most recent one if there are duplicates (which there are)
		implementationContracts[p.ProxyContractAddress] = &SingleImplementationContract{
			ProxyContractAddress: p.ContractAddress,
			ImplementationContract: &ImplementationContract{
				Contract: ContractImport{
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
	return &ContractsToImport{
		CoreContracts: utils.Values(proxyContracts),
		ImplementationContracts: utils.Map(utils.Values(proxyContracts), func(cc *ContractImport, i uint64) *ImplementationContractImport {
			return &ImplementationContractImport{
				ProxyContractAddress: cc.Address,
				ImplementationContracts: utils.Reduce(utils.Values(implementationContracts), func(acc []*ImplementationContract, ic *SingleImplementationContract) []*ImplementationContract {
					if ic.ProxyContractAddress == cc.Address {
						return append(acc, ic.ImplementationContract)
					}
					return acc
				}, make([]*ImplementationContract, 0)),
			}
		}),
	}
}
