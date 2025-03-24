// Package helpers provides utility functions for importing and managing core contracts.
package helpers

import (
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"strings"
)

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
