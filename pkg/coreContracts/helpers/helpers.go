package helpers

import (
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"gorm.io/gorm"
	"strings"
)

type ContractImport struct {
	Address      string
	Abi          string
	BytecodeHash string
}

type ImplementationContract struct {
	// The implementation to create
	Contract ContractImport

	// the block number at which it was created
	BlockNumber uint64
}

type ImplementationContractImport struct {
	// top-level proxy to associate the implementation contract with
	ProxyContractAddress string

	ImplementationContracts []ImplementationContract
}

type ContractsToImport struct {
	CoreContracts           []ContractImport
	ImplementationContracts []ImplementationContractImport
}

func ImportCoreContracts(coreContracts []ContractImport, grm *gorm.DB, cm contractManager.ContractManager, cs contractStore.ContractStore) ([]string, error) {
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

func ImportImplementationContracts(
	implementationContracts []ImplementationContractImport,
	grm *gorm.DB,
	cm contractManager.ContractManager,
	cs contractStore.ContractStore,
) error {
	for _, i := range implementationContracts {
		for _, c := range i.ImplementationContracts {
			contract, _, err := cs.FindOrCreateContract(
				strings.ToLower(c.Contract.Address),
				c.Contract.Abi,
				true,
				c.Contract.BytecodeHash,
				i.ProxyContractAddress,
				true,
				contractStore.ContractType_Core,
			)
			if err != nil {
				return err
			}

			_, err = cs.SetContractCheckedForProxy(contract.ContractAddress)
			if err != nil {
				return err
			}

			_, _, err = cs.FindOrCreateProxyContract(c.BlockNumber, i.ProxyContractAddress, contract.ContractAddress)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
