package _202507281314_taskMailboxContracts

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/helpers"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ContractMigration struct{}

func (m *ContractMigration) Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	contractsMap := cfg.GetContractsMapForChain()
	if contractsMap == nil {
		l.Sugar().Errorw("No contracts map found for Holesky chain", zap.String("chain", cfg.Chain.String()))
		return nil, fmt.Errorf("no contracts map found for Holesky chain")
	}

	contractsToImport := &helpers.ContractsToImport{
		CoreContracts: []*helpers.ContractImport{},
		ImplementationContracts: []*helpers.ImplementationContractImport{
			{
				ProxyContractAddress: contractsMap.TaskMailbox,
				ImplementationContracts: []*helpers.ImplementationContract{
					{
						Contract: helpers.ContractImport{
							Address:      "0x0000000000000000000000000000000000000000",
							Abi:          "",
							BytecodeHash: "",
						},
					},
				},
			},
		},
	}

	createdContracts, err := helpers.ImportImplementationContracts(contractsToImport.ImplementationContracts, cs)

	return nil, nil
}

func (m *ContractMigration) GetName() string {
	return "202507281314_taskMailboxContracts"
}
