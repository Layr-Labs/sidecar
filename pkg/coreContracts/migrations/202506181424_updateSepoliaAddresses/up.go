package _202506181424_updateSepoliaAddresses

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

type CoreContractMigration struct{}

func (m *CoreContractMigration) GetName() string {
	return "202506181424_updateSepoliaAddresses"
}

func (m *CoreContractMigration) Up(db *gorm.DB, contractStore contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	// Only run this migration for Sepolia
	if cfg.Chain != config.Chain_Sepolia {
		l.Info("Skipping Sepolia address update migration for non-Sepolia chain", zap.String("chain", string(cfg.Chain)))
		return &types.MigrationResult{
			CoreContractsAdded:           []types.CoreContractAdded{},
			ImplementationContractsAdded: []string{},
		}, nil
	}

	// Read the updated Sepolia contract data
	jsonData, err := CoreContracts.ReadFile("coreContracts/sepolia.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read sepolia contract data: %w", err)
	}

	// Unmarshal the JSON data
	var contractData helpers.CoreContractsData
	if err := json.Unmarshal(jsonData, &contractData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sepolia contract data: %w", err)
	}

	contracts := helpers.MassageContractData(&contractData)

	importedCoreContracts, err := helpers.ImportCoreContracts(contracts.CoreContracts, contractStore)
	if err != nil {
		return nil, err
	}
	l.Sugar().Infow("Imported core contracts", zap.Int("count", len(importedCoreContracts)))

	importedImplementationContracts, err := helpers.ImportImplementationContracts(contracts.ImplementationContracts, contractStore)
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
