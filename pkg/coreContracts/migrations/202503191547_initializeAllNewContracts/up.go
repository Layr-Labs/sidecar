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

func loadContractData(globalConfig *config.Config) (*helpers.CoreContractsData, error) {
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
	data := &helpers.CoreContractsData{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode core contracts data: %w", err)
	}
	return data, nil
}

type ContractMigration struct{}

func (m *ContractMigration) Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	data, err := loadContractData(cfg)
	if err != nil {
		return nil, err
	}

	contracts := helpers.MassageContractData(data)

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

	importedImplementationContracts, err := helpers.ImportImplementationContracts(contracts.ImplementationContracts, cs)
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
