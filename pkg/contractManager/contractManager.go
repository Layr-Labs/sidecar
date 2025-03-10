// Package contractManager provides functionality for managing Ethereum smart contracts,
// including handling contract proxies, upgrades, and ABI fetching.
package contractManager

import (
	"context"
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.uber.org/zap"
)

// ContractManager handles operations related to smart contracts, including
// retrieving contract information, handling contract upgrades, and managing
// proxy contracts.
type ContractManager struct {
	// ContractStore provides storage and retrieval of contract data
	ContractStore contractStore.ContractStore
	// EthereumClient is used to interact with the Ethereum blockchain
	EthereumClient *ethereum.Client
	// AbiFetcher is used to fetch contract ABIs
	AbiFetcher *abiFetcher.AbiFetcher
	// metricsSink collects metrics about contract operations
	metricsSink *metrics.MetricsSink
	// Logger is used for logging contract operations
	Logger *zap.Logger
}

// NewContractManager creates a new ContractManager instance with the provided dependencies.
func NewContractManager(
	cs contractStore.ContractStore,
	e *ethereum.Client,
	af *abiFetcher.AbiFetcher,
	ms *metrics.MetricsSink,
	l *zap.Logger,
) *ContractManager {
	return &ContractManager{
		ContractStore:  cs,
		EthereumClient: e,
		AbiFetcher:     af,
		metricsSink:    ms,
		Logger:         l,
	}
}

// GetContractWithProxy retrieves a contract and its associated proxy contract (if any)
// for the given contract address at the specified block number.
// It returns a ContractsTree containing the contract and proxy information.
func (cm *ContractManager) GetContractWithProxy(
	contractAddress string,
	blockNumber uint64,
) (*contractStore.ContractsTree, error) {
	cm.Logger.Sugar().Debugw(fmt.Sprintf("Getting contract for address '%s'", contractAddress))

	contract, err := cm.ContractStore.GetContractWithProxyContract(contractAddress, blockNumber)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to get contract for address", zap.Error(err), zap.String("contractAddress", contractAddress))
		return nil, err
	}

	return contract, nil
}

// HandleContractUpgrade processes an Upgraded contract log event and updates the database
// with the new implementation address. It handles both EIP-1967 compliant upgrades and
// custom upgrade patterns by checking event arguments and storage slots.
// Returns an error if the upgrade cannot be processed.
func (cm *ContractManager) HandleContractUpgrade(ctx context.Context, blockNumber uint64, upgradedLog *parser.DecodedLog) error {
	// the new address that the contract points to
	newProxiedAddress := ""

	// Check the arguments for the new address. EIP-1967 contracts include this as an argument.
	// Otherwise, we'll check the storage slot
	for _, arg := range upgradedLog.Arguments {
		if arg.Name == "implementation" && arg.Value != "" && arg.Value != nil {
			newProxiedAddress = arg.Value.(common.Address).String()
			break
		}
	}

	if newProxiedAddress == "" {
		// check the storage slot at the provided block number of the transaction
		storageValue, err := cm.EthereumClient.GetStorageAt(ctx, upgradedLog.Address, ethereum.EIP1967_STORAGE_SLOT, hexutil.EncodeUint64(blockNumber))
		if err != nil || storageValue == "" {
			cm.Logger.Sugar().Errorw("Failed to get storage value",
				zap.Error(err),
				zap.Uint64("block", blockNumber),
				zap.String("upgradedLogAddress", upgradedLog.Address),
			)
			return err
		}
		if len(storageValue) != 66 {
			cm.Logger.Sugar().Errorw("Invalid storage value",
				zap.Uint64("block", blockNumber),
				zap.String("storageValue", storageValue),
			)
			return err
		}

		newProxiedAddress = "0x" + storageValue[26:]
	}

	if newProxiedAddress == "" {
		cm.Logger.Sugar().Debugw("No new proxied address found", zap.String("address", upgradedLog.Address))
		return fmt.Errorf("no new proxied address found for %s during the 'Upgraded' event", upgradedLog.Address)
	}

	err := cm.CreateUpgradedProxyContract(ctx, blockNumber, upgradedLog.Address, newProxiedAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create proxy contract", zap.Error(err))
		return err
	}
	cm.Logger.Sugar().Infow("Upgraded proxy contract", zap.String("contractAddress", upgradedLog.Address), zap.String("proxyContractAddress", newProxiedAddress))
	return nil
}

// CreateUpgradedProxyContract creates a new proxy contract relationship in the database.
// It creates entries for both the proxy contract and the implementation contract,
// fetching the ABI for the implementation contract.
// If the proxy contract already exists, it returns without error.
func (cm *ContractManager) CreateUpgradedProxyContract(
	ctx context.Context,
	blockNumber uint64,
	contractAddress string,
	proxyContractAddress string,
) error {
	// Check if proxy contract already exists
	proxyContract, _ := cm.ContractStore.GetProxyContractForAddress(blockNumber, contractAddress)
	if proxyContract != nil {
		cm.Logger.Sugar().Debugw("Found existing proxy contract when trying to create one",
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil
	}

	// Create a proxy contract
	_, err := cm.ContractStore.CreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create proxy contract",
			zap.Error(err),
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return err
	}

	// Fetch ABIs
	bytecodeHash, abi, err := cm.AbiFetcher.FetchContractDetails(ctx, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to fetch metadata from proxy contract",
			zap.Error(err),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return err
	}

	// Create contract
	_, err = cm.ContractStore.CreateContract(
		proxyContractAddress,
		abi,
		true,
		bytecodeHash,
		"",
		true,
	)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create new contract for proxy contract",
			zap.Error(err),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return err
	}
	cm.Logger.Sugar().Debugf("Created new contract for proxy contract", zap.String("proxyContractAddress", proxyContractAddress))

	return nil
}
