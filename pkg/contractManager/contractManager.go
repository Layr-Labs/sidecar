// Package contractManager provides functionality for managing Ethereum smart contracts,
// including handling contract proxies, upgrades, and ABI fetching.
package contractManager

import (
	"context"
	"fmt"

	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ContractLoadParams contains all the parameters needed to load a contract
type ContractLoadParams struct {
	Address          string
	Abi              string
	BytecodeHash     string
	BlockNumber      uint64
	AssociateToProxy string
}

// ContractManager handles operations related to smart contracts, including
// retrieving contract information, handling contract upgrades, and managing
// proxy contracts.
type ContractManager struct {
	Db *gorm.DB
	// ContractStore provides storage and retrieval of contract data
	ContractStore contractStore.ContractStore
	// EthereumClient is used to interact with the Ethereum blockchain
	EthereumClient *ethereum.Client
	// AbiFetcher is used to fetch contract ABIs
	AbiFetcher *abiFetcher.AbiFetcher
	// Logger is used for logging contract operations
	Logger *zap.Logger
}

// NewContractManager creates a new ContractManager instance with the provided dependencies.
func NewContractManager(
	db *gorm.DB,
	cs contractStore.ContractStore,
	e *ethereum.Client,
	af *abiFetcher.AbiFetcher,
	l *zap.Logger,
) *ContractManager {
	return &ContractManager{
		Db:             db,
		ContractStore:  cs,
		EthereumClient: e,
		AbiFetcher:     af,
		Logger:         l,
	}
}

func (cm *ContractManager) GetContractWithImplementations(contractAddress string) ([]*contractStore.Contract, error) {
	return cm.ContractStore.GetProxyContractWithImplementations(contractAddress)
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
	_, found, err := cm.ContractStore.FindOrCreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create proxy contract",
			zap.Error(err),
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return err
	}
	// if the proxy exists, no need to create it again
	if found {
		cm.Logger.Sugar().Infow("Proxy contract already exists, skipping creation",
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil
	}

	// Fetch bytecode hash
	bytecodeHash, err := cm.AbiFetcher.FetchContractBytecodeHash(ctx, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to fetch contract bytecode hash",
			zap.Error(err),
			zap.String("address", proxyContractAddress),
		)
		return err
	}

	// Fetch ABI
	abi, err := cm.AbiFetcher.FetchContractAbi(ctx, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to fetch contract abi",
			zap.Error(err),
			zap.String("proxyContractAddress", proxyContractAddress),
			zap.String("contractAddress", contractAddress),
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
		contractStore.ContractType_External,
	)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create upgraded proxy contract",
			zap.Error(err),
			zap.String("address", proxyContractAddress),
		)
		return err
	}

	cm.Logger.Sugar().Debugf("Created new contract for proxy contract", zap.String("proxyContractAddress", proxyContractAddress))

	return nil
}

// LoadContract loads a contract into the contract store with the given parameters.
// If bytecodeHash is empty, it will be fetched using the AbiFetcher.
// If associateToProxy is not empty, it will associate the contract with the proxy contract.
// The function handles the transaction management internally.
// Returns the contract address and any error encountered.
func (cm *ContractManager) LoadContract(
	ctx context.Context,
	params ContractLoadParams,
) (string, error) {
	if params.Address == "" {
		return "", fmt.Errorf("contract address is required")
	}
	if params.Abi == "" {
		return "", fmt.Errorf("contract ABI is required with the contract address")
	}

	if params.BytecodeHash == "" {
		var err error
		params.BytecodeHash, err = cm.AbiFetcher.FetchContractBytecodeHash(ctx, params.Address)
		if err != nil {
			return "", fmt.Errorf("failed to fetch contract bytecode hash: %w", err)
		}
	}

	contract, err := helpers.WrapTxAndCommit[*contractStore.Contract](func(tx *gorm.DB) (*contractStore.Contract, error) {

		contract, err := cm.ContractStore.CreateContract(
			params.Address,
			params.Abi,
			true,
			params.BytecodeHash,
			"",
			true,
			contractStore.ContractType_External,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load contract: %w", err)
		}

		// Associate to proxy if specified
		if params.AssociateToProxy != "" {
			proxyContract, err := cm.ContractStore.GetContractForAddress(params.AssociateToProxy)
			if err != nil {
				return nil, fmt.Errorf("error checking for proxy contract: %w", err)
			}
			if proxyContract == nil {
				return nil, fmt.Errorf("proxy contract %s not found", params.AssociateToProxy)
			}
			if params.BlockNumber == 0 {
				return nil, fmt.Errorf("block number is required to load a proxy contract")
			}
			_, err = cm.ContractStore.CreateProxyContract(params.BlockNumber, params.AssociateToProxy, params.Address)
			if err != nil {
				return nil, fmt.Errorf("failed to load to proxy contract: %w", err)
			}
		}

		return contract, nil
	}, cm.Db, nil)
	if err != nil {
		return "", fmt.Errorf("failed to load contract: %w", err)
	}

	return contract.ContractAddress, nil
}
