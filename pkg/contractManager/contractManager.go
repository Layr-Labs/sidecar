package contractManager

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"go.uber.org/zap"
)

type ContractManager struct {
	ContractStore  contractStore.ContractStore
	EthereumClient *ethereum.Client
	metricsSink    *metrics.MetricsSink
	Logger         *zap.Logger
}

func NewContractManager(
	cs contractStore.ContractStore,
	e *ethereum.Client,
	ms *metrics.MetricsSink,
	l *zap.Logger,
) *ContractManager {
	return &ContractManager{
		ContractStore:  cs,
		EthereumClient: e,
		metricsSink:    ms,
		Logger:         l,
	}
}

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

func (cm *ContractManager) CreateProxyContract(
	contractAddress string,
	proxyContractAddress string,
	blockNumber uint64,
	reindexContract bool,
) (*contractStore.ProxyContract, error) {
	proxyContract, found, err := cm.ContractStore.FindOrCreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create proxy contract",
			zap.Error(err),
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
	} else {
		if found {
			cm.Logger.Sugar().Debugw("Found existing proxy contract",
				zap.String("contractAddress", contractAddress),
				zap.String("proxyContractAddress", proxyContractAddress),
			)
		} else {
			cm.Logger.Sugar().Debugw("Created proxy contract",
				zap.String("contractAddress", contractAddress),
				zap.String("proxyContractAddress", proxyContractAddress),
			)
		}
	}
	// Check to see if the contract we're proxying to is already in the database
	proxiedContract, err := cm.ContractStore.GetContractForAddress(proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to get contract for address",
			zap.Error(err),
			zap.String("contractAddress", proxyContractAddress),
		)
	}
	if proxiedContract != nil {
		cm.Logger.Sugar().Debugw("Found proxied contract",
			zap.String("contractAddress", proxyContractAddress),
			zap.String("proxiedContractAddress", proxiedContract.ContractAddress),
		)
		if proxiedContract.ContractAbi == "" {
			updatedContract := cm.FindAndSetContractAbi(proxyContractAddress)
			if updatedContract != nil {
				proxiedContract = updatedContract
			}
		}
	} else {
		_, err := cm.CreateContract(proxyContractAddress, "", reindexContract)
		if err != nil {
			cm.Logger.Sugar().Errorw("Failed to create contract",
				zap.Error(err),
				zap.String("contractAddress", proxyContractAddress),
			)
		} else {
			cm.Logger.Sugar().Debugw("Created contract",
				zap.String("contractAddress", proxyContractAddress),
			)
		}
	}
	return proxyContract, nil
}
