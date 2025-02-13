package contractManager

import (
	"context"
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
) (*contractStore.ProxyContract, error) {
	proxyContract, found, err := cm.ContractStore.FindOrCreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create proxy contract",
			zap.Error(err),
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil, err
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
	
	bytecode, err := cm.EthereumClient.GetCode(context.Background(), proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to get proxy contract bytecode",
			zap.Error(err),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil, err
	}

	bytecodeHash := ethereum.HashBytecode(bytecode)
	cm.Logger.Sugar().Debug("Fetched proxy contract bytecode",
		zap.String("proxyContractAddress", proxyContractAddress),
		zap.String("bytecodeHash", bytecodeHash),
	)

	_, _, err = cm.ContractStore.FindOrCreateContract(
		proxyContractAddress,
		"",
		false,
		bytecodeHash,
		"",
		false,
	)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create new contract for proxy contract",
			zap.Error(err),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil, err
	} else {
		cm.Logger.Sugar().Debugf("Created new contract for proxy contract", zap.String("proxyContractAddress", proxyContractAddress))
	}

	return proxyContract, nil
}
