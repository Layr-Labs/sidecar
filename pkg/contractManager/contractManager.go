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

func (cm *ContractManager) GetContractWithImplementations(contractAddress string) ([]*contractStore.Contract, error) {
	return cm.ContractStore.GetProxyContractWithImplementations(contractAddress)
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
