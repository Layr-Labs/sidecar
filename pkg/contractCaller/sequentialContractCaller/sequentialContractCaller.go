package sequentialContractCaller

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/go-sidecar/pkg/contractCaller"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"go.uber.org/zap"
	"math/big"
	"regexp"
	"strings"
	"time"
)

type SequentialContractCaller struct {
	EthereumClient *ethereum.Client
	Logger         *zap.Logger
}

func NewContractCaller(ec *ethereum.Client, l *zap.Logger) *SequentialContractCaller {
	return &SequentialContractCaller{
		EthereumClient: ec,
		Logger:         l,
	}
}

func isExecutionRevertedError(err error) bool {
	r := regexp.MustCompile(`execution reverted`)
	return r.MatchString(err.Error())
}

func getOperatorRestakedStrategiesRetryable(ctx context.Context, avs string, operator string, blockNumber uint64, client *ethereum.Client, l *zap.Logger) ([]common.Address, error) {
	retries := []int{0, 2, 5, 10}
	for i, backoff := range retries {
		results, err := getOperatorRestakedStrategies(ctx, avs, operator, blockNumber, client, l)
		if err != nil {
			l.Sugar().Errorw("GetOperatorRestakedStrategiesRetryable - failed to get results",
				zap.Int("attempt", i+1),
				zap.String("avs", avs),
				zap.String("operator", operator),
				zap.Uint64("blockNumber", blockNumber),
				zap.Error(err),
			)
			time.Sleep(time.Second * time.Duration(backoff))
		} else {
			return results, nil
		}
	}
	return nil, nil
}

func getOperatorRestakedStrategies(ctx context.Context, avs string, operator string, blockNumber uint64, client *ethereum.Client, l *zap.Logger) ([]common.Address, error) {
	a, err := abi.JSON(strings.NewReader(contractCaller.AvsServiceManagerAbi))
	if err != nil {
		l.Sugar().Errorw("GetOperatorRestakedStrategies - failed to parse abi", zap.Error(err))
		return nil, err
	}

	callerClient, err := client.GetEthereumContractCaller()
	if err != nil {
		l.Sugar().Errorw("GetOperatorRestakedStrategies - failed to get contract caller", zap.Error(err))
		return nil, err
	}

	contract := bind.NewBoundContract(common.HexToAddress(avs), a, callerClient, nil, nil)

	bigBlockNumber := big.NewInt(int64(blockNumber))

	results := make([]interface{}, 0)

	err = contract.Call(&bind.CallOpts{BlockNumber: bigBlockNumber, Context: ctx}, &results, "getOperatorRestakedStrategies", common.HexToAddress(operator))
	if err != nil {
		if isExecutionRevertedError(err) {
			return nil, nil
		}
		return nil, err
	}

	return results[0].([]common.Address), nil
}

func (cc *SequentialContractCaller) GetOperatorRestakedStrategies(ctx context.Context, avs string, operator string, blockNumber uint64) ([]common.Address, error) {
	return getOperatorRestakedStrategiesRetryable(ctx, avs, operator, blockNumber, cc.EthereumClient, cc.Logger)
}

type ReconciledContractCaller struct {
	EthereumClients []*ethereum.Client
	Logger          *zap.Logger
}

func NewRecociledContractCaller(ec []*ethereum.Client, l *zap.Logger) (*ReconciledContractCaller, error) {
	if len(ec) == 0 {
		return nil, fmt.Errorf("No ethereum clients provided")
	}
	return &ReconciledContractCaller{
		EthereumClients: ec,
		Logger:          l,
	}, nil
}

func (rcc *ReconciledContractCaller) GetOperatorRestakedStrategies(ctx context.Context, avs string, operator string, blockNumber uint64) ([]common.Address, error) {
	allResults := make([][]common.Address, 0)
	for i, ec := range rcc.EthereumClients {
		ec = ec
		results, err := getOperatorRestakedStrategiesRetryable(ctx, avs, operator, blockNumber, ec, rcc.Logger)
		if err != nil {
			rcc.Logger.Sugar().Errorw("Error fetching results for client", zap.Error(err), zap.Int("clientIndex", i))
		} else {
			allResults = append(allResults, results)
		}
	}

	// make sure the number of total results is equal to the number of clients
	if len(allResults) != len(rcc.EthereumClients) {
		return nil, fmt.Errorf("Failed to fetch results for all clients")
	}

	if len(allResults) == 1 {
		return allResults[0], nil
	}

	// make sure that the results from each client are all the same length
	expectedLength := len(allResults[0])
	for i := 1; i < len(allResults); i++ {
		if len(allResults[i]) != expectedLength {
			return nil, fmt.Errorf("Client %d returned unexpected number of results", i)
		}
	}

	// check each item in each result to make sure they are all the same
	for _, clientResult := range allResults[1:] {
		for i, item := range clientResult {
			if allResults[0][i] != item {
				return nil, fmt.Errorf("Client results do not match")
			}
		}
	}

	return allResults[0], nil
}
