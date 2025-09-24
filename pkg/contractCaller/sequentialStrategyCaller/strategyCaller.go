package sequentialStrategyCaller

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"go.uber.org/zap"
)

type SequentialStrategyCaller struct {
	EthereumClient *ethereum.Client
	Logger         *zap.Logger
}

func NewSequentialStrategyCaller(ec *ethereum.Client, l *zap.Logger) *SequentialStrategyCaller {
	return &SequentialStrategyCaller{
		EthereumClient: ec,
		Logger:         l,
	}
}

// GetSharesToUnderlying fetches the underlying amount for a specific strategy and shares
func (ssc *SequentialStrategyCaller) GetSharesToUnderlying(ctx context.Context, strategy string, shares *big.Int) (*big.Int, error) {
	if ssc.EthereumClient == nil {
		return nil, fmt.Errorf("ethereum client not available")
	}

	// Parse the strategy ABI
	parsedABI, err := abi.JSON(strings.NewReader(contractCaller.StrategyAbi))
	if err != nil {
		return nil, fmt.Errorf("failed to parse strategy ABI: %v", err)
	}

	// Get Ethereum contract caller
	ethClient, err := ssc.EthereumClient.GetEthereumContractCaller()
	if err != nil {
		return nil, fmt.Errorf("failed to get ethereum contract caller: %v", err)
	}

	strategyAddress := common.HexToAddress(strategy)

	// Create bound contract for this strategy
	contract := bind.NewBoundContract(strategyAddress, parsedABI, ethClient, nil, nil)

	// Call sharesToUnderlying
	var result []interface{}
	err = contract.Call(&bind.CallOpts{Context: ctx}, &result, "sharesToUnderlying", shares)
	if err != nil {
		return nil, fmt.Errorf("failed to call sharesToUnderlying for strategy %s: %v", strategy, err)
	}

	if len(result) == 0 || result[0] == nil {
		return nil, fmt.Errorf("got nil or empty result from sharesToUnderlying for strategy %s", strategy)
	}

	// Extract the underlying value from the result
	underlying, ok := result[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("got unexpected result type from sharesToUnderlying for strategy %s", strategy)
	}

	return underlying, nil
}

// GetSharesToUnderlyingAmounts fetches underlying amounts for multiple strategies
func (ssc *SequentialStrategyCaller) GetSharesToUnderlyingAmounts(ctx context.Context, strategyShares map[string]*big.Int) (map[string]*big.Int, error) {
	if ssc.EthereumClient == nil {
		ssc.Logger.Sugar().Warnw("Ethereum client not available, using shares as fallback amounts")
		// Return shares as fallback (1:1 ratio)
		amounts := make(map[string]*big.Int)
		for strategy, shares := range strategyShares {
			amounts[strategy] = new(big.Int).Set(shares) // Copy to avoid modifying original
		}
		return amounts, nil
	}

	amounts := make(map[string]*big.Int)

	// Call sharesToUnderlying for each strategy
	for strategyAddr, shares := range strategyShares {
		if shares == nil || shares.Cmp(big.NewInt(0)) == 0 {
			amounts[strategyAddr] = big.NewInt(0)
			continue
		}

		underlying, err := ssc.GetSharesToUnderlying(ctx, strategyAddr, shares)
		if err != nil {
			ssc.Logger.Sugar().Warnw("Failed to get sharesToUnderlying for strategy, using shares as fallback",
				zap.String("strategy", strategyAddr),
				zap.Error(err),
			)
			// Fallback to shares (1:1 ratio)
			amounts[strategyAddr] = new(big.Int).Set(shares)
			continue
		}

		amounts[strategyAddr] = underlying

		ssc.Logger.Sugar().Debugw("Retrieved sharesToUnderlying amount",
			zap.String("strategy", strategyAddr),
			zap.String("shares", shares.String()),
			zap.String("underlying", underlying.String()),
		)
	}

	return amounts, nil
}
