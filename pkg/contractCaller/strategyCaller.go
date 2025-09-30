package contractCaller

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// StrategyUnderlyingAmount represents the result of a sharesToUnderlying call
type StrategyUnderlyingAmount struct {
	Strategy   string
	Shares     *big.Int
	Underlying *big.Int
	Error      error
}

// IStrategyCaller defines the interface for strategy contract operations
type IStrategyCaller interface {
	// GetSharesToUnderlyingAmounts fetches underlying amounts for multiple strategies
	GetSharesToUnderlyingAmounts(ctx context.Context, strategyShares map[string]*big.Int) (map[string]*big.Int, error)

	// GetSharesToUnderlying fetches the underlying amount for a specific strategy and shares
	GetSharesToUnderlying(ctx context.Context, strategy string, shares *big.Int) (*big.Int, error)

	// GetUnderlyingToken fetches the underlying token address for a specific strategy
	GetUnderlyingToken(ctx context.Context, strategy string) (common.Address, error)

	// GetUnderlyingTokens fetches underlying token addresses for multiple strategies
	GetUnderlyingTokens(ctx context.Context, strategies []string) (map[string]common.Address, error)
}
