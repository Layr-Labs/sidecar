package sequentialStrategyCaller

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func setup() (
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.EthereumRpcConfig.BaseUrl = "http://72.46.85.253:8545"
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	return l, cfg, nil
}

func Test_SequentialStrategyCaller(t *testing.T) {
	l, cfg, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = cfg.EthereumRpcConfig.BaseUrl

	client := ethereum.NewClient(ethConfig, l)

	ssc := NewSequentialStrategyCaller(client, l)

	t.Run("Get shares to underlying for stETH strategy", func(t *testing.T) {
		// stETH strategy address from mainnet
		stETHStrategyAddress := "0x93c4b944d05dfe6df7645a86cd2206016c51564d"

		// Test with 1 ETH worth of shares (1e18 wei)
		oneETHShares := big.NewInt(0)
		oneETHShares.SetString("1000000000000000000", 10) // 1e18

		underlying, err := ssc.GetSharesToUnderlying(context.Background(), stETHStrategyAddress, oneETHShares)
		if err != nil {
			t.Fatal(err)
		}

		assert.NotNil(t, underlying)
		assert.True(t, underlying.Cmp(big.NewInt(0)) > 0, "Underlying amount should be greater than 0")

		// For stETH, the underlying should be close to the shares but might be slightly different due to rebasing
		// Let's check it's within a reasonable range (between 0.9 and 1.2 ETH for 1 ETH of shares)
		minExpected := big.NewInt(0)
		minExpected.SetString("900000000000000000", 10) // 0.9 ETH
		maxExpected := big.NewInt(0)
		maxExpected.SetString("1200000000000000000", 10) // 1.2 ETH

		assert.True(t, underlying.Cmp(minExpected) >= 0,
			fmt.Sprintf("Underlying amount %s should be at least %s", underlying.String(), minExpected.String()))
		assert.True(t, underlying.Cmp(maxExpected) <= 0,
			fmt.Sprintf("Underlying amount %s should be at most %s", underlying.String(), maxExpected.String()))

		fmt.Printf("stETH Strategy - Shares: %s, Underlying: %s\n", oneETHShares.String(), underlying.String())
	})

	t.Run("Get shares to underlying amounts for multiple strategies", func(t *testing.T) {
		// Test with multiple strategies
		strategyShares := map[string]*big.Int{
			"0x93c4b944d05dfe6df7645a86cd2206016c51564d": big.NewInt(0), // stETH
			"0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2": big.NewInt(0), // rETH
		}

		// Set 1 ETH worth of shares for each
		oneETH := "1000000000000000000"
		strategyShares["0x93c4b944d05dfe6df7645a86cd2206016c51564d"].SetString(oneETH, 10)
		strategyShares["0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2"].SetString(oneETH, 10)

		amounts, err := ssc.GetSharesToUnderlyingAmounts(context.Background(), strategyShares)
		if err != nil {
			t.Fatal(err)
		}

		assert.NotNil(t, amounts)
		assert.Len(t, amounts, 2)

		// Check stETH amount
		stETHAmount, exists := amounts["0x93c4b944d05dfe6df7645a86cd2206016c51564d"]
		assert.True(t, exists)
		assert.NotNil(t, stETHAmount)
		assert.True(t, stETHAmount.Cmp(big.NewInt(0)) > 0, "stETH amount should be greater than 0")

		// Check rETH amount
		rETHAmount, exists := amounts["0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2"]
		assert.True(t, exists)
		assert.NotNil(t, rETHAmount)
		assert.True(t, rETHAmount.Cmp(big.NewInt(0)) > 0, "rETH amount should be greater than 0")

		fmt.Printf("Strategy Amounts - stETH: %s, rETH: %s\n", stETHAmount.String(), rETHAmount.String())
	})

	t.Run("Handle zero shares gracefully", func(t *testing.T) {
		strategyShares := map[string]*big.Int{
			"0x93c4b944d05dfe6df7645a86cd2206016c51564d": big.NewInt(0), // stETH with 0 shares
		}

		amounts, err := ssc.GetSharesToUnderlyingAmounts(context.Background(), strategyShares)
		if err != nil {
			t.Fatal(err)
		}

		assert.NotNil(t, amounts)
		assert.Len(t, amounts, 1)

		// Should return 0 for zero shares
		stETHAmount, exists := amounts["0x93c4b944d05dfe6df7645a86cd2206016c51564d"]
		assert.True(t, exists)
		assert.Equal(t, big.NewInt(0), stETHAmount)

		fmt.Printf("Zero shares amount: %s\n", stETHAmount.String())
	})

	t.Run("Handle invalid strategy address gracefully", func(t *testing.T) {
		invalidStrategy := "0x0000000000000000000000000000000000000000"
		oneETHShares := big.NewInt(0)
		oneETHShares.SetString("1000000000000000000", 10)

		strategyShares := map[string]*big.Int{
			invalidStrategy: oneETHShares,
		}

		amounts, err := ssc.GetSharesToUnderlyingAmounts(context.Background(), strategyShares)
		if err != nil {
			t.Fatal(err)
		}

		assert.NotNil(t, amounts)
		assert.Len(t, amounts, 1)

		// Should fallback to original shares for invalid strategy
		invalidAmount, exists := amounts[invalidStrategy]
		assert.True(t, exists)
		assert.Equal(t, oneETHShares, invalidAmount)

		fmt.Printf("Invalid strategy amount (fallback to shares): %s\n", invalidAmount.String())
	})
}
