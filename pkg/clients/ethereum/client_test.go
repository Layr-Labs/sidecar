package ethereum

import (
	"context"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/stretchr/testify/assert"
)

func Test_EthereumClient(t *testing.T) {
	l, err := logger.NewLogger(&logger.LoggerConfig{
		Debug: false,
	})
	assert.Nil(t, err)

	client := NewClient(&EthereumClientConfig{
		BaseUrl: "http://72.46.85.253:8545",
	}, l)

	t.Run("eth_getBlockReceipts", func(t *testing.T) {
		receipts, err := client.GetBlockTransactionReceipts(context.Background(), uint64(22019077))
		assert.Nil(t, err)
		assert.NotNil(t, receipts)
	})

	t.Run("eth_getLogs", func(t *testing.T) {
		// Use a known contract address, for example, WETH on mainnet
		contractAddress := "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
		fromBlock := uint64(17000000)
		toBlock := uint64(17000100)

		logs, err := client.GetLogs(context.Background(), contractAddress, fromBlock, toBlock)
		assert.Nil(t, err)
		assert.NotNil(t, logs)

		if len(logs) > 0 {
			// Verify that logs contain the expected fields
			for _, log := range logs {
				assert.NotEmpty(t, log.Address)
				assert.NotEmpty(t, log.BlockHash)
				assert.NotEmpty(t, log.TransactionHash)
				// BlockNumber should be within the requested range
				blockNum := log.BlockNumber.Value()
				assert.True(t, blockNum >= fromBlock && blockNum <= toBlock)
			}
		}
	})
}
