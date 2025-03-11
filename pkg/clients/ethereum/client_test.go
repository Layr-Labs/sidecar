package ethereum

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/stretchr/testify/assert"
	"testing"
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
}
