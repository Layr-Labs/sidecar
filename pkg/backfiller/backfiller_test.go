package backfiller

import (
	"context"
	"fmt"
	"testing"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func Test_Backfiller(t *testing.T) {
	_, _, l, cfg, err := setup()
	assert.NoError(t, err)

	baseUrl := "http://72.46.85.253:8545"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)

	b := NewBackfiller(client, cfg, l)
	t.Run("Test getting transaction receipts", func(t *testing.T) {
		receipts, _ := b.Backfill(context.Background(), 19592323)
		for _, receipt := range receipts {
			if receipt.TransactionHash == "0xb51ad742d1c13af667acb1608d33790a5dcc4970153a6ac2f415390b16fb485e" {
				fmt.Println(receipt.ContractAddress)
			}
		}
		assert.Equal(t, 1, len(receipts))
	})
}
