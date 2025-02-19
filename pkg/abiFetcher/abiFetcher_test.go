package abiFetcher

import (
	"context"
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

func Test_AbiFetcher(t *testing.T) {
	_, _, l, _, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	baseUrl := "http://72.46.85.253:8545"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)

	t.Run("Test getting IPFS url from bytecode", func(t *testing.T) {
		af := NewAbiFetcher(client, l)

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		bytecode, err := af.EthereumClient.GetCode(context.Background(), address)
		assert.Nil(t, err)

		url, err := af.GetIPFSUrlFromBytecode(bytecode)
		assert.Nil(t, err)

		expectedUrl := "https://ipfs.io/ipfs/QmeuBk6fmBdgW3B3h11LRkFw8shYLbMb4w7ko82jCxg6jR"
		assert.Equal(t, expectedUrl, url)
	})
	t.Run("Test fetching ABI from IPFS", func(t *testing.T) {
		af := NewAbiFetcher(client, l)

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		bytecode, err := af.EthereumClient.GetCode(context.Background(), address)
		assert.Nil(t, err)

		abi, err := af.FetchAbiFromIPFS(address, bytecode)
		assert.Nil(t, err)

		assert.Greater(t, len(abi), 0)
	})
}
