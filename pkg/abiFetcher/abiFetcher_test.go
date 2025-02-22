package abiFetcher

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/jarcoal/httpmock"
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
	_, _, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	baseUrl := "http://72.46.85.253:8545"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)

	t.Run("Test getting IPFS url from bytecode", func(t *testing.T) {
		af := NewAbiFetcher(client, httpClient, l, cfg)

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		bytecode, err := af.EthereumClient.GetCode(context.Background(), address)
		assert.Nil(t, err)

		url, err := af.GetIPFSUrlFromBytecode(bytecode)
		assert.Nil(t, err)

		expectedUrl := "https://ipfs.io/ipfs/QmeuBk6fmBdgW3B3h11LRkFw8shYLbMb4w7ko82jCxg6jR"
		assert.Equal(t, expectedUrl, url)
	})
	t.Run("Test fetching ABI from IPFS", func(t *testing.T) {
		mockUrl := "https://test"
		patches := gomonkey.ApplyMethod(reflect.TypeOf(&AbiFetcher{}), "GetIPFSUrlFromBytecode",
			func(_ *AbiFetcher, _ string) (string, error) {
				return mockUrl, nil
			})
		defer patches.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		mockAbiResponse := `{
			"output": {
				"abi": "[{\"type\":\"function\",\"name\":\"test\"}]"
			}
		}`

		httpmock.RegisterResponder("GET", mockUrl,
			httpmock.NewStringResponder(200, mockAbiResponse))

		mockHttpClient := &http.Client{
			Transport: httpmock.DefaultTransport,
		}

		af := NewAbiFetcher(client, mockHttpClient, l, cfg)

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		abi, err := af.FetchAbiFromIPFS(address, "mocked")
		assert.Nil(t, err)

		expectedAbi := `"[{\"type\":\"function\",\"name\":\"test\"}]"`
		assert.Equal(t, abi, expectedAbi)
	})
}
