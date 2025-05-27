package etherscan

import (
	"net/http"
	"testing"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/etherscan"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
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

func Test_Etherscan(t *testing.T) {
	_, _, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	mockHttpClient := &http.Client{
		Transport: httpmock.DefaultTransport,
	}

	ec := etherscan.NewEtherscanClient(mockHttpClient, l, cfg)
	eas := NewEtherscan(ec, l)

	t.Run("Test fetching ABI from Etherscan", func(t *testing.T) {
		mockAbiResponse := `{
			"status": "1",
			"message": "OK",
			"result": "[{\"constant\":true,\"inputs\":[],\"name\":\"get\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"
		}`

		// Register the responder with a pattern that matches the full URL with query parameters
		httpmock.RegisterResponder("GET", `=~^https://api\.etherscan\.io/api\?.*address=0x29a954e9e7f12936db89b183ecdf879fbbb99f14.*`,
			httpmock.NewStringResponder(200, mockAbiResponse))

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		abi, err := eas.FetchAbi(address, "mocked")
		assert.Nil(t, err)

		expectedAbi := "[{\"constant\":true,\"inputs\":[],\"name\":\"get\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"
		assert.Equal(t, expectedAbi, abi)
	})
	t.Run("Test fetching ABI with error status", func(t *testing.T) {
		mockErrorResponse := `{
			"status": "0",
			"message": "NOTOK",
			"result": "Error fetching ABI"
		}`

		// Register the responder with a pattern that matches the full URL with query parameters
		httpmock.RegisterResponder("GET", `=~^https://api\.etherscan\.io/api\?.*address=0x29a954e9e7f12936db89b183ecdf879fbbb99f14.*`,
			httpmock.NewStringResponder(200, mockErrorResponse))

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		abi, err := eas.FetchAbi(address, "mocked")
		assert.NotNil(t, err)
		assert.Equal(t, "", abi)
	})
}
