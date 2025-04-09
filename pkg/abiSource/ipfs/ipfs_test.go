package ipfs

import (
	"net/http"
	"reflect"
	"testing"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

func Test_Ipfs(t *testing.T) {
	_, _, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test fetching ABI from IPFS", func(t *testing.T) {
		mockUrl := "https://test"
		patches := gomonkey.ApplyMethod(reflect.TypeOf(&Ipfs{}), "GetIPFSUrlFromBytecode",
			func(_ *Ipfs, _ string) (string, error) {
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

		ias := NewIpfs(mockHttpClient, l, cfg)

		address := "0x29a954e9e7f12936db89b183ecdf879fbbb99f14"
		abi, err := ias.FetchAbi(address, "mocked")
		assert.Nil(t, err)

		expectedAbi := `"[{\"type\":\"function\",\"name\":\"test\"}]"`
		assert.Equal(t, expectedAbi, abi)
	})
}
