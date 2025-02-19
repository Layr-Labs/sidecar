package sequentialContractCaller

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
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

func Test_SequentialContractCaller(t *testing.T) {
	l, cfg, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = cfg.EthereumRpcConfig.BaseUrl

	client := ethereum.NewClient(ethConfig, l)

	scc := NewSequentialContractCaller(client, cfg, 10, l)

	t.Run("Get distribution root by index", func(t *testing.T) {
		distributionRoot, err := scc.GetDistributionRootByIndex(context.Background(), 8)
		if err != nil {
			t.Fatal(err)
		}

		rootString := utils.ConvertBytesToString(distributionRoot.Root[:])

		assert.Equal(t, distributionRoot.Disabled, true)
		assert.Equal(t, "0x8a7099a557f56bf18761cd2c303baeec71a60ee135107e7e02546dc547c16d99", rootString)

		fmt.Printf("Distribution root: %+v\n", distributionRoot)

	})
}
