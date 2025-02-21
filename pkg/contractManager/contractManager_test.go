package contractManager

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"reflect"
	"testing"
	"time"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/ethereum/go-ethereum/common"
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

func Test_ContractManager(t *testing.T) {
	dbName, grm, l, cfg, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("POST", "http://72.46.85.253:8545",
		httpmock.NewStringResponder(200, `{"result": "0x0000000000000000000000004567890123456789012345678901234567890123"}`))

	mockHttpClient := &http.Client{
		Transport: httpmock.DefaultTransport,
	}

	baseUrl := "http://72.46.85.253:8545"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)
	client.SetHttpClient(mockHttpClient)

	af := abiFetcher.NewAbiFetcher(client, &http.Client{Timeout: 5 * time.Second}, l, cfg)

	metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	contract := &contractStore.Contract{
		ContractAddress:         "0x1234567890abcdef1234567890abcdef12345678",
		ContractAbi:             "[]",
		Verified:                true,
		BytecodeHash:            "bdb91271fe8c69b356d8f42eaa7e00d0e119258706ae4179403aa2ea45caffed",
		MatchingContractAddress: "",
	}
	proxyContract := &contractStore.ProxyContract{
		BlockNumber:          1,
		ContractAddress:      contract.ContractAddress,
		ProxyContractAddress: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
	}

	sdc, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	t.Run("Test indexing contract upgrades", func(t *testing.T) {
		// Create a contract
		_, err := contractStore.CreateContract(contract.ContractAddress, contract.ContractAbi, contract.Verified, contract.BytecodeHash, contract.MatchingContractAddress, false)
		assert.Nil(t, err)

		// Create a proxy contract
		_, err = contractStore.CreateProxyContract(uint64(proxyContract.BlockNumber), proxyContract.ContractAddress, proxyContract.ProxyContractAddress)
		assert.Nil(t, err)

		// Check if contract and proxy contract exist
		var contractCount int
		contractAddress := contract.ContractAddress
		res := grm.Raw(`select count(*) from contracts where contract_address=@contractAddress`, sql.Named("contractAddress", contractAddress)).Scan(&contractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, contractCount)

		proxyContractAddress := proxyContract.ContractAddress
		res = grm.Raw(`select count(*) from contracts where contract_address=@proxyContractAddress`, sql.Named("proxyContractAddress", proxyContractAddress)).Scan(&contractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, contractCount)

		var proxyContractCount int
		res = grm.Raw(`select count(*) from proxy_contracts where contract_address=@contractAddress`, sql.Named("contractAddress", contractAddress)).Scan(&proxyContractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, proxyContractCount)

		// An upgrade event
		upgradedLog := &parser.DecodedLog{
			LogIndex:  0,
			Address:   contract.ContractAddress,
			EventName: "Upgraded",
			Arguments: []parser.Argument{
				{
					Name:    "implementation",
					Type:    "address",
					Value:   common.HexToAddress("0x7890123456789012345678901234567890123456"),
					Indexed: true,
				},
			},
		}

		// Patch abiFetcher
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchMetadataFromAddress",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, string, error) {
				return "mockedBytecodeHash", "mockedAbi", nil
			})
		defer patches.Reset()

		// Perform the upgrade
		blockNumber := 5
		cm := NewContractManager(contractStore, client, af, sdc, l)
		err = cm.HandleContractUpgrade(context.Background(), uint64(blockNumber), upgradedLog)
		assert.Nil(t, err)

		// Verify database state after upgrade
		newProxyContractAddress := upgradedLog.Arguments[0].Value.(common.Address).Hex()
		res = grm.Raw(`select count(*) from contracts where contract_address=@newProxyContractAddress`, sql.Named("newProxyContractAddress", newProxyContractAddress)).Scan(&contractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, contractCount)

		res = grm.Raw(`select count(*) from proxy_contracts where contract_address=@contractAddress`, sql.Named("contractAddress", contractAddress)).Scan(&proxyContractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 2, proxyContractCount)
	})
	t.Run("Test getting address from storage slot", func(t *testing.T) {
		// An upgrade event without implementation argument
		upgradedLog := &parser.DecodedLog{
			LogIndex:  0,
			Address:   contract.ContractAddress,
			EventName: "Upgraded",
			Arguments: []parser.Argument{},
		}

		// Patch abiFetcher
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchMetadataFromAddress",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, string, error) {
				return "mockedBytecodeHash", "mockedAbi", nil
			})
		defer patches.Reset()

		// Perform the upgrade
		blockNumber := 10
		cm := NewContractManager(contractStore, client, af, sdc, l)
		err = cm.HandleContractUpgrade(context.Background(), uint64(blockNumber), upgradedLog)
		assert.Nil(t, err)

		// Verify database state after upgrade
		var contractCount int
		var proxyContractCount int
		newProxyContractAddress := "0x4567890123456789012345678901234567890123"
		res := grm.Raw(`select count(*) from contracts where contract_address=@newProxyContractAddress`, sql.Named("newProxyContractAddress", newProxyContractAddress)).Scan(&contractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, contractCount)

		res = grm.Raw(`select count(*) from proxy_contracts where contract_address=@contractAddress`, sql.Named("contractAddress", contract.ContractAddress)).Scan(&proxyContractCount)
		assert.Nil(t, res.Error)
		assert.Equal(t, 3, proxyContractCount)
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
