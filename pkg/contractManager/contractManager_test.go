package contractManager

import (
	"context"
	"database/sql"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts"
	coreContractMigrations "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations"
	"net/http"
	"reflect"
	"testing"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

	af := abiFetcher.NewAbiFetcher(client, abiFetcher.DefaultHttpClient(), l, []abiSource.AbiSource{})

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

	cs := postgresContractStore.NewPostgresContractStore(grm, l, cfg)

	ccm := coreContracts.NewCoreContractManager(grm, cfg, cs, l)
	if _, err := ccm.MigrateCoreContracts(coreContractMigrations.GetCoreContractMigrations()); err != nil {
		l.Fatal("Failed to migrate core contracts", zap.Error(err))
	}

	t.Run("Test indexing contract upgrades", func(t *testing.T) {
		// Create a contract
		_, err := cs.CreateContract(contract.ContractAddress, contract.ContractAbi, contract.Verified, contract.BytecodeHash, contract.MatchingContractAddress, false, contractStore.ContractType_External)
		assert.Nil(t, err)

		// Create a proxy contract
		_, err = cs.CreateProxyContract(uint64(proxyContract.BlockNumber), proxyContract.ContractAddress, proxyContract.ProxyContractAddress)
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
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchContractBytecodeHash",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "mockedBytecodeHash", nil
			})
		patches.ApplyMethod(reflect.TypeOf(af), "FetchContractAbi",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "mockedAbi", nil
			})
		defer patches.Reset()

		// Perform the upgrade
		blockNumber := 5
		cm := NewContractManager(grm, cs, client, af, l)
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
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchContractBytecodeHash",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "mockedBytecodeHash", nil
			})
		patches.ApplyMethod(reflect.TypeOf(af), "FetchContractAbi",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "mockedAbi", nil
			})
		defer patches.Reset()

		// Perform the upgrade
		blockNumber := 10
		cm := NewContractManager(grm, cs, client, af, l)
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
	t.Run("Test LoadContract with valid parameters", func(t *testing.T) {
		// Patch abiFetcher
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchContractBytecodeHash",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "mockedBytecodeHash", nil
			})
		defer patches.Reset()

		cm := NewContractManager(grm, cs, client, af, l)

		params := ContractLoadParams{
			Address:      "0x2468ace02468ace02468ace02468ace02468ace0",
			Abi:          `[{"type":"function","name":"test","inputs":[],"outputs":[]}]`,
			BytecodeHash: "precomputedHash123",
		}

		address, err := cm.LoadContract(context.Background(), params)
		assert.Nil(t, err)
		assert.Equal(t, params.Address, address)

		// Verify contract was stored correctly
		contract, err := cs.GetContractForAddress(params.Address)
		assert.Nil(t, err)
		assert.NotNil(t, contract)
		assert.Equal(t, params.Address, contract.ContractAddress)
		assert.Equal(t, params.Abi, contract.ContractAbi)
		assert.Equal(t, params.BytecodeHash, contract.BytecodeHash)
	})

	t.Run("Test LoadContract with empty bytecode hash", func(t *testing.T) {
		// Patch abiFetcher to return a mocked bytecode hash
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchContractBytecodeHash",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "fetchedBytecodeHash", nil
			})
		defer patches.Reset()

		cm := NewContractManager(grm, cs, client, af, l)

		params := ContractLoadParams{
			Address:      "0x1357924680135792468013579246801357924680",
			Abi:          `[{"type":"function","name":"testFunc","inputs":[],"outputs":[]}]`,
			BytecodeHash: "", // Empty bytecode hash should trigger fetch
		}

		address, err := cm.LoadContract(context.Background(), params)
		assert.Nil(t, err)
		assert.Equal(t, params.Address, address)

		// Verify contract was stored with fetched bytecode hash
		contract, err := cs.GetContractForAddress(params.Address)
		assert.Nil(t, err)
		assert.NotNil(t, contract)
		assert.Equal(t, "fetchedBytecodeHash", contract.BytecodeHash)
	})

	t.Run("Test LoadContract with proxy association", func(t *testing.T) {
		// Create a proxy contract first
		proxyAddress := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		_, err := cs.CreateContract(
			proxyAddress,
			`[{"type":"function","name":"proxy","inputs":[],"outputs":[]}]`,
			true,
			"proxyBytecodeHash",
			"",
			true,
			contractStore.ContractType_External,
		)
		assert.Nil(t, err)

		// Patch abiFetcher
		patches := gomonkey.ApplyMethod(reflect.TypeOf(af), "FetchContractBytecodeHash",
			func(_ *abiFetcher.AbiFetcher, _ context.Context, _ string) (string, error) {
				return "implBytecodeHash", nil
			})
		defer patches.Reset()

		cm := NewContractManager(grm, cs, client, af, l)

		params := ContractLoadParams{
			Address:          "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Abi:              `[{"type":"function","name":"implementation","inputs":[],"outputs":[]}]`,
			BytecodeHash:     "implBytecodeHash",
			BlockNumber:      100,
			AssociateToProxy: proxyAddress,
		}

		address, err := cm.LoadContract(context.Background(), params)
		assert.Nil(t, err)
		assert.Equal(t, params.Address, address)

		// Verify the proxy relationship
		proxyContract, err := cs.GetProxyContractForAddress(params.BlockNumber, proxyAddress)
		assert.Nil(t, err)
		assert.NotNil(t, proxyContract)
		assert.Equal(t, proxyAddress, proxyContract.ContractAddress)
		assert.Equal(t, params.Address, proxyContract.ProxyContractAddress)
	})

	t.Run("Test LoadContract with proxy association but no block number", func(t *testing.T) {
		// Create a proxy contract first
		proxyAddress := "0xcccccccccccccccccccccccccccccccccccccccc"
		_, err := cs.CreateContract(
			proxyAddress,
			`[{"type":"function","name":"proxy2","inputs":[],"outputs":[]}]`,
			true,
			"proxy2BytecodeHash",
			"",
			true,
			contractStore.ContractType_External,
		)
		assert.Nil(t, err)

		cm := NewContractManager(grm, cs, client, af, l)

		params := ContractLoadParams{
			Address:          "0xdddddddddddddddddddddddddddddddddddddddd",
			Abi:              `[{"type":"function","name":"implementation2","inputs":[],"outputs":[]}]`,
			BytecodeHash:     "impl2BytecodeHash",
			BlockNumber:      0, // Missing block number
			AssociateToProxy: proxyAddress,
		}

		_, err = cm.LoadContract(context.Background(), params)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "block number is required")
	})

	t.Run("Test LoadContract with nonexistent proxy", func(t *testing.T) {
		cm := NewContractManager(grm, cs, client, af, l)

		params := ContractLoadParams{
			Address:          "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			Abi:              `[{"type":"function","name":"implementation3","inputs":[],"outputs":[]}]`,
			BytecodeHash:     "impl3BytecodeHash",
			BlockNumber:      200,
			AssociateToProxy: "0xffffffffffffffffffffffffffffffffffffffff", // Non-existent proxy
		}

		_, err = cm.LoadContract(context.Background(), params)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Test LoadContract with missing parameters", func(t *testing.T) {
		cm := NewContractManager(grm, cs, client, af, l)

		// Missing address
		params1 := ContractLoadParams{
			Abi:          `[{"type":"function","name":"test","inputs":[],"outputs":[]}]`,
			BytecodeHash: "hash",
		}
		_, err := cm.LoadContract(context.Background(), params1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "address is required")

		// Missing ABI
		params2 := ContractLoadParams{
			Address:      "0x1111111111111111111111111111111111111111",
			BytecodeHash: "hash",
		}
		_, err = cm.LoadContract(context.Background(), params2)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "ABI is required")
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
