package indexer

import (
	"context"
	"database/sql"
	"log"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/multicallContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"github.com/ethereum/go-ethereum/common"
)

func Test_Indexer(t *testing.T) {
	// uses setup from restakedStrategies_test
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	baseUrl := "https://winter-white-crater.ethereum-holesky.quiknode.pro/1b1d75c4ada73b7ad98e1488880649d4ea637733/"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)

	metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	contract := &contractStore.Contract{
		ContractAddress:         "0x123",
		ContractAbi:             "[]",
		Verified:                true,
		BytecodeHash:            "0x123",
		MatchingContractAddress: "",
	}
	proxyContract := &contractStore.ProxyContract{
		BlockNumber:          1,
		ContractAddress:      contract.ContractAddress,
		ProxyContractAddress: "0x456",
	}

	sdc, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)

	fetchr := fetcher.NewFetcher(client, cfg, l)

	mccc := multicallContractCaller.NewMulticallContractCaller(client, l)

	cm := contractManager.NewContractManager(contractStore, client, sdc, l)

	t.Run("Test indexing contract upgrades", func(t *testing.T) {
		// Create a contract
		_, found, err := contractStore.FindOrCreateContract(contract.ContractAddress, contract.ContractAbi, contract.Verified, contract.BytecodeHash, contract.MatchingContractAddress, false)
		assert.Nil(t, err)
		assert.False(t, found)

		// Create a proxy contract
		_, found, err = contractStore.FindOrCreateProxyContract(uint64(proxyContract.BlockNumber), proxyContract.ContractAddress, proxyContract.ProxyContractAddress)
		assert.Nil(t, err)
		assert.False(t, found)

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
					Value:   common.HexToAddress("0x789"),
					Indexed: true,
				},
			},
		}

		// Perform the upgrade
		idxr := NewIndexer(mds, contractStore, cm, client, fetchr, mccc, grm, l, cfg)
		err = idxr.IndexContractUpgrade(context.Background(), 5, upgradedLog)
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
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
