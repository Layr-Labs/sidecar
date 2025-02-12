package indexer

import (
	"context"
	"log"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/multicallContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
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
		contractAddress := "0x055733000064333caddbc92763c58bf0192ffebf"
        blockNumber := uint64(2969816)

        upgradedLog := &parser.DecodedLog{
            LogIndex:  0,
            Address:   contractAddress,
            EventName: "Upgraded",
            Arguments: []parser.Argument{
                {
                    Name:  "implementation",
                    Type:  "address",
                    Value: common.HexToAddress("0xa504276dfdee6210c26c55385d1f793bf52089a0"),
                    Indexed: true,
                },
            },
        }

		idxr := NewIndexer(mds, contractStore, cm, client, fetchr, mccc, grm, l, cfg)


        // Get initial state

        // Perform the upgrade
        err = idxr.IndexContractUpgrade(context.Background(), blockNumber, upgradedLog)
        assert.Nil(t, err)

        // Verify database state after upgrade
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
