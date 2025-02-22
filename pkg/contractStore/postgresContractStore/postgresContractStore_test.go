package postgresContractStore

import (
	"testing"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
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
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func Test_PostgresContractStore(t *testing.T) {
	dbName, db, l, cfg, err := setup()
	cfg.Chain = config.Chain_Holesky

	if err != nil {
		t.Fatal(err)
	}

	cs := NewPostgresContractStore(db, l, cfg)

	createdContracts := make([]*contractStore.Contract, 0)
	createdProxyContracts := make([]*contractStore.ProxyContract, 0)

	t.Run("Read core contracts from disk", func(t *testing.T) {
		contracts, err := cs.loadContractData()
		assert.Nil(t, err)
		assert.NotEmpty(t, contracts)
	})
	t.Run("Initialize core contracts", func(t *testing.T) {
		err := cs.InitializeCoreContracts()
		assert.Nil(t, err)

		contracts, err := cs.loadContractData()
		assert.Nil(t, err)

		assert.True(t, len(contracts.CoreContracts) > 0)
		for _, contract := range contracts.CoreContracts {
			c, err := cs.GetContractForAddress(contract.ContractAddress)
			assert.Nil(t, err)
			assert.Equal(t, contract.ContractAddress, c.ContractAddress)
			assert.Equal(t, contract.ContractAbi, c.ContractAbi)
			assert.Equal(t, true, c.Verified)
			assert.Equal(t, contract.BytecodeHash, c.BytecodeHash)
		}

		assert.True(t, len(contracts.ProxyContracts) > 0)
		for _, proxy := range contracts.ProxyContracts {
			tree, err := cs.GetContractWithProxyContract(proxy.ContractAddress, uint64(proxy.BlockNumber))
			assert.Nil(t, err)
			assert.NotNil(t, tree)
			assert.Equal(t, proxy.ContractAddress, tree.BaseAddress)
			assert.Equal(t, proxy.ProxyContractAddress, tree.BaseProxyAddress)
			assert.True(t, len(tree.BaseAbi) > 0)
			assert.True(t, len(tree.BaseProxyAbi) > 0)
		}

		contractAddress := "0x055733000064333caddbc92763c58bf0192ffebf"
		proxyContractAddress := "0xef5ba995bc7722fd1e163edf8dc09375de3d3e3a"
		blockNumber := uint64(1167044)
		tree, err := cs.GetContractWithProxyContract(contractAddress, blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, contractAddress, tree.BaseAddress)
		assert.Equal(t, proxyContractAddress, tree.BaseProxyAddress)
	})

	t.Run("Create contract", func(t *testing.T) {
		contract := &contractStore.Contract{
			ContractAddress:         "0x123",
			ContractAbi:             "[]",
			Verified:                true,
			BytecodeHash:            "0x123",
			MatchingContractAddress: "",
		}

		createdContract, found, err := cs.FindOrCreateContract(contract.ContractAddress, contract.ContractAbi, contract.Verified, contract.BytecodeHash, contract.MatchingContractAddress, false)
		assert.Nil(t, err)
		assert.False(t, found)
		assert.Equal(t, contract.ContractAddress, createdContract.ContractAddress)
		assert.Equal(t, contract.ContractAbi, createdContract.ContractAbi)
		assert.Equal(t, contract.Verified, createdContract.Verified)
		assert.Equal(t, contract.BytecodeHash, createdContract.BytecodeHash)
		assert.Equal(t, contract.MatchingContractAddress, createdContract.MatchingContractAddress)

		createdContracts = append(createdContracts, createdContract)
	})
	t.Run("Find contract rather than create", func(t *testing.T) {
		contract := &contractStore.Contract{
			ContractAddress:         "0x123",
			ContractAbi:             "[]",
			Verified:                true,
			BytecodeHash:            "0x123",
			MatchingContractAddress: "",
		}

		createdContract, found, err := cs.FindOrCreateContract(contract.ContractAddress, contract.ContractAbi, contract.Verified, contract.BytecodeHash, contract.MatchingContractAddress, false)
		assert.Nil(t, err)
		assert.True(t, found)
		assert.Equal(t, contract.ContractAddress, createdContract.ContractAddress)
		assert.Equal(t, contract.ContractAbi, createdContract.ContractAbi)
		assert.Equal(t, contract.Verified, createdContract.Verified)
		assert.Equal(t, contract.BytecodeHash, createdContract.BytecodeHash)
		assert.Equal(t, contract.MatchingContractAddress, createdContract.MatchingContractAddress)
	})
	t.Run("Create proxy contract", func(t *testing.T) {
		proxyContract := &contractStore.ProxyContract{
			BlockNumber:          1,
			ContractAddress:      createdContracts[0].ContractAddress,
			ProxyContractAddress: "0x456",
		}

		proxy, found, err := cs.FindOrCreateProxyContract(uint64(proxyContract.BlockNumber), proxyContract.ContractAddress, proxyContract.ProxyContractAddress)
		assert.Nil(t, err)
		assert.False(t, found)
		assert.Equal(t, proxyContract.BlockNumber, proxy.BlockNumber)
		assert.Equal(t, proxyContract.ContractAddress, proxy.ContractAddress)
		assert.Equal(t, proxyContract.ProxyContractAddress, proxy.ProxyContractAddress)

		newProxyContract := &contractStore.Contract{
			ContractAddress:         proxyContract.ProxyContractAddress,
			ContractAbi:             "[]",
			Verified:                true,
			BytecodeHash:            "0x456",
			MatchingContractAddress: "",
		}
		createdProxy, _, err := cs.FindOrCreateContract(newProxyContract.ContractAddress, newProxyContract.ContractAbi, newProxyContract.Verified, newProxyContract.BytecodeHash, newProxyContract.MatchingContractAddress, false)
		assert.Nil(t, err)
		createdContracts = append(createdContracts, createdProxy)

		createdProxyContracts = append(createdProxyContracts, proxy)
	})
	t.Run("Find proxy contract rather than create", func(t *testing.T) {
		proxyContract := &contractStore.ProxyContract{
			BlockNumber:          1,
			ContractAddress:      createdContracts[0].ContractAddress,
			ProxyContractAddress: "0x456",
		}

		proxy, found, err := cs.FindOrCreateProxyContract(uint64(proxyContract.BlockNumber), proxyContract.ContractAddress, proxyContract.ProxyContractAddress)
		assert.Nil(t, err)
		assert.True(t, found)
		assert.Equal(t, proxyContract.BlockNumber, proxy.BlockNumber)
		assert.Equal(t, proxyContract.ContractAddress, proxy.ContractAddress)
		assert.Equal(t, proxyContract.ProxyContractAddress, proxy.ProxyContractAddress)
	})
	t.Run("Get contract from address", func(t *testing.T) {
		address := createdContracts[0].ContractAddress

		contract, err := cs.GetContractForAddress(address)
		assert.Nil(t, err)
		assert.Equal(t, address, contract.ContractAddress)
		assert.Equal(t, createdContracts[0].ContractAbi, contract.ContractAbi)
		assert.Equal(t, createdContracts[0].Verified, contract.Verified)
		assert.Equal(t, createdContracts[0].BytecodeHash, contract.BytecodeHash)
		assert.Equal(t, createdContracts[0].MatchingContractAddress, contract.MatchingContractAddress)
	})
	t.Run("Get contract with proxy contract", func(t *testing.T) {
		address := createdContracts[0].ContractAddress

		contracts := make([]contractStore.Contract, 0)
		db.Raw(`select * from contracts`, address).Scan(&contracts)

		contractsTree, err := cs.GetContractWithProxyContract(address, 1)
		assert.Nil(t, err)
		assert.Equal(t, createdContracts[0].ContractAddress, contractsTree.BaseAddress)
		assert.Equal(t, createdContracts[0].ContractAbi, contractsTree.BaseAbi)
		assert.Equal(t, createdContracts[1].ContractAddress, contractsTree.BaseProxyAddress)
		assert.Equal(t, createdContracts[1].ContractAbi, contractsTree.BaseProxyAbi)
		assert.Equal(t, "", contractsTree.BaseLikeAddress)
		assert.Equal(t, "", contractsTree.BaseLikeAbi)
	})
	t.Run("Get proxy contract from address", func(t *testing.T) {
		proxyContract := &contractStore.ProxyContract{
			BlockNumber:          1,
			ContractAddress:      createdContracts[0].ContractAddress,
			ProxyContractAddress: "0x456",
		}

		proxy, err := cs.GetProxyContractForAddress(uint64(proxyContract.BlockNumber), proxyContract.ContractAddress)
		assert.Nil(t, err)
		assert.Equal(t, proxyContract.BlockNumber, proxy.BlockNumber)
		assert.Equal(t, proxyContract.ContractAddress, proxy.ContractAddress)
		assert.Equal(t, proxyContract.ProxyContractAddress, proxy.ProxyContractAddress)
	})
	t.Run("Set contract checked for proxy", func(t *testing.T) {
		address := createdContracts[0].ContractAddress

		contract, err := cs.SetContractCheckedForProxy(address)
		assert.Nil(t, err)
		assert.Equal(t, address, contract.ContractAddress)
		assert.True(t, contract.CheckedForProxy)
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, db, l)
	})
}
