package postgresContractStore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PostgresContractStore struct {
	Db           *gorm.DB
	Logger       *zap.Logger
	globalConfig *config.Config
}

func NewPostgresContractStore(db *gorm.DB, l *zap.Logger, cfg *config.Config) *PostgresContractStore {
	cs := &PostgresContractStore{
		Db:           db,
		Logger:       l,
		globalConfig: cfg,
	}
	return cs
}

func (s *PostgresContractStore) GetContractForAddress(address string) (*contractStore.Contract, error) {
	var contract *contractStore.Contract

	result := s.Db.First(&contract, "contract_address = ?", address)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			s.Logger.Sugar().Debugf("Contract not found in store '%s'", address)
			return nil, nil
		}
		return nil, result.Error
	}

	return contract, nil
}

func (s *PostgresContractStore) GetProxyContractForAddress(blockNumber uint64, address string) (*contractStore.ProxyContract, error) {
	var proxyContract *contractStore.ProxyContract

	result := s.Db.First(&proxyContract, "contract_address = ? and block_number = ?", address, blockNumber)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			s.Logger.Sugar().Debugf("proxyContract not found in store '%s'", address)
			return nil, nil
		}
		return nil, result.Error
	}

	return proxyContract, nil
}

func (s *PostgresContractStore) CreateContract(
	address string,
	abiJson string,
	verified bool,
	bytecodeHash string,
	matchingContractAddress string,
	checkedForAbi bool,
) (*contractStore.Contract, error) {
	contract := &contractStore.Contract{
		ContractAddress:         strings.ToLower(address),
		ContractAbi:             abiJson,
		Verified:                verified,
		BytecodeHash:            bytecodeHash,
		MatchingContractAddress: matchingContractAddress,
		CheckedForAbi:           checkedForAbi,
	}

	result := s.Db.Create(contract)
	if result.Error != nil {
		return nil, result.Error
	}

	return contract, nil
}

func (s *PostgresContractStore) FindOrCreateContract(
	address string,
	abiJson string,
	verified bool,
	bytecodeHash string,
	matchingContractAddress string,
	checkedForAbi bool,
) (*contractStore.Contract, bool, error) {
	found := false
	upsertedContract, err := helpers.WrapTxAndCommit[*contractStore.Contract](func(tx *gorm.DB) (*contractStore.Contract, error) {
		contract, err := s.GetContractForAddress(address)
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to get contract for address", zap.Error(err), zap.String("address", address))
			return nil, err
		}

		// found contract
		if contract.ContractAddress == address {
			found = true
			return contract, nil
		}

		contract, err = s.CreateContract(address, abiJson, verified, bytecodeHash, matchingContractAddress, checkedForAbi)
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to create contract", zap.Error(err), zap.String("address", address))
			return nil, err
		}

		return contract, nil
	}, s.Db, nil)
	return upsertedContract, found, err
}

func (s *PostgresContractStore) CreateProxyContract(
	blockNumber uint64,
	contractAddress string,
	proxyContractAddress string,
) (*contractStore.ProxyContract, error) {
	proxyContract := &contractStore.ProxyContract{
		BlockNumber:          int64(blockNumber),
		ContractAddress:      contractAddress,
		ProxyContractAddress: proxyContractAddress,
	}

	result := s.Db.Model(&contractStore.ProxyContract{}).Clauses(clause.Returning{}).Create(&proxyContract)
	if result.Error != nil {
		return nil, result.Error
	}

	return proxyContract, nil
}

func (s *PostgresContractStore) FindOrCreateProxyContract(
	blockNumber uint64,
	contractAddress string,
	proxyContractAddress string,
) (*contractStore.ProxyContract, bool, error) {
	found := false
	contractAddress = strings.ToLower(contractAddress)
	proxyContractAddress = strings.ToLower(proxyContractAddress)

	upsertedContract, err := helpers.WrapTxAndCommit[*contractStore.ProxyContract](func(tx *gorm.DB) (*contractStore.ProxyContract, error) {
		// Proxy contracts are unique on block_number && contract
		contract, err := s.GetProxyContractForAddress(blockNumber, contractAddress)
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to get proxy contract for address", zap.Error(err), zap.String("contractAddress", contractAddress))
			return nil, err
		}

		// found contract
		if contract.ContractAddress == contractAddress {
			found = true
			return contract, nil
		}

		proxyContract, err := s.CreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to create proxy contract", zap.Error(err), zap.String("contractAddress", contractAddress))
			return nil, err
		}

		return proxyContract, nil
	}, s.Db, nil)
	return upsertedContract, found, err
}

func (s *PostgresContractStore) GetContractWithProxyContract(address string, atBlockNumber uint64) (*contractStore.ContractsTree, error) {
	address = strings.ToLower(address)

	query := `select
		c.contract_address as base_address,
		c.contract_abi as base_abi,
		pcc.contract_address as base_proxy_address,
		pcc.contract_abi as base_proxy_abi,
		pcclike.contract_address as base_proxy_like_address,
		pcclike.contract_abi as base_proxy_like_abi,
		clike.contract_address as base_like_address,
		clike.contract_abi as base_like_abi
	from contracts as c
	left join (
		select
			*
		from proxy_contracts
		where contract_address = @contractAddress and block_number <= @blockNumber
		order by block_number desc limit 1
	) as pc on (1=1)
	left join contracts as pcc on (pcc.contract_address = pc.proxy_contract_address)
	left join contracts as pcclike on (pcc.matching_contract_address = pcclike.contract_address)
	left join contracts as clike on (c.matching_contract_address = clike.contract_address)
	where
		c.contract_address = @contractAddress
	`
	contractTree := &contractStore.ContractsTree{}
	result := s.Db.Raw(query,
		sql.Named("contractAddress", address),
		sql.Named("blockNumber", atBlockNumber),
	).Scan(&contractTree)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			s.Logger.Sugar().Debug(fmt.Sprintf("Contract not found '%s'", address))
			return nil, nil
		}
		return nil, result.Error
	}
	if contractTree.BaseAddress == "" {
		s.Logger.Sugar().Debug(fmt.Sprintf("Contract not found in store '%s'", address))
		return nil, nil
	}

	return contractTree, nil
}

func (s *PostgresContractStore) SetContractCheckedForProxy(address string) (*contractStore.Contract, error) {
	contract := &contractStore.Contract{}

	result := s.Db.Model(contract).
		Clauses(clause.Returning{}).
		Where("contract_address = ?", strings.ToLower(address)).
		Updates(&contractStore.Contract{
			CheckedForProxy: true,
		})

	if result.Error != nil {
		return nil, result.Error
	}

	return contract, nil
}

func (s *PostgresContractStore) loadContractData() (*contractStore.CoreContractsData, error) {
	var filename string
	switch s.globalConfig.Chain {
	case config.Chain_Mainnet:
		filename = "mainnet.json"
	case config.Chain_Holesky:
		filename = "testnet.json"
	case config.Chain_Preprod:
		filename = "preprod.json"
	default:
		return nil, fmt.Errorf("Unknown environment.")
	}
	jsonData, err := contractStore.CoreContracts.ReadFile(fmt.Sprintf("coreContracts/%s", filename))
	if err != nil {
		return nil, fmt.Errorf("Failed to open core contracts file: %w", err)
	}

	// read entire file and marshal it into a CoreContractsData struct
	data := &contractStore.CoreContractsData{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return nil, fmt.Errorf("Failed to decode core contracts data: %w", err)
	}
	return data, nil
}

func (s *PostgresContractStore) InitializeCoreContracts() error {
	coreContracts, err := s.loadContractData()
	if err != nil {
		return fmt.Errorf("Failed to load core contracts: %w", err)
	}

	contracts := make([]*contractStore.Contract, 0)
	res := s.Db.Find(&contracts)
	if res.Error != nil {
		return fmt.Errorf("Failed to fetch contracts: %w", res.Error)
	}

	for _, contract := range coreContracts.CoreContracts {
		_, found, err := s.FindOrCreateContract(
			contract.ContractAddress,
			contract.ContractAbi,
			true,
			contract.BytecodeHash,
			"",
			true,
		)
		if err != nil {
			return fmt.Errorf("Failed to create core contract: %w", err)
		}
		if found {
			s.Logger.Sugar().Debugw("Contract already exists", zap.String("contractAddress", contract.ContractAddress))
			continue
		}

		_, err = s.SetContractCheckedForProxy(contract.ContractAddress)
		if err != nil {
			return fmt.Errorf("Failed to create core contract: %w", err)
		}
		s.Logger.Sugar().Debugw("Created core contract", zap.String("contractAddress", contract.ContractAddress))
	}
	for _, proxy := range coreContracts.ProxyContracts {
		_, found, err := s.FindOrCreateProxyContract(
			uint64(proxy.BlockNumber),
			proxy.ContractAddress,
			proxy.ProxyContractAddress,
		)
		if err != nil {
			return fmt.Errorf("Failed to create core proxy contract: %w", err)
		}
		if found {
			s.Logger.Sugar().Debugw("Proxy contract already exists",
				zap.String("contractAddress", proxy.ContractAddress),
				zap.String("proxyContractAddress", proxy.ContractAddress),
			)
			continue
		}
		s.Logger.Sugar().Debugw("Created proxy contract",
			zap.String("contractAddress", proxy.ContractAddress),
			zap.String("proxyContractAddress", proxy.ContractAddress),
		)
	}
	return nil
}
