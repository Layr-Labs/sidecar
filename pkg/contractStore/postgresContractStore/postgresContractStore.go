// Package postgresContractStore provides a PostgreSQL implementation of the contractStore interface
// for storing and retrieving smart contract data, including ABIs, bytecode hashes, and proxy relationships.
package postgresContractStore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// PostgresContractStore implements the contractStore.ContractStore interface using PostgreSQL.
// It provides methods for storing and retrieving contract data, including ABIs and proxy relationships.
type PostgresContractStore struct {
	// Db is the GORM database connection
	Db *gorm.DB
	// Logger is used for logging operations
	Logger *zap.Logger
}

// NewPostgresContractStore creates a new PostgresContractStore with the provided database connection
// and logger
func NewPostgresContractStore(db *gorm.DB, l *zap.Logger) *PostgresContractStore {
	cs := &PostgresContractStore{
		Db:     db,
		Logger: l,
	}
	return cs
}

// GetContractForAddress retrieves a contract by its address.
// Returns nil if the contract is not found.
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

// GetProxyContractForAddress retrieves a proxy contract by its address and block number.
// Returns nil if the proxy contract is not found.
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

// ListInterestingContractAddresses returns all proxy addresses in the database as a slice of strings.
func (s *PostgresContractStore) ListInterestingContractAddresses() ([]string, error) {
	var addresses []string

	result := s.Db.Raw(`select distinct(contract_address) from proxy_contracts`).Scan(&addresses)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			s.Logger.Sugar().Debugf("No proxy contracts found in store")
			return nil, nil
		}
		return nil, result.Error
	}
	return addresses, nil
}

// CreateContract creates a new contract record in the database.
// It stores the contract address, ABI, verification status, bytecode hash,
// and other metadata.
func (s *PostgresContractStore) CreateContract(
	address string,
	abiJson string,
	verified bool,
	bytecodeHash string,
	matchingContractAddress string,
	checkedForAbi bool,
	contractType contractStore.ContractType,
) (*contractStore.Contract, error) {

	contract := &contractStore.Contract{
		ContractAddress:         strings.ToLower(address),
		ContractAbi:             abiJson,
		ContractType:            contractType,
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

// FindOrCreateContract looks for a contract with the given address and creates it if not found.
// Returns the contract, a boolean indicating whether it was found, and any error encountered.
func (s *PostgresContractStore) FindOrCreateContract(
	address string,
	abiJson string,
	verified bool,
	bytecodeHash string,
	matchingContractAddress string,
	checkedForAbi bool,
	contractType contractStore.ContractType,
) (*contractStore.Contract, bool, error) {
	var contract *contractStore.Contract
	result := s.Db.First(&contract, "contract_address = ?", strings.ToLower(address))
	if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, false, result.Error
	}

	// found contract
	if contract != nil && contract.ContractAddress == address {
		return contract, true, nil
	}

	contract, err := s.CreateContract(address, abiJson, verified, bytecodeHash, matchingContractAddress, checkedForAbi, contractType)
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to create contract", zap.Error(err), zap.String("address", address))
		return nil, false, err
	}

	return contract, false, nil
}

// CreateProxyContract creates a new proxy contract relationship in the database.
// It records the block number at which the proxy relationship was established,
// the proxy contract address, and the implementation contract address.
func (s *PostgresContractStore) CreateProxyContract(
	blockNumber uint64,
	upgradeableProxyAddress string,
	implementationAddress string,
) (*contractStore.ProxyContract, error) {
	proxyContract := &contractStore.ProxyContract{
		BlockNumber:          int64(blockNumber),
		ContractAddress:      strings.ToLower(upgradeableProxyAddress),
		ProxyContractAddress: strings.ToLower(implementationAddress),
	}

	result := s.Db.Model(&contractStore.ProxyContract{}).Clauses(clause.Returning{}).Create(&proxyContract)
	if result.Error != nil {
		return nil, result.Error
	}

	return proxyContract, nil
}

// FindOrCreateProxyContract looks for a proxy contract with the given parameters and creates it if not found.
// Returns the proxy contract, a boolean indicating whether it was found, and any error encountered.
func (s *PostgresContractStore) FindOrCreateProxyContract(
	blockNumber uint64,
	upgradeableProxyAddress string,
	implementationAddress string,
) (*contractStore.ProxyContract, bool, error) {
	upgradeableProxyAddress = strings.ToLower(upgradeableProxyAddress)
	implementationAddress = strings.ToLower(implementationAddress)

	var contract *contractStore.ProxyContract
	// Proxy contracts are unique on block_number && contract
	result := s.Db.First(&contract, "contract_address = ? and proxy_contract_address = ?", upgradeableProxyAddress, implementationAddress)
	if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, false, result.Error
	}

	// found contract
	if contract != nil && contract.ContractAddress == upgradeableProxyAddress {
		return contract, true, nil
	}

	proxyContract, err := s.CreateProxyContract(blockNumber, upgradeableProxyAddress, implementationAddress)
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to create proxy contract", zap.Error(err), zap.String("upgradeableProxyAddress", upgradeableProxyAddress))
		return nil, false, err
	}

	return proxyContract, false, nil
}

func (s *PostgresContractStore) GetProxyContractWithImplementations(contractAddress string) ([]*contractStore.Contract, error) {
	contractAddress = strings.ToLower(contractAddress)
	contracts := make([]*contractStore.Contract, 0)

	query := `select
		c.contract_address,
		c.contract_abi,
		c.bytecode_hash
	from contracts as c
	where c.contract_address = @contractAddress
	or c.contract_address in (
		select proxy_contract_address from proxy_contracts where contract_address = @contractAddress
	)
	`
	result := s.Db.Raw(query,
		sql.Named("contractAddress", contractAddress),
	).Scan(&contracts)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			s.Logger.Sugar().Debug(fmt.Sprintf("Proxy contract not found '%s'", contractAddress))
			return nil, result.Error
		}
		return nil, result.Error
	}

	return contracts, nil
}

// GetContractWithProxyContract retrieves a contract and its associated proxy contract (if any)
// for the given address at the specified block number.
// It returns a ContractsTree containing the contract and proxy information.
func (s *PostgresContractStore) GetContractWithProxyContract(address string, atBlockNumber uint64) (*contractStore.ContractsTree, error) {
	address = strings.ToLower(address)

	query := `select
		c.contract_address as base_address,
		c.contract_abi as base_abi,
		c.contract_type as contract_type,
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

// SetContractCheckedForProxy marks a contract as having been checked for a proxy relationship.
// This is used to avoid redundant checks for contracts that don't use proxies.
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

func (s *PostgresContractStore) InitializeContracts(contractsData *contractStore.CoreContractsData, contractType contractStore.ContractType) error {
	contracts := make([]*contractStore.Contract, 0)
	res := s.Db.Find(&contracts)
	if res.Error != nil {
		return fmt.Errorf("failed to fetch contracts: %w", res.Error)
	}

	for _, contract := range contractsData.CoreContracts {
		_, found, err := s.FindOrCreateContract(
			contract.ContractAddress,
			contract.ContractAbi,
			true,
			contract.BytecodeHash,
			"",
			true,
			contractType,
		)
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to create core contract", zap.Error(err), zap.String("contractAddress", contract.ContractAddress), zap.String("contractAbi", contract.ContractAbi), zap.String("bytecodeHash", contract.BytecodeHash))
			return fmt.Errorf("Failed to create core contract: %w", err)
		}
		if found {
			s.Logger.Sugar().Debugw("Contract already exists", zap.String("contractAddress", contract.ContractAddress))
			continue
		}

		_, err = s.SetContractCheckedForProxy(contract.ContractAddress)
		if err != nil {
			return fmt.Errorf("failed to create core contract: %w", err)
		}
		s.Logger.Sugar().Debugw("Created core contract", zap.String("contractAddress", contract.ContractAddress))
	}
	for _, proxy := range contractsData.ProxyContracts {
		_, found, err := s.FindOrCreateProxyContract(
			uint64(proxy.BlockNumber),
			proxy.ContractAddress,
			proxy.ProxyContractAddress,
		)
		if err != nil {
			return fmt.Errorf("failed to create core proxy contract: %w", err)
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

func (s *PostgresContractStore) InitializeExternalContracts(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open external contracts file: %w", err)
	}
	defer file.Close()

	return s.InitializeExternalContractsFromReader(file)
}

func (s *PostgresContractStore) InitializeExternalContractsFromReader(reader io.Reader) error {
	jsonData, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read external contracts data: %w", err)
	}

	// read entire file and marshal it into a CoreContractsData struct
	data := &contractStore.CoreContractsData{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return fmt.Errorf("failed to decode external contracts data: %w", err)
	}

	if err := s.InitializeContracts(data, contractStore.ContractType_External); err != nil {
		return fmt.Errorf("failed to initialize external contracts: %w", err)
	}

	return nil
}
