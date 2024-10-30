package contractStore

import (
	"embed"
	"fmt"
	"strings"
	"time"
)

//go:embed coreContracts
var CoreContracts embed.FS

type ContractStore interface {
	GetContractForAddress(address string) (*Contract, error)
	FindOrCreateContract(address string, abiJson string, verified bool, bytecodeHash string, matchingContractAddress string, checkedForAbi bool) (*Contract, bool, error)
	FindVerifiedContractWithMatchingBytecodeHash(bytecodeHash string, address string) (*Contract, error)
	FindOrCreateProxyContract(blockNumber uint64, contractAddress string, proxyContractAddress string) (*ProxyContract, bool, error)
	GetContractWithProxyContract(address string, atBlockNumber uint64) (*ContractsTree, error)
	SetContractCheckedForProxy(address string) (*Contract, error)
	SetContractAbi(address string, abi string, verified bool) (*Contract, error)
	SetContractMatchingContractAddress(address string, matchingContractAddress string) (*Contract, error)
	GetProxyContractForAddressAtBlock(address string, blockNumber uint64) (*ProxyContract, error)

	InitializeCoreContracts() error
}

// Tables.
type Contract struct {
	ContractAddress         string
	ContractAbi             string
	MatchingContractAddress string
	Verified                bool
	BytecodeHash            string
	CheckedForProxy         bool
	CheckedForAbi           bool
	CreatedAt               time.Time
	UpdatedAt               time.Time
	DeletedAt               time.Time
}

type UnverifiedContract struct {
	ContractAddress string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       time.Time
}

type ProxyContract struct {
	BlockNumber          int64
	ContractAddress      string
	ProxyContractAddress string
	CreatedAt            time.Time
	UpdatedAt            time.Time
	DeletedAt            time.Time
}

// Result queries.
type ContractWithProxyContract struct {
	ContractAddress       string
	ContractAbi           string
	Verified              bool
	IsProxyContract       bool
	ProxyContractAddress  string
	ProxyContractAbi      string
	ProxyContractVerified string
}

func (c *ContractWithProxyContract) CombineAbis() (string, error) {
	if c.ProxyContractAbi == "" {
		return c.ContractAbi, nil
	}

	if c.ContractAbi == "" && c.ProxyContractAbi != "" {
		return c.ProxyContractAbi, nil
	}

	if c.ContractAbi == "" && c.ProxyContractAbi == "" {
		return "", nil
	}

	strippedContractAbi := c.ContractAbi[1 : len(c.ContractAbi)-1]
	strippedProxyContractAbi := c.ProxyContractAbi[1 : len(c.ProxyContractAbi)-1]

	return fmt.Sprintf("[%s,%s]", strippedContractAbi, strippedProxyContractAbi), nil
}

type ContractsTree struct {
	BaseAddress string
	BaseAbi     string

	// Address the base address proxies to
	BaseProxyAddress string
	BaseProxyAbi     string

	// Proxy address may look like another contract
	BaseProxyLikeAddress string
	BaseProxyLikeAbi     string

	// Base contract may look like another contract
	BaseLikeAddress string
	BaseLikeAbi     string
}

func stripJsonBrackets(abi string) string {
	return abi[1 : len(abi)-1]
}

func (c *ContractsTree) CombineAbis() string {
	abisToCombine := make([]string, 0)

	if c.BaseProxyLikeAbi != "" {
		abisToCombine = append(abisToCombine, stripJsonBrackets(c.BaseProxyLikeAbi))
	}

	if c.BaseProxyAbi != "" {
		abisToCombine = append(abisToCombine, stripJsonBrackets(c.BaseProxyAbi))
	}

	if c.BaseAbi != "" {
		abisToCombine = append(abisToCombine, stripJsonBrackets(c.BaseAbi))
	}

	if c.BaseLikeAbi != "" {
		abisToCombine = append(abisToCombine, stripJsonBrackets(c.BaseLikeAbi))
	}

	combinedAbi := fmt.Sprintf("[%s]", strings.Join(abisToCombine, ","))
	return combinedAbi
}

type CoreContract struct {
	ContractAddress string `json:"contract_address"`
	ContractAbi     string `json:"contract_abi"`
	BytecodeHash    string `json:"bytecode_hash"`
}

type CoreProxyContract struct {
	ContractAddress      string `json:"contract_address"`
	ProxyContractAddress string `json:"proxy_contract_address"`
	BlockNumber          int64  `json:"block_number"`
}

type CoreContractsData struct {
	CoreContracts  []CoreContract      `json:"core_contracts"`
	ProxyContracts []CoreProxyContract `json:"proxy_contracts"`
}
