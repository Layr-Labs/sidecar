package config

import (
	"fmt"
	"strings"
)

type EnvScope string

type Network uint
type Environment int

const (
	Network_Holesky  Network = 0
	Network_Ethereum Network = 1

	Environment_PreProd Environment = 1
	Environment_Testnet Environment = 2
	Environment_Mainnet Environment = 3
	Environment_Local   Environment = 4
)

type ChainConfig struct {
	Name        string
	Network     Network
	Environment Environment
}

func ParseChainConfig(name string) (ChainConfig, error) {
	if name == "" {
		return ChainConfig{}, fmt.Errorf("chain not found")
	}
	chainConfig := [4]ChainConfig{
		{"local", Network_Holesky, Environment_Local},
		{"preprod", Network_Holesky, Environment_PreProd},
		{"testnet", Network_Holesky, Environment_Testnet},
		{"ethereum", Network_Ethereum, Environment_Mainnet},
	}
	for _, c := range chainConfig {
		if c.Name == name {
			return c, nil
		}
	}
	return ChainConfig{}, fmt.Errorf("unsupported chain %s", name)
}

func GetEnvironmentAsString(e Environment) (string, error) {
	switch e {
	case Environment_PreProd:
		return "preprod", nil
	case Environment_Testnet:
		return "testnet", nil
	case Environment_Mainnet:
		return "mainnet", nil
	case Environment_Local:
		return "local", nil
	default:
		return "", fmt.Errorf("unsupported environment %d", e)
	}
}

func GetNetworkAsString(n Network) (string, error) {
	switch n {
	case Network_Ethereum:
		return "ethereum", nil
	case Network_Holesky:
		return "holesky", nil
	default:
		return "", fmt.Errorf("unsupported network %d", n)
	}
}

func parseStringAsList(envVar string) []string {
	if envVar == "" {
		return []string{}
	}
	// split on commas
	stringList := strings.Split(envVar, ",")

	for i, s := range stringList {
		stringList[i] = strings.TrimSpace(s)
	}
	l := make([]string, 0)
	for _, s := range stringList {
		if s != "" {
			l = append(l, s)
		}
	}
	return l
}

type Config struct {
	Network           Network
	Environment       Environment
	Debug             bool
	StatsdUrl         string
	EthereumRpcConfig EthereumRpcConfig
	EtherscanConfig   EtherscanConfig
	SqliteConfig      SqliteConfig
	RpcConfig         RpcConfig
}

type EthereumRpcConfig struct {
	BaseUrl string
	WsUrl   string
}

type EtherscanConfig struct {
	ApiKeys []string
}

type SqliteConfig struct {
	InMemory   bool
	DbFilePath string
}

type RpcConfig struct {
	GrpcPort int
	HttpPort int
}

func (s *SqliteConfig) GetSqlitePath() string {
	if s.InMemory {
		return "file::memory:?cache=shared"
	}
	return s.DbFilePath
}

func NewConfig(options *Options) *Config {
	chainConfig, err := ParseChainConfig(options.ChainConfig)
	if err != nil {
		panic(err)
	}
	return &Config{
		Network:     chainConfig.Network,
		Environment: chainConfig.Environment,
		Debug:       options.Debug,
		StatsdUrl:   options.StatsdUrl,

		EthereumRpcConfig: EthereumRpcConfig{
			BaseUrl: options.EthereumRPCBaseURL,
			WsUrl:   options.EthereumWSURL,
		},

		EtherscanConfig: EtherscanConfig{
			ApiKeys: parseStringAsList(options.EtherscanAPIKeys),
		},

		SqliteConfig: SqliteConfig{
			InMemory:   options.SqliteInMemory,
			DbFilePath: options.SqliteDBFilePath,
		},

		RpcConfig: RpcConfig{
			GrpcPort: options.RPCGRPCPort,
			HttpPort: options.RPCHTTPPort,
		},
	}
}

func (c *Config) GetAVSDirectoryForEnvAndNetwork() string {
	return AVSDirectoryAddresses[c.Environment][c.Network]
}

var AVSDirectoryAddresses = map[Environment]map[Network]string{
	Environment_PreProd: {
		Network_Holesky: "0x141d6995556135D4997b2ff72EB443Be300353bC",
	},
	Environment_Testnet: {
		Network_Holesky: "0x055733000064333CaDDbC92763c58BF0192fFeBf",
	},
	Environment_Mainnet: {
		Network_Ethereum: "0x135dda560e946695d6f155dacafc6f1f25c1f5af",
	},
}

type ContractAddresses struct {
	RewardsCoordinator string
	EigenpodManager    string
	StrategyManager    string
	DelegationManager  string
	AvsDirectory       string
}

func (c *Config) GetContractsMapForEnvAndNetwork() *ContractAddresses {
	if c.Environment == Environment_PreProd {
		return &ContractAddresses{
			RewardsCoordinator: "0xb22ef643e1e067c994019a4c19e403253c05c2b0",
			EigenpodManager:    "0xb8d8952f572e67b11e43bc21250967772fa883ff",
			StrategyManager:    "0xf9fbf2e35d8803273e214c99bf15174139f4e67a",
			DelegationManager:  "0x75dfe5b44c2e530568001400d3f704bc8ae350cc",
			AvsDirectory:       "0x141d6995556135d4997b2ff72eb443be300353bc",
		}
	} else if c.Environment == Environment_Testnet {
		return &ContractAddresses{
			RewardsCoordinator: "0xacc1fb458a1317e886db376fc8141540537e68fe",
			EigenpodManager:    "0x30770d7e3e71112d7a6b7259542d1f680a70e315",
			StrategyManager:    "0xdfb5f6ce42aaa7830e94ecfccad411bef4d4d5b6",
			DelegationManager:  "0xa44151489861fe9e3055d95adc98fbd462b948e7",
			AvsDirectory:       "0x055733000064333caddbc92763c58bf0192ffebf",
		}
	} else if c.Environment == Environment_Mainnet {
		return &ContractAddresses{
			RewardsCoordinator: "0x7750d328b314effa365a0402ccfd489b80b0adda",
			EigenpodManager:    "0x91e677b07f7af907ec9a428aafa9fc14a0d3a338",
			StrategyManager:    "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
			DelegationManager:  "0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a",
			AvsDirectory:       "0x135dda560e946695d6f155dacafc6f1f25c1f5af",
		}
	} else {
		return nil
	}
}

func (c *Config) GetInterestingAddressForConfigEnv() []string {
	addresses := c.GetContractsMapForEnvAndNetwork()

	if addresses == nil {
		return []string{}
	}

	return []string{
		addresses.RewardsCoordinator,
		addresses.EigenpodManager,
		addresses.StrategyManager,
		addresses.DelegationManager,
		addresses.AvsDirectory,
	}
}

func (c *Config) GetGenesisBlockNumber() uint64 {
	switch c.Environment {
	case Environment_PreProd:
		return 1140406
	case Environment_Testnet:
		return 1167044
	case Environment_Mainnet:
		return 19492759
	default:
		return 0
	}
}

func (c *Config) GetEigenLayerGenesisBlockHeight() (uint64, error) {
	switch c.Environment {
	case Environment_PreProd, Environment_Testnet:
		return 1, nil
	case Environment_Mainnet:
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported environment %d", c.Environment)
	}
}

func (c *Config) GetOperatorRestakedStrategiesStartBlock() int64 {
	switch c.Environment {
	case Environment_PreProd:
	case Environment_Testnet:
		return 1162800
	case Environment_Mainnet:
		return 19616400
	}
	return 0
}
