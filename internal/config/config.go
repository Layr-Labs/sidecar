package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type EnvScope string

type Network uint
type Environment int

type Chain string

func (c Chain) String() string {
	return string(c)
}

type ForkName string

const (
	Chain_Mainnet Chain = "mainnet"
	Chain_Holesky Chain = "holesky"
	Chain_Preprod Chain = "preprod"

	Fork_Nile    ForkName = "nile"
	Fork_Amazon  ForkName = "amazon"
	Fork_Panama  ForkName = "panama"
	Fork_Arno    ForkName = "arno"
	Fork_Trinity ForkName = "trinity"

	ENV_PREFIX = "SIDECAR"
)

func normalizeFlagName(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

type EthereumRpcConfig struct {
	BaseUrl               string
	ContractCallBatchSize int  // Number of contract calls to make in parallel
	UseNativeBatchCall    bool // Use the native eth_call method for batch calls
	NativeBatchCallSize   int  // Number of calls to put in a single eth_call request
	ChunkedBatchCallSize  int  // Number of calls to make in parallel
}

type DatabaseConfig struct {
	Host       string
	Port       int
	User       string
	Password   string
	DbName     string
	SchemaName string
}

type RpcConfig struct {
	GrpcPort int
	HttpPort int
}

type RewardsConfig struct {
	ValidateRewardsRoot          bool
	GenerateStakerOperatorsTable bool
}

type StatsdConfig struct {
	Enabled bool
	Url     string
}

type DataDogConfig struct {
	StatsdConfig StatsdConfig
}

type PrometheusConfig struct {
	Enabled bool
	Port    int
}

type Config struct {
	Debug             bool
	EthereumRpcConfig EthereumRpcConfig
	DatabaseConfig    DatabaseConfig
	RpcConfig         RpcConfig
	Chain             Chain
	Rewards           RewardsConfig
	DataDogConfig     DataDogConfig
	PrometheusConfig  PrometheusConfig
}

func StringWithDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

var (
	Debug              = "debug"
	DatabaseHost       = "database.host"
	DatabasePort       = "database.port"
	DatabaseUser       = "database.user"
	DatabasePassword   = "database.password"
	DatabaseDbName     = "database.db_name"
	DatabaseSchemaName = "database.schema_name"

	RewardsValidateRewardsRoot          = "rewards.validate_rewards_root"
	RewardsGenerateStakerOperatorsTable = "rewards.generate_staker_operators_table"

	EthereumRpcBaseUrl               = "ethereum.rpc_url"
	EthereumRpcContractCallBatchSize = "ethereum.contract_call_batch_size"
	EthereumRpcUseNativeBatchCall    = "ethereum.use_native_batch_call"
	EthereumRpcNativeBatchCallSize   = "ethereum.native_batch_call_size"
	EthereumRpcChunkedBatchCallSize  = "ethereum.chunked_batch_call_size"

	DataDogStatsdEnabled = "datadog.statsd.enabled"
	DataDogStatsdUrl     = "datadog.statsd.url"

	PrometheusEnabled = "prometheus.enabled"
	PrometheusPort    = "prometheus.port"
)

func NewConfig() *Config {
	return &Config{
		Debug: viper.GetBool(normalizeFlagName("debug")),
		Chain: Chain(StringWithDefault(viper.GetString(normalizeFlagName("chain")), "holesky")),

		EthereumRpcConfig: EthereumRpcConfig{
			BaseUrl:               viper.GetString(normalizeFlagName(EthereumRpcBaseUrl)),
			ContractCallBatchSize: viper.GetInt(normalizeFlagName(EthereumRpcContractCallBatchSize)),
			UseNativeBatchCall:    viper.GetBool(normalizeFlagName(EthereumRpcUseNativeBatchCall)),
			NativeBatchCallSize:   viper.GetInt(normalizeFlagName(EthereumRpcNativeBatchCallSize)),
			ChunkedBatchCallSize:  viper.GetInt(normalizeFlagName(EthereumRpcChunkedBatchCallSize)),
		},

		DatabaseConfig: DatabaseConfig{
			Host:       viper.GetString(normalizeFlagName(DatabaseHost)),
			Port:       viper.GetInt(normalizeFlagName(DatabasePort)),
			User:       viper.GetString(normalizeFlagName(DatabaseUser)),
			Password:   viper.GetString(normalizeFlagName(DatabasePassword)),
			DbName:     viper.GetString(normalizeFlagName(DatabaseDbName)),
			SchemaName: viper.GetString(normalizeFlagName(DatabaseSchemaName)),
		},

		RpcConfig: RpcConfig{
			GrpcPort: viper.GetInt(normalizeFlagName("rpc.grpc_port")),
			HttpPort: viper.GetInt(normalizeFlagName("rpc.http_port")),
		},

		Rewards: RewardsConfig{
			ValidateRewardsRoot:          viper.GetBool(normalizeFlagName(RewardsValidateRewardsRoot)),
			GenerateStakerOperatorsTable: viper.GetBool(normalizeFlagName(RewardsGenerateStakerOperatorsTable)),
		},

		DataDogConfig: DataDogConfig{
			StatsdConfig: StatsdConfig{
				Enabled: viper.GetBool(normalizeFlagName(DataDogStatsdEnabled)),
				Url:     viper.GetString(normalizeFlagName(DataDogStatsdUrl)),
			},
		},

		PrometheusConfig: PrometheusConfig{
			Enabled: viper.GetBool(normalizeFlagName(PrometheusEnabled)),
			Port:    viper.GetInt(normalizeFlagName(PrometheusPort)),
		},
	}
}

func (c *Config) GetAVSDirectoryForChain() string {
	return c.GetContractsMapForChain().AvsDirectory
}

var AVSDirectoryAddresses = map[Chain]string{
	Chain_Preprod: "0x141d6995556135D4997b2ff72EB443Be300353bC",
	Chain_Holesky: "0x055733000064333CaDDbC92763c58BF0192fFeBf",
	Chain_Mainnet: "0x135dda560e946695d6f155dacafc6f1f25c1f5af",
}

type ContractAddresses struct {
	RewardsCoordinator string
	EigenpodManager    string
	StrategyManager    string
	DelegationManager  string
	AvsDirectory       string
}

func (c *Config) GetContractsMapForChain() *ContractAddresses {
	if c.Chain == Chain_Preprod {
		return &ContractAddresses{
			RewardsCoordinator: "0xb22ef643e1e067c994019a4c19e403253c05c2b0",
			EigenpodManager:    "0xb8d8952f572e67b11e43bc21250967772fa883ff",
			StrategyManager:    "0xf9fbf2e35d8803273e214c99bf15174139f4e67a",
			DelegationManager:  "0x75dfe5b44c2e530568001400d3f704bc8ae350cc",
			AvsDirectory:       "0x141d6995556135d4997b2ff72eb443be300353bc",
		}
	} else if c.Chain == Chain_Holesky {
		return &ContractAddresses{
			RewardsCoordinator: "0xacc1fb458a1317e886db376fc8141540537e68fe",
			EigenpodManager:    "0x30770d7e3e71112d7a6b7259542d1f680a70e315",
			StrategyManager:    "0xdfb5f6ce42aaa7830e94ecfccad411bef4d4d5b6",
			DelegationManager:  "0xa44151489861fe9e3055d95adc98fbd462b948e7",
			AvsDirectory:       "0x055733000064333caddbc92763c58bf0192ffebf",
		}
	} else if c.Chain == Chain_Mainnet {
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
	addresses := c.GetContractsMapForChain()

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
	switch c.Chain {
	case Chain_Preprod:
		return 1140406
	case Chain_Holesky:
		return 1167044
	case Chain_Mainnet:
		return 17445563
	default:
		return 0
	}
}

type ForkMap map[ForkName]string

func (c *Config) GetForkDates() (ForkMap, error) {
	switch c.Chain {
	case Chain_Preprod:
		return ForkMap{
			Fork_Amazon:  "1970-01-01", // Amazon hard fork was never on preprod as we backfilled
			Fork_Nile:    "2024-08-14", // Last calculation end timestamp was 8-13: https://holesky.etherscan.io/tx/0xb5a6855e88c79312b7c0e1c9f59ae9890b97f157ea27e69e4f0fadada4712b64#eventlog
			Fork_Panama:  "2024-10-01",
			Fork_Arno:    "2024-12-11",
			Fork_Trinity: "2025-01-13",
		}, nil
	case Chain_Holesky:
		return ForkMap{
			Fork_Amazon:  "1970-01-01", // Amazon hard fork was never on testnet as we backfilled
			Fork_Nile:    "2024-08-13", // Last calculation end timestamp was 8-12: https://holesky.etherscan.io/tx/0x5fc81b5ed2a78b017ef313c181d8627737a97fef87eee85acedbe39fc8708c56#eventlog
			Fork_Panama:  "2024-10-01",
			Fork_Arno:    "2024-12-13",
			Fork_Trinity: "2025-01-13",
		}, nil
	case Chain_Mainnet:
		return ForkMap{
			Fork_Amazon:  "2024-08-02", // Last calculation end timestamp was 8-01: https://etherscan.io/tx/0x2aff6f7b0132092c05c8f6f41a5e5eeeb208aa0d95ebcc9022d7823e343dd012#eventlog
			Fork_Nile:    "2024-08-12", // Last calculation end timestamp was 8-11: https://etherscan.io/tx/0x922d29d93c02d189fc2332041f01a80e0007cd7a625a5663ef9d30082f7ef66f#eventlog
			Fork_Panama:  "2024-10-01",
			Fork_Arno:    "2025-01-21",
			Fork_Trinity: "2025-01-21",
		}, nil
	}
	return nil, errors.New("unsupported chain")
}

func (c *Config) GetEigenLayerGenesisBlockHeight() (uint64, error) {
	switch c.Chain {
	case Chain_Preprod, Chain_Holesky:
		return 1, nil
	case Chain_Mainnet:
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported chain %s", c.Chain)
	}
}

func (c *Config) GetOperatorRestakedStrategiesStartBlock() uint64 {
	switch c.Chain {
	case Chain_Preprod:
	case Chain_Holesky:
		return 1162800
	case Chain_Mainnet:
		return 19616400
	}
	return 0
}

func (c *Config) IsRewardsV2EnabledForCutoffDate(cutoffDate string) (bool, error) {
	forks, err := c.GetForkDates()
	if err != nil {
		return false, err
	}
	cutoffDateTime, err := time.Parse(time.DateOnly, cutoffDate)
	if err != nil {
		return false, errors.Join(fmt.Errorf("failed to parse cutoff date %s", cutoffDate), err)
	}
	arnoForkDateTime, err := time.Parse(time.DateOnly, forks[Fork_Arno])
	if err != nil {
		return false, errors.Join(fmt.Errorf("failed to parse Arno fork date %s", forks[Fork_Arno]), err)
	}

	return cutoffDateTime.Compare(arnoForkDateTime) >= 0, nil
}

// CanIgnoreIncorrectRewardsRoot returns true if the rewards root can be ignored for the given block number
//
// Due to inconsistencies in the rewards root calculation on testnet, we know that some roots
// are not fully reproducible and can be ignored.
func (c *Config) CanIgnoreIncorrectRewardsRoot(blockNumber uint64) bool {
	switch c.Chain {
	case Chain_Preprod:
		// roughly 2024-08-01
		if blockNumber < 2046020 {
			return true
		}
		// test root posted that was invalid for 2024-11-23 (cutoff date 2024-11-24)
		if blockNumber == 2812052 {
			return true
		}

		// ignore rewards-v2 deployment/testing range
		if blockNumber >= 2877938 && blockNumber <= 2909856 {
			return true
		}
	case Chain_Holesky:
		// roughly 2024-08-01
		if blockNumber < 2046020 {
			return true
		}
	case Chain_Mainnet:
	}
	return false
}

func KebabToSnakeCase(str string) string {
	return strings.ReplaceAll(str, "-", "_")
}

func GetEnvVarVar(key string) string {
	// replace . with _ and uppercase
	key = strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	return fmt.Sprintf("%s_%s", ENV_PREFIX, key)
}

func StringVarToInt(val string) int {
	v, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return v
}
