package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

const ENV_VAR_PREFIX = "SIDECAR"

// Options holds the configuration values from the command-line arguments and environment variables.
type Options struct {
	ChainConfig        string
	Debug              bool
	EthereumRPCBaseURL string
	EthereumWSURL      string
	StatsdUrl          string
	SqliteInMemory     bool
	SqliteDBFilePath   string
	RPCGRPCPort        int
	RPCHTTPPort        int
	EtherscanAPIKeys   string
}

func ParseArgs(args []string, envs map[string]string) (*Config, error) {
	opts, err := ParseArgsAndEnvironment(args, envs)
	if err != nil {
		return nil, err
	}
	if opts != nil {
		return NewConfig(opts), nil
	}
	return nil, nil
}

// ParseArgs parses command-line arguments and environment variables.
func ParseArgsAndEnvironment(args []string, envs map[string]string) (*Options, error) {
	hasResult := false
	opts := &Options{}

	var rootCmd = &cobra.Command{
		Use:   "sidecar",
		Short: "Sidecar indexer",
	}

	var runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the sidecar indexer",

		RunE: func(cmd *cobra.Command, cmdArgs []string) error {
			// Validate required flags
			if _, err := ParseChainConfig(opts.ChainConfig); err != nil {
				return fmt.Errorf("--chain is required: %v", err)
			}
			if opts.EthereumRPCBaseURL == "" && opts.EthereumWSURL == "" {
				return fmt.Errorf("--ethereum.rpc-base-url is required")
			}
			if opts.EtherscanAPIKeys == "" {
				return fmt.Errorf("--etherscan-api-keys is required")
			}
			hasResult = true
			return nil
		},
	}

	// Set flags with environment variable support
	runCmd.Flags().StringVar(&opts.ChainConfig, "chain", getPrefixedEnvVar(envs, "CHAIN", ""), "<ethereum | preprod | testnet | local> (required)")
	runCmd.Flags().StringVar(&opts.EthereumRPCBaseURL, "ethereum.rpc-base-url", getPrefixedEnvVar(envs, "ETHEREUM_RPC_BASE_URL", ""), "Ethereum RPC base URL (required)")
	runCmd.Flags().StringVar(&opts.EthereumWSURL, "ethereum.ws-url", getPrefixedEnvVar(envs, "ETHEREUM_WS_URL", ""), "Ethereum WebSocket URL (required)")
	runCmd.Flags().StringVar(&opts.StatsdUrl, "statsd-url", getPrefixedEnvVar(envs, "STATSD_URL", ""), "StatsD URL")
	runCmd.Flags().BoolVar(&opts.Debug, "debug", getPrefixedEnvBool(envs, "DEBUG", false), "Enable debug mode")
	runCmd.Flags().BoolVar(&opts.SqliteInMemory, "sqlite.in-memory", getPrefixedEnvBool(envs, "SQLITE_IN_MEMORY", false), "Use SQLite in-memory database")
	runCmd.Flags().StringVar(&opts.SqliteDBFilePath, "sqlite.db-file-path", getPrefixedEnvVar(envs, "SQLITE_DB_FILE_PATH", "./sqlite.db"), "SQLite database file path")
	runCmd.Flags().IntVar(&opts.RPCGRPCPort, "rpc.grpc-port", getPrefixedEnvInt(envs, "RPC_GRPC_PORT", 7100), "gRPC port")
	runCmd.Flags().IntVar(&opts.RPCHTTPPort, "rpc.http-port", getPrefixedEnvInt(envs, "RPC_HTTP_PORT", 7101), "HTTP port")
	runCmd.Flags().StringVar(&opts.EtherscanAPIKeys, "etherscan-api-keys", getPrefixedEnvVar(envs, "ETHERSCAN_API_KEYS", ""), "Etherscan API keys (required)")

	rootCmd.AddCommand(runCmd)

	// Set the command-line arguments
	rootCmd.SetArgs(args)

	// Execute the command
	if err := rootCmd.Execute(); err != nil {
		return nil, err
	}

	if hasResult {
		return opts, nil
	}
	// simple help message
	return nil, nil
}

// Helper functions to get environment variables with default values
func getPrefixedEnvVar(envs map[string]string, key string, default_value string) string {
	return getEnv(envs, fmt.Sprintf("%s_%s", ENV_VAR_PREFIX, key), default_value)
}

func getEnv(envs map[string]string, key string, defaultValue string) string {
	if value, exists := envs[key]; exists {
		return value
	}
	return defaultValue
}

func getPrefixedEnvInt(envs map[string]string, key string, defaultValue int) int {
	return getEnvInt(envs, fmt.Sprintf("%s_%s", ENV_VAR_PREFIX, key), defaultValue)
}

func getEnvInt(envs map[string]string, key string, defaultValue int) int {
	if valueStr, exists := envs[key]; exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return defaultValue
}

func getPrefixedEnvBool(envs map[string]string, key string, defaultValue bool) bool {
	return getEnvBool(envs, fmt.Sprintf("%s_%s", ENV_VAR_PREFIX, key), defaultValue)
}

func getEnvBool(envs map[string]string, key string, defaultValue bool) bool {
	if valueStr, exists := envs[key]; exists {
		if value, err := strconv.ParseBool(valueStr); err == nil {
			return value
		}
	}
	return defaultValue
}

func GetEnvAsMap() map[string]string {
	envMap := make(map[string]string)
	for _, e := range os.Environ() {
		pair := splitOnce(e, '=')
		envMap[pair[0]] = pair[1]
	}
	return envMap
}

func splitOnce(s string, sep rune) [2]string {
	for i, c := range s {
		if c == sep {
			return [2]string{s[:i], s[i+1:]}
		}
	}
	return [2]string{s, ""}
}
