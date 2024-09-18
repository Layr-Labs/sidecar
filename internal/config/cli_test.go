package config

import (
	"reflect"
	"testing"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envs      map[string]string
		want      *Options
		expectErr bool
		errorMsg  string
	}{
		{
			name: "All required flags provided via command line",
			args: []string{
				"run",
				"--network", "mainnet",
				"--environment", "mainnet",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{},
			want: &Options{
				Network:            "mainnet",
				Environment:        "mainnet",
				EthereumRPCBaseURL: "http://localhost:8545",
				EthereumWSURL:      "ws://localhost:8546",
				SqliteInMemory:     false,
				SqliteDBFilePath:   "./sqlite.db",
				RPCGRPCPort:        7100,
				RPCHTTPPort:        7101,
				EtherscanAPIKeys:   "your-api-key",
			},
			expectErr: false,
		},
		{
			name: "Missing required flag --network",
			args: []string{
				"run",
				"--environment", "preprod",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs:      map[string]string{},
			expectErr: true,
			errorMsg:  "--network is required: network not found",
		},
		{
			name: "Invalid network",
			args: []string{
				"run",
				"--network", "preprod",
				"--environment", "preprod",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs:      map[string]string{},
			expectErr: true,
			errorMsg:  "--network is required: unsupported network preprod",
		},
		{
			name: "environment testnet in mainnet",
			args: []string{
				"run",
				"--network", "mainnet",
				"--environment", "testnet",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
			},
			envs:      map[string]string{},
			expectErr: true,
			errorMsg:  "mainnet environment required",
		},
		{
			name: "environment preprod in mainnet",
			args: []string{
				"run",
				"--network", "mainnet",
				"--environment", "preprod",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
			},
			envs:      map[string]string{},
			expectErr: true,
			errorMsg:  "mainnet environment required",
		},
		{
			name: "Default values are applied",
			args: []string{
				"run",
				"--network", "holesky",
				"--environment", "preprod",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{},
			want: &Options{
				Network:            "holesky",
				Environment:        "preprod",
				EthereumRPCBaseURL: "http://localhost:8545",
				EthereumWSURL:      "ws://localhost:8546",
				SqliteInMemory:     false,
				SqliteDBFilePath:   "./sqlite.db",
				RPCGRPCPort:        7100,
				RPCHTTPPort:        7101,
				EtherscanAPIKeys:   "your-api-key",
			},
			expectErr: false,
		},
		{
			name: "Flags overridden by environment variables",
			args: []string{
				"run",
				"--network", "holesky",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{
				"SIDECAR_ENVIRONMENT":           "preprod",
				"SIDECAR_ETHEREUM_RPC_BASE_URL": "http://env-rpc:8545",
				"SIDECAR_ETHEREUM_WS_URL":       "ws://env-ws:8546",
				"SIDECAR_SQLITE_IN_MEMORY":      "true",
				"SIDECAR_RPC_GRPC_PORT":         "8100",
				"SIDECAR_RPC_HTTP_PORT":         "8101",
			},
			want: &Options{
				Network:            "holesky",
				Environment:        "preprod",
				EthereumRPCBaseURL: "http://env-rpc:8545",
				EthereumWSURL:      "ws://env-ws:8546",
				SqliteInMemory:     true,
				SqliteDBFilePath:   "./sqlite.db",
				RPCGRPCPort:        8100,
				RPCHTTPPort:        8101,
				EtherscanAPIKeys:   "your-api-key",
			},
			expectErr: false,
		},
		{
			name: "Invalid integer in environment variable",
			args: []string{
				"run",
				"--network", "mainnet",
				"--environment", "mainnet",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{
				"SIDECAR_RPC_GRPC_PORT": "not-an-int",
			},
			want: &Options{
				Network:            "mainnet",
				Environment:        "mainnet",
				EthereumRPCBaseURL: "http://localhost:8545",
				EthereumWSURL:      "ws://localhost:8546",
				SqliteInMemory:     false,
				SqliteDBFilePath:   "./sqlite.db",
				RPCGRPCPort:        7100, // Default value because of parsing error
				RPCHTTPPort:        7101,
				EtherscanAPIKeys:   "your-api-key",
			},
			expectErr: false,
		},
		{
			name: "Invalid boolean in environment variable",
			args: []string{
				"run",
				"--network", "mainnet",
				"--environment", "mainnet",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{
				"SIDECAR_SQLITE_IN_MEMORY": "not-a-bool",
			},
			want: &Options{
				Network:            "mainnet",
				Environment:        "mainnet",
				EthereumRPCBaseURL: "http://localhost:8545",
				EthereumWSURL:      "ws://localhost:8546",
				SqliteInMemory:     false, // Default value because of parsing error
				SqliteDBFilePath:   "./sqlite.db",
				RPCGRPCPort:        7100,
				RPCHTTPPort:        7101,
				EtherscanAPIKeys:   "your-api-key",
			},
			expectErr: false,
		},
		{
			name: "No arguments or environment variables",
			args: []string{"run"},
			envs: map[string]string{},
			want: &Options{
				SqliteInMemory:   false,
				SqliteDBFilePath: "./sqlite.db",
				RPCGRPCPort:      7100,
				RPCHTTPPort:      7101,
			},
			expectErr: true,
			errorMsg:  "--network is required: network not found",
		},
		{
			name: "Environment variable with empty value",
			args: []string{
				"run",
				"--network", "holesky",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{},
			want: &Options{
				Network:            "holesky",
				EthereumRPCBaseURL: "http://localhost:8545",
				EthereumWSURL:      "ws://localhost:8546",
				SqliteInMemory:     false,
			},
			expectErr: true,
			errorMsg:  "--environment is required: environment not found",
		},
		{
			name: "Environment variable not required for mainnet",
			args: []string{
				"run",
				"--network", "mainnet",
				"--ethereum.rpc-base-url", "http://localhost:8545",
				"--ethereum.ws-url", "ws://localhost:8546",
				"--etherscan-api-keys", "your-api-key",
			},
			envs: map[string]string{},
			want: &Options{
				Network:            "mainnet",
				Environment:        "mainnet",
				Debug:              false,
				EthereumRPCBaseURL: "http://localhost:8545",
				EthereumWSURL:      "ws://localhost:8546",
				SqliteInMemory:     false,
				SqliteDBFilePath:   "./sqlite.db",
				RPCGRPCPort:        7100,
				RPCHTTPPort:        7101,
				EtherscanAPIKeys:   "your-api-key",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := ParseArgsAndEnvironment(tt.args, tt.envs)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("Expected error but got nil")
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Fatalf("Expected error message '%s', but got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !reflect.DeepEqual(opts, tt.want) {
					t.Errorf("Options mismatch.\nGot:  %+v\nWant: %+v", opts, tt.want)
				}
			}
		})
	}
}

func Test_getEnv(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		key      string
		defaults string
		want     string
	}{
		{
			name:     "Key exists",
			envs:     map[string]string{"KEY": "value"},
			key:      "KEY",
			defaults: "default",
			want:     "value",
		},
		{
			name:     "Key does not exist",
			envs:     map[string]string{},
			key:      "KEY",
			defaults: "default",
			want:     "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getEnv(tt.envs, tt.key, tt.defaults); got != tt.want {
				t.Errorf("getEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getEnvBool(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		key      string
		defaults bool
		want     bool
	}{
		{
			name:     "Key exists",
			envs:     map[string]string{"KEY": "true"},
			key:      "KEY",
			defaults: false,
			want:     true,
		},
		{
			name:     "Key does not exist",
			envs:     map[string]string{},
			key:      "KEY",
			defaults: true,
			want:     true,
		},
		{
			name:     "Invalid value",
			envs:     map[string]string{"KEY": "not-a-bool"},
			key:      "KEY",
			defaults: true,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getEnvBool(tt.envs, tt.key, tt.defaults); got != tt.want {
				t.Errorf("getEnvBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getEnvInt(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		key      string
		defaults int
		want     int
	}{
		{
			name:     "Key exists",
			envs:     map[string]string{"KEY": "42"},
			key:      "KEY",
			defaults: 0,
			want:     42,
		},
		{
			name:     "Key does not exist",
			envs:     map[string]string{},
			key:      "KEY",
			defaults: 42,
			want:     42,
		},
		{
			name:     "Invalid value",
			envs:     map[string]string{"KEY": "not-an-int"},
			key:      "KEY",
			defaults: 42,
			want:     42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getEnvInt(tt.envs, tt.key, tt.defaults); got != tt.want {
				t.Errorf("getEnvInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getPrefixedEnvVar(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		key      string
		defaults string
		want     string
	}{
		{
			name:     "Key exists",
			envs:     map[string]string{"SIDECAR_KEY": "value"},
			key:      "KEY",
			defaults: "default",
			want:     "value",
		},
		{
			name:     "Key does not exist",
			envs:     map[string]string{},
			key:      "KEY",
			defaults: "default",
			want:     "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getPrefixedEnvVar(tt.envs, tt.key, tt.defaults); got != tt.want {
				t.Errorf("getPrefixedEnvVar() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getPrefixedEnvInt(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		key      string
		defaults int
		want     int
	}{
		{
			name:     "Key exists",
			envs:     map[string]string{"SIDECAR_KEY": "42"},
			key:      "KEY",
			defaults: 0,
			want:     42,
		},
		{
			name:     "Key does not exist",
			envs:     map[string]string{},
			key:      "KEY",
			defaults: 42,
			want:     42,
		},
		{
			name:     "Invalid value",
			envs:     map[string]string{"SIDECAR_KEY": "not-an-int"},
			key:      "KEY",
			defaults: 42,
			want:     42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getPrefixedEnvInt(tt.envs, tt.key, tt.defaults); got != tt.want {
				t.Errorf("getPrefixedEnvInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getPrefixedEnvBool(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		key      string
		defaults bool
		want     bool
	}{
		{
			name:     "Key exists",
			envs:     map[string]string{"SIDECAR_KEY": "true"},
			key:      "KEY",
			defaults: false,
			want:     true,
		},
		{
			name:     "Key does not exist",
			envs:     map[string]string{},
			key:      "KEY",
			defaults: true,
			want:     true,
		},
		{
			name:     "Invalid value",
			envs:     map[string]string{"SIDECAR_KEY": "not-a-bool"},
			key:      "KEY",
			defaults: true,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getPrefixedEnvBool(tt.envs, tt.key, tt.defaults); got != tt.want {
				t.Errorf("getPrefixedEnvBool() = %v, want %v", got, tt.want)
			}
		})
	}
}
