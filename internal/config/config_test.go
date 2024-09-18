package config

import (
	"testing"
)

func TestGetEnvironmentAsString(t *testing.T) {
	tests := []struct {
		input    Environment
		expected string
		hasError bool
	}{
		{Environment_PreProd, "preprod", false},
		{Environment_Testnet, "testnet", false},
		{Environment_Mainnet, "mainnet", false},
		{Environment_Local, "local", false},
		{Environment(999), "", true},
	}

	for _, test := range tests {
		result, err := GetEnvironmentAsString(test.input)
		if (err != nil) != test.hasError {
			t.Errorf("GetEnvironmentAsString(%v) error = %v, wantErr %v", test.input, err, test.hasError)
		}
		if result != test.expected {
			t.Errorf("GetEnvironment(%v) = %v, want %v", test.input, result, test.expected)
		}
	}
}

func TestGetNetwork(t *testing.T) {
	tests := []struct {
		input    Network
		expected string
		hasError bool
	}{
		{Network_Ethereum, "ethereum", false},
		{Network_Holesky, "holesky", false},
		{Network(999), "", true},
	}

	for _, test := range tests {
		result, err := GetNetworkAsString(test.input)
		if (err != nil) != test.hasError {
			t.Errorf("GetNetworkAsString(%v) error = %v, wantErr %v", test.input, err, test.hasError)
		}
		if result != test.expected {
			t.Errorf("GetNetwork(%v) = %v, want %v", test.input, result, test.expected)
		}
	}
}

func TestParseStringAsList(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{}},
		{"a,b,c", []string{"a", "b", "c"}},
		{"a, b, c", []string{"a", "b", "c"}},
		{"a,,c", []string{"a", "c"}},
	}

	for _, test := range tests {
		result := parseStringAsList(test.input)
		if len(result) != len(test.expected) {
			t.Errorf("parseStringAsList(%s) = %v, want %v", test.input, result, test.expected)
		}
		for i, v := range result {
			if v != test.expected[i] {
				t.Errorf("parseStringAsList(%s)[%d] = %v, want %v", test.input, i, v, test.expected[i])
			}
		}
	}
}

func TestGetSqlitePath(t *testing.T) {
	tests := []struct {
		config   SqliteConfig
		expected string
	}{
		{SqliteConfig{InMemory: true}, "file::memory:?cache=shared"},
		{SqliteConfig{InMemory: false, DbFilePath: "/path/to/db"}, "/path/to/db"},
	}

	for _, test := range tests {
		result := test.config.GetSqlitePath()
		if result != test.expected {
			t.Errorf("GetSqlitePath() = %v, want %v", result, test.expected)
		}
	}
}

func TestParseChainConfig(t *testing.T) {
	tests := []struct {
		input    string
		expected ChainConfig
		hasError bool
	}{
		{"ethereum", ChainConfig{"ethereum", Network_Ethereum, Environment_Mainnet}, false},
		{"testnet", ChainConfig{"testnet", Network_Holesky, Environment_Testnet}, false},
		{"preprod", ChainConfig{"preprod", Network_Holesky, Environment_PreProd}, false},
		{"local", ChainConfig{"local", Network_Holesky, Environment_Local}, false},
		{"unknown", ChainConfig{}, true},
		{"", ChainConfig{}, true},
	}

	for _, test := range tests {
		result, err := ParseChainConfig(test.input)
		if (err != nil) != test.hasError {
			t.Errorf("ParseChainConfig(%s) error = %v, wantErr %v", test.input, err, test.hasError)
		}
		if result != test.expected {
			t.Errorf("ParseChainConfig(%s) = %v, want %v", test.input, result, test.expected)
		}
	}
}
