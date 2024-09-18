package config

import (
	"testing"
)

func TestParseNetwork(t *testing.T) {
	tests := []struct {
		input    string
		expected Network
		hasError bool
	}{
		{"mainnet", Network_Ethereum, false},
		{"holesky", Network_Holesky, false},
		{"", Network_Holesky, true},
		{"unknown", Network_Holesky, true},
	}

	for _, test := range tests {
		result, err := ParseNetwork(test.input)
		if (err != nil) != test.hasError {
			t.Errorf("ParseNetwork(%s) error = %v, wantErr %v", test.input, err, test.hasError)
		}
		if result != test.expected {
			t.Errorf("ParseNetwork(%s) = %v, want %v", test.input, result, test.expected)
		}
	}
}

func TestParseEnvironment(t *testing.T) {
	tests := []struct {
		input    string
		expected Environment
		hasError bool
	}{
		{"preprod", Environment_PreProd, false},
		{"testnet", Environment_Testnet, false},
		{"mainnet", Environment_Mainnet, false},
		{"local", Environment_Local, false},
		{"", Environment_PreProd, true},
		{"unknown", Environment_PreProd, true},
	}

	for _, test := range tests {
		result, err := parseEnvironment(test.input)
		if (err != nil) != test.hasError {
			t.Errorf("parseEnvironment(%s) error = %v, wantErr %v", test.input, err, test.hasError)
		}
		if result != test.expected {
			t.Errorf("parseEnvironment(%s) = %v, want %v", test.input, result, test.expected)
		}
	}
}

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
