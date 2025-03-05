// Package utils provides utility functions and constants for common operations
// throughout the application.
package utils

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// Ethereum address constants
var (
	// NullEthereumAddress is the null Ethereum address without the 0x prefix
	NullEthereumAddress = "0000000000000000000000000000000000000000"
	
	// NullEthereumAddressHex is the null Ethereum address with the 0x prefix
	NullEthereumAddressHex = fmt.Sprintf("0x%s", NullEthereumAddress)
)

// AreAddressesEqual compares two Ethereum addresses for equality, ignoring case.
//
// Parameters:
//   - a: First Ethereum address
//   - b: Second Ethereum address
//
// Returns:
//   - bool: True if the addresses are equal (case-insensitive), false otherwise
func AreAddressesEqual(a, b string) bool {
	return strings.EqualFold(a, b)
}

// ConvertBytesToString converts a byte array to a hexadecimal string with 0x prefix.
//
// Parameters:
//   - b: Byte array to convert
//
// Returns:
//   - string: Hexadecimal string representation with 0x prefix
func ConvertBytesToString(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}

// SnakeCase converts a string to snake_case by replacing hyphens and other
// non-alphanumeric characters with underscores.
//
// Parameters:
//   - s: String to convert
//
// Returns:
//   - string: The input string converted to snake_case
func SnakeCase(s string) string {
	notSnake := regexp.MustCompile(`[_-]`)
	return notSnake.ReplaceAllString(s, "_")
}
