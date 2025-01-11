package utils

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	NullEthereumAddress    = "0000000000000000000000000000000000000000"
	NullEthereumAddressHex = fmt.Sprintf("0x%s", NullEthereumAddress)
)

func AreAddressesEqual(a, b string) bool {
	return strings.EqualFold(a, b)
}

func ConvertBytesToString(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}

func SnakeCase(s string) string {
	notSnake := regexp.MustCompile(`[_-]`)
	return notSnake.ReplaceAllString(s, "_")
}

// ExpandHomeDir expands the ~ in file paths to the user's home directory.
func ExpandHomeDir(path string) (string, error) {
	if strings.HasPrefix(path, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		path = filepath.Join(homeDir, path[2:])
	}
	return path, nil
}
