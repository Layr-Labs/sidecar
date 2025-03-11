package ipfs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/btcsuite/btcutil/base58"
	"go.uber.org/zap"
)

type Ipfs struct {
	httpClient *http.Client
	logger     *zap.Logger
	config     *config.Config
}

type Response struct {
	Output struct {
		ABI json.RawMessage `json:"abi"` // Use json.RawMessage to capture the ABI JSON
	} `json:"output"`
}

func NewIpfs(hc *http.Client, l *zap.Logger, cfg *config.Config) *Ipfs {
	return &Ipfs{
		httpClient: hc,
		logger:     l,
		config:     cfg,
	}
}

func DefaultHttpClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
	}
}

func (ias *Ipfs) GetIPFSUrlFromBytecode(bytecode string) (string, error) {
	markerSequence := "a264697066735822"
	index := strings.Index(strings.ToLower(bytecode), markerSequence)

	if index == -1 {
		return "", fmt.Errorf("CBOR marker sequence not found")
	}

	// Extract the IPFS hash (34 bytes = 68 hex characters)
	startIndex := index + len(markerSequence)
	if len(bytecode) < startIndex+68 {
		return "", fmt.Errorf("bytecode too short to contain complete IPFS hash")
	}

	ipfsHash := bytecode[startIndex : startIndex+68]

	// Decode the hex string to bytes
	// Skip the 1220 prefix when decoding
	bytes, err := hex.DecodeString(ipfsHash)
	if err != nil {
		return "", fmt.Errorf("failed to decode IPFS hash: %v", err)
	}

	// Convert to base58
	base58Hash := base58.Encode(bytes)

	return fmt.Sprintf("%s/%s", ias.config.IpfsConfig.Url, base58Hash), nil
}

func (ias *Ipfs) FetchAbi(address string, bytecode string) (string, error) {
	url, err := ias.GetIPFSUrlFromBytecode(bytecode)
	if err != nil {
		ias.logger.Sugar().Errorw("Failed to get IPFS URL from bytecode",
			zap.Error(err),
			zap.String("address", address),
		)
		return "", err
	}
	ias.logger.Sugar().Debug("Successfully retrieved IPFS URL",
		zap.String("address", address),
		zap.String("ipfsUrl", url),
	)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		ias.logger.Sugar().Errorw("Failed to create a new HTTP request with context",
			zap.Error(err),
			zap.String("address", address),
		)
		return "", err
	}

	resp, err := ias.httpClient.Do(req)
	if err != nil {
		ias.logger.Sugar().Errorw("Failed to perform HTTP request",
			zap.Error(err),
			zap.String("address", address),
		)
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("gateway returned status: %d", resp.StatusCode)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result *Response
	if err := json.Unmarshal(content, &result); err != nil {
		ias.logger.Sugar().Errorw("Failed to parse json from IPFS URL content",
			zap.Error(err),
		)
		return "", err
	}

	ias.logger.Sugar().Debug("Successfully fetched ABI from IPFS",
		zap.String("address", address),
	)
	return string(result.Output.ABI), nil
}
