package abiFetcher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/btcsuite/btcutil/base58"
	"go.uber.org/zap"
)

type AbiFetcher struct {
	EthereumClient *ethereum.Client
	Logger         *zap.Logger
}

type Response struct {
    Output struct {
        ABI json.RawMessage `json:"abi"` // Use json.RawMessage to capture the ABI JSON
    } `json:"output"`
}

func NewAbiFetcher(
	e *ethereum.Client,
	l *zap.Logger,
) *AbiFetcher {
	return &AbiFetcher{
		EthereumClient: e,
		Logger:         l,
	}
}

func (af *AbiFetcher) FetchMetadataFromAddress(ctx context.Context, address string) (string, string, error) {
	bytecode, err := af.EthereumClient.GetCode(ctx, address)
	if err != nil {
		af.Logger.Sugar().Errorw("Failed to get the contract bytecode",
			zap.Error(err),
			zap.String("address", address),
		)
		return "", "", err
	}

	bytecodeHash := ethereum.HashBytecode(bytecode)
	af.Logger.Sugar().Debug("Fetched the contract bytecodeHash",
		zap.String("address", address),
		zap.String("bytecodeHash", bytecodeHash),
	)
	fmt.Printf("bytecodeHash: %s", bytecodeHash)

	// fetch ABI using IPFS
	// TODO: add a fallback method using Etherscan
	abi, err := af.FetchAbiFromIPFS(address, bytecode)
	if err != nil {
		af.Logger.Sugar().Errorw("Failed to fetch ABI from IPFS",
			zap.Error(err),
			zap.String("address", address),
		)
		return "", "", err
	}
	
	return bytecodeHash, abi, nil
}

func (af *AbiFetcher) GetIPFSUrlFromBytecode(bytecode string) (string, error) {
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
    
    ipfsHash := bytecode[startIndex:startIndex+68]
    
    // Decode the hex string to bytes
    // Skip the 1220 prefix when decoding
    bytes, err := hex.DecodeString(ipfsHash)
    if err != nil {
        return "", fmt.Errorf("failed to decode IPFS hash: %v", err)
    }
    
    // Convert to base58
    base58Hash := base58.Encode(bytes)
    
    return fmt.Sprintf("https://ipfs.io/ipfs/%s", base58Hash), nil
}

func (af *AbiFetcher) FetchAbiFromIPFS(address string, bytecode string) (string, error) {
	url, err := af.GetIPFSUrlFromBytecode(bytecode)
	if err != nil {
		af.Logger.Sugar().Errorw("Failed to get IPFS URL from bytecode",
			zap.Error(err),
			zap.String("address", address),
		)
		return "", err
	}
	af.Logger.Sugar().Debug("Successfully retrieved IPFS URL",
		zap.String("address", address),
		zap.String("IPFS URL", url),
	)
	fmt.Printf("IPFS URL: %s \n", url)

    httpClient := &http.Client{
        Timeout: 5 * time.Second,
    }

    req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
    if err != nil {
        af.Logger.Sugar().Errorw("Failed to create a new HTTP request with context",
            zap.Error(err),
            zap.String("address", address),
        )
        return "", err
    }

    resp, err := httpClient.Do(req)
    if err != nil {
        af.Logger.Sugar().Errorw("Failed to perform HTTP request",
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

	var result Response
	if err := json.Unmarshal(content, &result); err != nil {
		af.Logger.Sugar().Errorw("Failed to parse json from IPFS URL content",
			zap.Error(err),
		)
        return "", err
    }

	af.Logger.Sugar().Debug("Successfully fetched ABI from IPFS",
		zap.String("address", address),
	)
	return string(result.Output.ABI), nil
}
