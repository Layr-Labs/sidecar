package abiFetcher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

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
        ABI []map[string]interface{} `json:"abi"`  // Changed from string to array of maps
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

func (af *AbiFetcher) GetMetadataURIFromBytecode(bytecode string) (string, error) {
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
        return "", fmt.Errorf("failed to decode hex: %v", err)
    }
    
    // Convert to base58
    base58Hash := base58.Encode(bytes)
    
    // Add Qm prefix and ipfs:// protocol
    return fmt.Sprintf("https://ipfs.io/ipfs/%s", base58Hash), nil
}

func (af *AbiFetcher) FetchAbiFromIPFS(ctx context.Context, address string) error {
	bytecode, err := af.EthereumClient.GetCode(ctx, address)
	if err != nil {
		af.Logger.Sugar().Errorw("Failed to get the contract bytecode",
			zap.Error(err),
			zap.String("address", address),
		)
		// return nil, err
		return err
	}
	// fmt.Printf("bytecode: %s", bytecode)

	bytecodeHash := ethereum.HashBytecode(bytecode)
	af.Logger.Sugar().Debug("Fetched the contract bytecode",
		zap.String("address", address),
		zap.String("bytecodeHash", bytecodeHash),
	)
	fmt.Printf("bytecodeHash: %s", bytecodeHash)

	uri, err := af.GetMetadataURIFromBytecode(bytecode)
	if err != nil {
		af.Logger.Sugar().Errorw("Failed to decode bytecode to IPFS",
			zap.Error(err),
			zap.String("address", address),
		)
		// return nil, err
		return err
	}
	fmt.Printf("IPFS URI: %s \n", uri)

	resp, err := http.Get(uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gateway returned status: %d", resp.StatusCode)
	}
	
	// Read response
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var result Response
	if err := json.Unmarshal(content, &result); err != nil {
        fmt.Printf("Error parsing JSON: %v\n", err)
        return err
    }
	
	fmt.Printf("abi: %s \n", result.Output.ABI)
	return nil
}
