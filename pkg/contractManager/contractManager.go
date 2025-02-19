package contractManager

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"go.uber.org/zap"
	"github.com/mr-tron/base58"
	// "gorm.io/gorm/clause"
)

type ContractManager struct {
	ContractStore  contractStore.ContractStore
	EthereumClient *ethereum.Client
	metricsSink    *metrics.MetricsSink
	Logger         *zap.Logger
}

type Response struct {
    Output struct {
        ABI []map[string]interface{} `json:"abi"`  // Changed from string to array of maps
    } `json:"output"`
}

func NewContractManager(
	cs contractStore.ContractStore,
	e *ethereum.Client,
	ms *metrics.MetricsSink,
	l *zap.Logger,
) *ContractManager {
	return &ContractManager{
		ContractStore:  cs,
		EthereumClient: e,
		metricsSink:    ms,
		Logger:         l,
	}
}

func (cm *ContractManager) GetContractWithProxy(
	contractAddress string,
	blockNumber uint64,
) (*contractStore.ContractsTree, error) {
	cm.Logger.Sugar().Debugw(fmt.Sprintf("Getting contract for address '%s'", contractAddress))

	contract, err := cm.ContractStore.GetContractWithProxyContract(contractAddress, blockNumber)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to get contract for address", zap.Error(err), zap.String("contractAddress", contractAddress))
		return nil, err
	}

	return contract, nil
}

func GetMetadataURIFromBytecode(bytecode string) (string, error) {
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

func (cm *ContractManager) FetchAbiFromIPFS(ctx context.Context, address string) error {
	bytecode, err := cm.EthereumClient.GetCode(ctx, address)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to get the contract bytecode",
			zap.Error(err),
			zap.String("address", address),
		)
		// return nil, err
		return err
	}
	// fmt.Printf("bytecode: %s", bytecode)

	bytecodeHash := ethereum.HashBytecode(bytecode)
	cm.Logger.Sugar().Debug("Fetched the contract bytecode",
		zap.String("address", address),
		zap.String("bytecodeHash", bytecodeHash),
	)
	fmt.Printf("bytecodeHash: %s", bytecodeHash)

	uri, err := GetMetadataURIFromBytecode(bytecode)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to decode bytecode to IPFS",
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


func (cm *ContractManager) HandleContractUpgrade(ctx context.Context, blockNumber uint64, upgradedLog *parser.DecodedLog) error {
	// the new address that the contract points to
	newProxiedAddress := ""

	// Check the arguments for the new address. EIP-1967 contracts include this as an argument.
	// Otherwise, we'll check the storage slot
	for _, arg := range upgradedLog.Arguments {
		if arg.Name == "implementation" && arg.Value != "" && arg.Value != nil {
			newProxiedAddress = arg.Value.(common.Address).String()
			break
		}
	}

	if newProxiedAddress != "" {
		_, err := cm.CreateProxyContract(context.Background(), blockNumber, upgradedLog.Address, newProxiedAddress)
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to create proxy contract", zap.Error(err))
			return err
		}
		return nil
	}

	// check the storage slot at the provided block number of the transaction
	storageValue, err := idx.Fetcher.GetContractStorageSlot(context.Background(), upgradedLog.Address, blockNumber)
	if err != nil || storageValue == "" {
		idx.Logger.Sugar().Errorw("Failed to get storage value",
			zap.Error(err),
			zap.Uint64("block", blockNumber),
			zap.String("upgradedLogAddress", upgradedLog.Address),
		)
		return err
	}
	if len(storageValue) != 66 {
		idx.Logger.Sugar().Errorw("Invalid storage value",
			zap.Uint64("block", blockNumber),
			zap.String("storageValue", storageValue),
		)
		return err
	}

	newProxiedAddress = storageValue[26:]

	if newProxiedAddress == "" {
		idx.Logger.Sugar().Debugw("No new proxied address found", zap.String("address", upgradedLog.Address))
		return fmt.Errorf("No new proxied address found for %s during the 'Upgraded' event", upgradedLog.Address)
	}

	idx.Logger.Sugar().Infow("Upgraded proxy contract", zap.String("contractAddress", upgradedLog.Address), zap.String("proxyContractAddress", newProxiedAddress))
	return nil
}

// used in indexer.go
func (cm *ContractManager) CreateProxyContract(
	ctx context.Context,
	blockNumber uint64,
	contractAddress string,
	proxyContractAddress string,
) (*contractStore.ProxyContract, error) {
	// Check if proxy contract already exists
	proxyContract, _ := cm.ContractStore.GetProxyContractForAddress(blockNumber, contractAddress)
	if proxyContract != nil {
		cm.Logger.Sugar().Debugw("Found existing proxy contract when trying to create one",
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return proxyContract, nil
	}
	
	// Create a proxy contract
	proxyContract, err := cm.ContractStore.CreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create proxy contract",
			zap.Error(err),
			zap.String("contractAddress", contractAddress),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil, err
	}	

	// bytecode, err := cm.EthereumClient.GetCode(context.Background(), proxyContractAddress)
	// if err != nil {
	// 	cm.Logger.Sugar().Errorw("Failed to get proxy contract bytecode",
	// 		zap.Error(err),
	// 		zap.String("proxyContractAddress", proxyContractAddress),
	// 	)
	// 	return nil, err
	// }

	// bytecodeHash := ethereum.HashBytecode(bytecode)
	// cm.Logger.Sugar().Debug("Fetched proxy contract bytecode",
	// 	zap.String("proxyContractAddress", proxyContractAddress),
	// 	zap.String("bytecodeHash", bytecodeHash),
	// )

	// get all info and create contract
	_, err = cm.ContractStore.CreateContract(
		proxyContractAddress,
		"",
		true,
		"",
		"",
		true,
	)
	if err != nil {
		cm.Logger.Sugar().Errorw("Failed to create new contract for proxy contract",
			zap.Error(err),
			zap.String("proxyContractAddress", proxyContractAddress),
		)
		return nil, err
	} else {
		cm.Logger.Sugar().Debugf("Created new contract for proxy contract", zap.String("proxyContractAddress", proxyContractAddress))
	}
	
	return proxyContract, nil
}
