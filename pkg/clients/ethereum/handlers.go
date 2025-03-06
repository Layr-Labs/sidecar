package ethereum

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type ResponseParserFunc[T any] func(res json.RawMessage) (T, error)

type RequestResponseHandler[T any] struct {
	RequestMethod  *RequestMethod
	ResponseParser ResponseParserFunc[T]
}

var (
	RPCMethod_GetBlock = &RequestResponseHandler[string]{
		RequestMethod: &RequestMethod{
			Name:    "eth_blockNumber",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) (string, error) {
			return strings.ReplaceAll(string(res), "\"", ""), nil
		},
	}
	RPCMethod_getBlockByNumber = &RequestResponseHandler[*EthereumBlock]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getBlockByNumber",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) (*EthereumBlock, error) {
			block := &EthereumBlock{}

			if err := json.Unmarshal(res, block); err != nil {
				return nil, err
			}
			return block, nil
		},
	}
	RPCMethod_getTransactionByHash = &RequestResponseHandler[*EthereumTransaction]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getTransactionByHash",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) (*EthereumTransaction, error) {
			receipt := &EthereumTransaction{}

			if err := json.Unmarshal(res, receipt); err != nil {
				return nil, err
			}
			return receipt, nil
		},
	}
	RPCMethod_getTransactionReceipt = &RequestResponseHandler[*EthereumTransactionReceipt]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getTransactionReceipt",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) (*EthereumTransactionReceipt, error) {
			receipt := &EthereumTransactionReceipt{}

			if err := json.Unmarshal(res, receipt); err != nil {
				return nil, err
			}
			return receipt, nil
		},
	}
	RPCMethod_getStorageAt = &RequestResponseHandler[string]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getStorageAt",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) (string, error) {
			// https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getstorageat
			return strings.ReplaceAll(string(res), "\"", ""), nil
		},
	}
	RPCMethod_getCode = &RequestResponseHandler[string]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getCode",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) (string, error) {
			// https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getstorageat
			return strings.ReplaceAll(string(res), "\"", ""), nil
		},
	}
	RPCMethod_getBlockReceipts = &RequestResponseHandler[[]*EthereumTransactionReceipt]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getBlockReceipts",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) ([]*EthereumTransactionReceipt, error) {
			receipts := []*EthereumTransactionReceipt{}

			if err := json.Unmarshal(res, &receipts); err != nil {
				return nil, err
			}
			return receipts, nil
		},
	}
	RPCMethod_getLogs = &RequestResponseHandler[[]*EthereumEventLog]{
		RequestMethod: &RequestMethod{
			Name:    "eth_getLogs",
			Timeout: time.Second * 5,
		},
		ResponseParser: func(res json.RawMessage) ([]*EthereumEventLog, error) {
			logs := []*EthereumEventLog{}

			if err := json.Unmarshal(res, &logs); err != nil {
				return nil, err
			}
			return logs, nil
		},
	}
)

func GetBlockRequest(id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_GetBlock.RequestMethod.Name,
		ID:      id,
	}
}

func GetBlockByNumberRequest(blockNumber uint64, id uint) *RPCRequest {
	hexBlockNumber := hexutil.EncodeUint64(blockNumber)
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getBlockByNumber.RequestMethod.Name,
		Params:  []interface{}{hexBlockNumber, true},
		ID:      id,
	}
}

func GetSafeBlockRequest(id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getBlockByNumber.RequestMethod.Name,
		Params:  []interface{}{"safe", true},
		ID:      id,
	}
}

func GetLatestBlockRequest(id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getBlockByNumber.RequestMethod.Name,
		Params:  []interface{}{"latest", true},
		ID:      id,
	}
}

func GetTransactionByHashRequest(txHash string, id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getTransactionByHash.RequestMethod.Name,
		Params:  []interface{}{txHash},
		ID:      id,
	}
}

func GetTransactionReceiptRequest(txHash string, id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getTransactionReceipt.RequestMethod.Name,
		Params:  []interface{}{txHash},
		ID:      id,
	}
}

// https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getstorageat
// GetStorageAt gets the value stored at the given position and block of an address
//
// Block can be:
// - The hex representation of a block number
// - "earliest"
// - "latest"
// - "safe"
// - "finalized"
// - "pending".
func GetStorageAtRequest(address string, storagePosition string, block string, id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getStorageAt.RequestMethod.Name,
		Params:  []interface{}{address, storagePosition, block},
		ID:      id,
	}
}

func GetCodeRequest(address string, id uint) *RPCRequest {
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getCode.RequestMethod.Name,
		Params:  []interface{}{address, "latest"},
		ID:      id,
	}
}

func GetBlockReceiptsRequest(blockNumber uint64, id uint) *RPCRequest {
	hexBlockNumber := hexutil.EncodeUint64(blockNumber)
	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getBlockReceipts.RequestMethod.Name,
		Params:  []interface{}{hexBlockNumber},
		ID:      id,
	}
}

func GetLogsRequest(address string, fromBlock uint64, toBlock uint64, id uint) *RPCRequest {
	hexFromBlock := hexutil.EncodeUint64(fromBlock)
	hexToBlock := hexutil.EncodeUint64(toBlock)

	// Create a filter object as expected by eth_getLogs
	filter := map[string]interface{}{
		"address":   address,
		"fromBlock": hexFromBlock,
		"toBlock":   hexToBlock,
	}

	return &RPCRequest{
		JSONRPC: jsonRPCVersion,
		Method:  RPCMethod_getLogs.RequestMethod.Name,
		Params:  []interface{}{filter},
		ID:      id,
	}
}
