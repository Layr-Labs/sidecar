package ethereum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

const (
	EIP1967_STORAGE_SLOT = "0x360894A13BA1A3210667C828492DB98DCA3E2076CC3735A920A3CA505D382BBC"
)

type BlockType string

const (
	BlockType_Safe   BlockType = "safe"
	BlockType_Latest BlockType = "latest"
)

type RequestMethod struct {
	Name    string
	Timeout time.Duration
}

type RPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      uint   `json:"id"`
}

type RPCError struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *uint           `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type GetBlockByNumberResponse struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      uint           `json:"id"`
	Result  *EthereumBlock `json:"result"`
}

type ClientParams struct {
}

var jsonRPCVersion = "2.0"

type Client struct {
	Logger       *zap.Logger
	httpClient   *http.Client
	clientConfig *EthereumClientConfig
}

type EthereumClientConfig struct {
	BaseUrl              string
	UseNativeBatchCall   bool // Use the native eth_call method for batch calls
	NativeBatchCallSize  int  // Number of calls to put in a single eth_call request
	ChunkedBatchCallSize int  // Number of calls to make in parallel
	BlockType            BlockType
}

func convertStringBlockTypeToBlockType(blockType string) BlockType {
	switch blockType {
	case "latest":
		return BlockType_Latest
	case "safe":
	default:
		return BlockType_Safe
	}
	return BlockType_Safe
}

func ConvertGlobalConfigToEthereumConfig(cfg *config.EthereumRpcConfig) *EthereumClientConfig {
	return &EthereumClientConfig{
		BaseUrl:              cfg.BaseUrl,
		UseNativeBatchCall:   cfg.UseNativeBatchCall,
		NativeBatchCallSize:  cfg.NativeBatchCallSize,
		ChunkedBatchCallSize: cfg.ChunkedBatchCallSize,
		BlockType:            convertStringBlockTypeToBlockType(cfg.BlockType),
	}
}

func DefaultNativeCallEthereumClientConfig() *EthereumClientConfig {
	return &EthereumClientConfig{
		UseNativeBatchCall:   true,
		NativeBatchCallSize:  500,
		ChunkedBatchCallSize: 25,
		BlockType:            BlockType_Safe,
	}
}

func DefaultChunkedCallEthereumClientConfig() *EthereumClientConfig {
	return &EthereumClientConfig{
		UseNativeBatchCall:   false,
		NativeBatchCallSize:  500,
		ChunkedBatchCallSize: 25,
		BlockType:            BlockType_Safe,
	}
}

func NewClient(cfg *EthereumClientConfig, l *zap.Logger) *Client {
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	l.Sugar().Infow("Creating new Ethereum client", zap.Any("config", cfg))

	return &Client{
		httpClient:   client,
		Logger:       l,
		clientConfig: cfg,
	}
}

func (c *Client) SetHttpClient(client *http.Client) {
	c.httpClient = client
}

func (c *Client) GetEthereumContractCaller() (*ethclient.Client, error) {
	d, err := ethclient.Dial(c.clientConfig.BaseUrl)
	if err != nil {
		c.Logger.Sugar().Error("Failed to create new eth client", zap.Error(err))
		return nil, err
	}
	return d, nil
}

func (c *Client) ListenForNewBlocks(
	ctx context.Context,
	wsc *ethclient.Client,
	quitChan chan struct{},
	recvBlockHandler func(block *types.Header) error,
) error {
	ch := make(chan *types.Header)
	sub, err := wsc.SubscribeNewHead(ctx, ch)
	if err != nil {
		return err
	}

	defer close(ch)
	for {
		select {
		case err := <-sub.Err():
			c.Logger.Sugar().Errorw("Received error", zap.Error(err))
		case header := <-ch:
			err = recvBlockHandler(header)
			if err != nil {
				c.Logger.Sugar().Errorw("Failed to handle block on exit", zap.Error(err))
			}
		case <-quitChan:
			c.Logger.Sugar().Infow("Received quit")
			return nil
		}
	}
}

func (c *Client) GetBlockNumber(ctx context.Context) (string, error) {
	res, err := c.Call(ctx, GetBlockRequest(1))

	if err != nil {
		return "", err
	}

	return strings.ReplaceAll(string(res.Result), "\"", ""), nil
}

func (c *Client) GetBlockNumberUint64(ctx context.Context) (uint64, error) {
	blockNumber, err := c.GetBlockNumber(ctx)
	if err != nil {
		return 0, err
	}

	blockNumberUint64, err := hexutil.DecodeUint64(blockNumber)
	if err != nil {
		return 0, err
	}

	return blockNumberUint64, nil
}

func (c *Client) GetLatestBlock(ctx context.Context) (uint64, error) {
	var rpcRequest *RPCRequest
	if c.clientConfig.BlockType == BlockType_Latest {
		rpcRequest = GetLatestBlockRequest(1)
	} else {
		rpcRequest = GetSafeBlockRequest(1)
	}

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return 0, err
	}
	ethBlock, err := RPCMethod_getBlockByNumber.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to parse block",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return 0, err
	}
	return ethBlock.Number.Value(), nil
}

func (c *Client) GetBlockByNumber(ctx context.Context, blockNumber uint64) (*EthereumBlock, error) {
	rpcRequest := GetBlockByNumberRequest(blockNumber, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}
	ethBlock, err := RPCMethod_getBlockByNumber.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to parse block",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return nil, err
	}
	return ethBlock, nil
}

func (c *Client) GetTransactionByHash(ctx context.Context, txHash string) (*EthereumTransaction, error) {
	rpcRequest := GetTransactionByHashRequest(txHash, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}
	txReceipt, err := RPCMethod_getTransactionByHash.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to parse transaction",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return nil, err
	}
	return txReceipt, nil
}

// GetTransactionReceipt retrieves the transaction receipt for a given transaction hash.
func (c *Client) GetTransactionReceipt(ctx context.Context, txHash string) (*EthereumTransactionReceipt, error) {
	rpcRequest := GetTransactionReceiptRequest(txHash, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}
	txReceipt, err := RPCMethod_getTransactionReceipt.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to parse transaction receipt",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return nil, err
	}
	return txReceipt, nil
}

// GetBlockTransactionReceipts retrieves the transaction receipts for a given block number.
func (c *Client) GetBlockTransactionReceipts(ctx context.Context, blockNumber uint64) ([]*EthereumTransactionReceipt, error) {
	rpcRequest := GetBlockReceiptsRequest(blockNumber, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}
	txReceipts, err := RPCMethod_getBlockReceipts.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to parse transaction receipts",
			zap.Error(err),
		)
		return nil, err
	}
	return txReceipts, nil
}

func (c *Client) GetLogs(ctx context.Context, address string, fromBlock uint64, toBlock uint64) ([]*EthereumEventLog, error) {
	rpcRequest := GetLogsRequest(address, fromBlock, toBlock, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}
	logs, err := RPCMethod_getLogs.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to parse logs",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return nil, err
	}
	return logs, nil
}

func (c *Client) GetStorageAt(ctx context.Context, address string, storagePosition string, block string) (string, error) {
	rpcRequest := GetStorageAtRequest(address, storagePosition, block, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return "", err
	}
	storageValue, err := RPCMethod_getStorageAt.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to get storage value",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return "", err
	}
	return storageValue, nil
}

func (c *Client) GetCode(ctx context.Context, address string) (string, error) {
	rpcRequest := GetCodeRequest(address, 1)

	res, err := c.Call(ctx, rpcRequest)
	if err != nil {
		return "", err
	}
	bytecode, err := RPCMethod_getCode.ResponseParser(res.Result)
	if err != nil {
		c.Logger.Sugar().Errorw("failed to get contract bytecode",
			zap.Error(err),
			zap.Any("raw response", res.Result),
		)
		return "", err
	}
	return bytecode, nil
}

type BatchRPCRequest[T any] struct {
	Request *RPCRequest
	Handler ResponseParserFunc[T]
}

func (c *Client) batchCall(ctx context.Context, requests []*RPCRequest) ([]*RPCResponse, error) {
	if len(requests) == 0 {
		return make([]*RPCResponse, 0), nil
	}
	requestBody, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal requests: %s", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.clientConfig.BaseUrl, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("Failed to make request: %s", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Request failed %v", err)
	}

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read body %v", err)
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received http error code %+v", response.StatusCode)
	}

	destination := []*RPCResponse{}

	if strings.HasPrefix(string(responseBody), "{") {
		errorResponse := RPCResponse{}
		if err := json.Unmarshal(responseBody, &errorResponse); err != nil {
			return nil, fmt.Errorf("failed to unmarshal error response: %s", err)
		}
		c.Logger.Sugar().Debugw("Error payload returned from batch call",
			zap.String("error", string(responseBody)),
		)
		if errorResponse.Error.Message != "empty batch" {
			return nil, fmt.Errorf("Error payload returned from batch call: %s", string(responseBody))
		}
	} else {
		if err := json.Unmarshal(responseBody, &destination); err != nil {
			c.Logger.Sugar().Errorw("failed to unmarshal batch call response",
				zap.Error(err),
				zap.String("response", string(responseBody)),
			)
			return nil, fmt.Errorf("failed to unmarshal response: %s", err)
		}
	}
	response.Body.Close()

	return destination, nil
}

func (c *Client) chunkedNativeBatchCall(ctx context.Context, requests []*RPCRequest) ([]*RPCResponse, error) {
	if len(requests) == 0 {
		c.Logger.Sugar().Warnw("No requests to batch call")
		return make([]*RPCResponse, 0), nil
	}
	batches := [][]*RPCRequest{}

	currentIndex := 0
	for {
		endIndex := currentIndex + c.clientConfig.NativeBatchCallSize
		if endIndex >= len(requests) {
			endIndex = len(requests)
		}
		batches = append(batches, requests[currentIndex:endIndex])
		currentIndex = currentIndex + c.clientConfig.NativeBatchCallSize
		if currentIndex >= len(requests) {
			break
		}
	}
	c.Logger.Sugar().Debugw(fmt.Sprintf("Batching '%v' requests into '%v' batches", len(requests), len(batches)))

	resultsChan := make(chan []*RPCResponse, len(requests))
	wg := sync.WaitGroup{}
	for i, batch := range batches {
		wg.Add(1)

		go func(b []*RPCRequest) {
			defer wg.Done()

			c.Logger.Sugar().Debugw(fmt.Sprintf("[batch %d] Fetching batch with '%d' requests", i, len(b)))
			res, err := c.batchCall(ctx, b)
			if err != nil {
				c.Logger.Sugar().Errorw("failed to batch call", zap.Error(err))
				return
			}
			c.Logger.Sugar().Debugw(fmt.Sprintf("[batch %d] Received '%d' results", i, len(res)))
			resultsChan <- res
		}(batch)
	}
	wg.Wait()
	close(resultsChan)

	results := []*RPCResponse{}
	for res := range resultsChan {
		results = append(results, res...)
	}

	// ensure responses are sorted by ID
	slices.SortFunc(results, func(i, j *RPCResponse) int {
		return int(*i.ID - *j.ID)
	})

	c.Logger.Sugar().Debugw(fmt.Sprintf("Received '%d' results", len(results)))
	return results, nil
}

type IndexedRpcRequestResponse struct {
	Index    int
	Request  *RPCRequest
	Response *RPCResponse
}

type BatchedResponse struct {
	Index    int
	Response *RPCResponse
}

// chunkedBatchCall splits the requests into chunks of CHUNKED_BATCH_SIZE and sends them in parallel
// by calling the regular client.call method rather than relying on the batch call method.
//
// This function allows for better retry and error handling over the batch call method.
func (c *Client) chunkedBatchCall(ctx context.Context, requests []*RPCRequest) ([]*RPCResponse, error) {
	if len(requests) == 0 {
		c.Logger.Sugar().Warnw("No requests to batch call")
		return make([]*RPCResponse, 0), nil
	}
	batches := [][]*IndexedRpcRequestResponse{}

	// all requests in a flat list with their index stored
	orderedRequestResponses := make([]*IndexedRpcRequestResponse, 0)
	for i, req := range requests {
		orderedRequestResponses = append(orderedRequestResponses, &IndexedRpcRequestResponse{
			Index:   i,
			Request: req,
		})
	}

	currentIndex := 0
	for {
		endIndex := currentIndex + c.clientConfig.ChunkedBatchCallSize
		if endIndex >= len(orderedRequestResponses) {
			endIndex = len(orderedRequestResponses)
		}
		batches = append(batches, orderedRequestResponses[currentIndex:endIndex])
		currentIndex = currentIndex + c.clientConfig.ChunkedBatchCallSize
		if currentIndex >= len(orderedRequestResponses) {
			break
		}
	}
	c.Logger.Sugar().Debugw(fmt.Sprintf("Batching '%v' requests into '%v' batches", len(requests), len(batches)))

	// iterate over batches
	for i, batch := range batches {
		var wg sync.WaitGroup

		responses := make(chan BatchedResponse, len(batch))

		c.Logger.Sugar().Debugw(fmt.Sprintf("[batch %d] Fetching batch", i),
			zap.Int("batchRequests", len(batch)),
		)

		// Iterate over requests in the current batch.
		// For each batch, create a waitgroup for the go routines and a channel
		// to capture the responses. Once all are complete, we can safely iterate
		// over the responses and update the origin batch with the responses.
		for j, req := range batch {
			wg.Add(1)

			// capture loop variable to local scope
			currentReq := req

			go func() {
				defer wg.Done()

				res, err := c.Call(ctx, currentReq.Request)
				if err != nil {
					c.Logger.Sugar().Errorw(fmt.Sprintf("[%d][%d]failed to batch call", i, j),
						zap.Error(err),
						zap.Any("request", req.Request),
					)
					return
				}
				responses <- BatchedResponse{
					Index:    currentReq.Index,
					Response: res,
				}
			}()
		}
		wg.Wait()
		close(responses)

		// now we can safely iterate over the responses channel and update the batch
		for response := range responses {
			orderedRequestResponses[response.Index].Response = response.Response
		}
	}

	allResults := []*RPCResponse{}
	for _, req := range orderedRequestResponses {
		allResults = append(allResults, req.Response)
	}

	if len(allResults) != len(requests) {
		return nil, fmt.Errorf("Failed to fetch results for all requests. Expected %d, got %d", len(requests), len(allResults))
	}
	return allResults, nil
}

func (c *Client) BatchCall(ctx context.Context, requests []*RPCRequest) ([]*RPCResponse, error) {
	if len(requests) == 0 {
		c.Logger.Sugar().Warnw("No requests to batch call")
		return make([]*RPCResponse, 0), nil
	}
	if c.clientConfig.UseNativeBatchCall {
		return c.chunkedNativeBatchCall(ctx, requests)
	}
	return c.chunkedBatchCall(ctx, requests)
}

func (c *Client) call(ctx context.Context, rpcRequest *RPCRequest) (*RPCResponse, error) {
	requestBody, err := json.Marshal(rpcRequest)

	c.Logger.Sugar().Debug("Request body", zap.String("requestBody", string(requestBody)))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, RPCMethod_GetBlock.RequestMethod.Timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.clientConfig.BaseUrl, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("Failed to make request %s", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Request failed %s", err)
	}

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read body %s", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received http error code %+v", response.StatusCode)
	}

	destination := &RPCResponse{}
	if err := json.Unmarshal(responseBody, destination); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %s", err)
	}

	if destination.Error != nil {
		return nil, fmt.Errorf("received error response: %+v", destination.Error)
	}

	response.Body.Close()

	return destination, nil
}

func (c *Client) Call(ctx context.Context, rpcRequest *RPCRequest) (*RPCResponse, error) {
	backoffs := []int{1, 3, 5, 10, 20, 30, 60}

	for i, backoff := range backoffs {
		res, err := c.call(ctx, rpcRequest)
		if err == nil {
			if i > 0 {
				c.Logger.Sugar().Infow("Successfully called after backoff",
					zap.Int("backoffSecs", backoff),
					zap.Any("rpcRequest", rpcRequest),
				)
			}
			return res, nil
		}
		c.Logger.Sugar().Errorw("Failed to call",
			zap.Error(err),
			zap.Int("backoffSecs", backoff),
			zap.Any("rpcRequest", rpcRequest),
		)
		time.Sleep(time.Second * time.Duration(backoff))
	}
	c.Logger.Sugar().Errorw("Exceeded retries for Call", zap.Any("rpcRequest", rpcRequest))
	return nil, fmt.Errorf("Exceeded retries for Call")
}
