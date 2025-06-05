// Package fetcher provides functionality for retrieving blocks and transaction data
// from Ethereum nodes. It handles batch requests, retries, and parallel processing
// to efficiently fetch blockchain data.
package fetcher

import (
	"context"
	"github.com/Layr-Labs/sidecar/pkg/providers"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"slices"
	"sync"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// FetcherConfig contains the configuration specific to the Fetcher
type FetcherConfig struct {
	// UseGetBlockReceipts determines whether to use eth_getBlockReceipts RPC method
	// instead of fetching receipts for each transaction individually
	UseGetBlockReceipts bool
}

// Fetcher is responsible for retrieving blockchain data from Ethereum nodes.
// It provides methods for fetching blocks and transaction receipts.
type Fetcher struct {
	// EthClient is used to communicate with Ethereum nodes
	EthClient *ethereum.Client
	// Logger is used for logging fetch operations
	Logger *zap.Logger
	// FetcherConfig contains the configuration specific to the Fetcher
	FetcherConfig *FetcherConfig

	InterestingLogsProvider providers.InterestingContractsProvider
}

// NewFetcher creates a new Fetcher with the provided Ethereum client, configuration, and logger.
func NewFetcher(
	ethClient *ethereum.Client,
	cfg *FetcherConfig,
	ilp providers.InterestingContractsProvider,
	l *zap.Logger,
) *Fetcher {
	l.Sugar().Infow("Created fetcher", zap.Any("config", cfg))
	return &Fetcher{
		EthClient:               ethClient,
		Logger:                  l,
		FetcherConfig:           cfg,
		InterestingLogsProvider: ilp,
	}
}

// FetchedBlock represents a block and its associated transaction receipts.
// It combines the block data with detailed information about each transaction.
type FetchedBlock struct {
	// Block contains the basic block information
	Block *ethereum.EthereumBlock
	// TxReceipts maps transaction hashes to their receipts
	TxReceipts map[string]*ethereum.EthereumTransactionReceipt
}

// FetchBlock retrieves a single block and its transaction receipts by block number.
// It returns a FetchedBlock containing the block and receipt data.
func (f *Fetcher) FetchBlock(ctx context.Context, blockNumber uint64) (*FetchedBlock, error) {
	block, err := f.EthClient.GetBlockByNumber(ctx, blockNumber)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to get block by number", zap.Error(err))
		return nil, err
	}

	receipts, err := f.GetReceiptsForBlock(ctx, block)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to fetch receipts for block", zap.Error(err))
		return nil, err
	}

	return &FetchedBlock{
		Block:      block,
		TxReceipts: receipts,
	}, nil
}

// FetchReceiptsForBlock retrieves transaction receipts for all transactions in a block.
// It uses batch requests to efficiently fetch multiple receipts at once.
// Returns a map of transaction hashes to their receipts.
func (f *Fetcher) FetchReceiptsForBlock(ctx context.Context, block *ethereum.EthereumBlock) (map[string]*ethereum.EthereumTransactionReceipt, error) {
	blockNumber := block.Number.Value()

	txReceiptRequests := make([]*ethereum.RPCRequest, 0)
	f.Logger.Sugar().Debugf("Fetching '%d' transactions from block '%d'", len(block.Transactions), blockNumber)

	for i, tx := range block.Transactions {
		txReceiptRequests = append(txReceiptRequests, ethereum.GetTransactionReceiptRequest(tx.Hash.Value(), uint(i)))
	}

	f.Logger.Sugar().Debugw("Fetching transaction receipts",
		zap.Int("count", len(txReceiptRequests)),
		zap.Uint64("blockNumber", blockNumber),
	)
	receipts := make(map[string]*ethereum.EthereumTransactionReceipt)

	if len(txReceiptRequests) == 0 {
		return receipts, nil
	}

	receiptResponses, err := f.EthClient.BatchCall(ctx, txReceiptRequests)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to batch call for transaction receipts", zap.Error(err))
		return nil, err
	}

	// Ensure that we have all the receipts.
	if len(receiptResponses) != len(txReceiptRequests) {
		f.Logger.Sugar().Errorw("failed to fetch all transaction receipts",
			zap.Int("fetched", len(receiptResponses)),
			zap.Int("expected", len(txReceiptRequests)),
		)
		return nil, errors.New("failed to fetch all transaction receipts")
	}

	for _, response := range receiptResponses {
		r, err := ethereum.RPCMethod_getTransactionReceipt.ResponseParser(response.Result)
		if err != nil {
			f.Logger.Sugar().Errorw("failed to parse transaction receipt",
				zap.Error(err),
				zap.Uint("response ID", *response.ID),
			)
			return nil, err
		}
		receipts[r.TransactionHash.Value()] = r
	}
	return receipts, nil
}

func findMissingReceipts(block *ethereum.EthereumBlock, receipts []*ethereum.EthereumTransactionReceipt) []string {
	missingReceipts := make([]string, 0)
	memoizedExpectedReceipts := make(map[string]bool)
	memoizedReceivedReceipts := make(map[string]bool)

	for _, receipt := range receipts {
		memoizedReceivedReceipts[receipt.TransactionHash.Value()] = true
	}

	for _, tx := range block.Transactions {
		memoizedExpectedReceipts[tx.Hash.Value()] = true
	}

	// find receipts that were received but not expected
	for _, receipt := range receipts {
		if _, ok := memoizedExpectedReceipts[receipt.TransactionHash.Value()]; !ok {
			missingReceipts = append(missingReceipts, receipt.TransactionHash.Value())
		}
	}

	// find receipts that were expected but not received
	for _, tx := range block.Transactions {
		if _, ok := memoizedReceivedReceipts[tx.Hash.Value()]; !ok {
			missingReceipts = append(missingReceipts, tx.Hash.Value())
		}
	}
	return missingReceipts
}

// FetchBlockReceipts retrieves transaction receipts for all transactions in a block using the eth_getBlockReceipts RPC method
// rather than iterating over a list of transactions and fetching each receipt individually.
func (f *Fetcher) FetchBlockReceipts(ctx context.Context, block *ethereum.EthereumBlock) (map[string]*ethereum.EthereumTransactionReceipt, error) {
	receipts, err := f.EthClient.GetBlockTransactionReceipts(ctx, block.Number.Value())
	if err != nil {
		f.Logger.Sugar().Errorw("failed to get block receipts", zap.Error(err))
		return nil, err
	}

	if len(receipts) != len(block.Transactions) {
		f.Logger.Sugar().Errorw("failed to fetch all transaction receipts",
			zap.Int("fetched", len(receipts)),
			zap.Int("expected", len(block.Transactions)),
		)

		missing := findMissingReceipts(block, receipts)
		f.Logger.Sugar().Errorw("missing receipts",
			zap.Int("count", len(missing)),
			zap.Strings("missing", missing),
		)

		return nil, errors.New("failed to fetch all transaction receipts")
	}

	receiptsMap := make(map[string]*ethereum.EthereumTransactionReceipt)
	for _, r := range receipts {
		receiptsMap[r.TransactionHash.Value()] = r
	}
	return receiptsMap, nil
}

func (f *Fetcher) GetReceiptsForBlock(ctx context.Context, block *ethereum.EthereumBlock) (map[string]*ethereum.EthereumTransactionReceipt, error) {
	if f.FetcherConfig.UseGetBlockReceipts {
		return f.FetchBlockReceipts(ctx, block)
	}
	return f.FetchReceiptsForBlock(ctx, block)
}

// FetchBlocksWithRetries attempts to fetch a range of blocks with exponential backoff retries.
// It will retry failed requests with increasing delays before giving up.
// Returns an array of FetchedBlock objects for the requested range.
func (f *Fetcher) FetchBlocksWithRetries(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64) ([]*FetchedBlock, error) {
	retries := []int{1, 2, 4, 8, 16, 32, 64}
	var e error
	for i, r := range retries {
		fetchedBlocks, err := f.FetchBlocksWithReceipts(ctx, startBlockInclusive, endBlockInclusive)
		if err == nil {
			if i > 0 {
				f.Logger.Sugar().Infow("successfully fetched blocks for range after retries",
					zap.Uint64("startBlock", startBlockInclusive),
					zap.Uint64("endBlock", endBlockInclusive),
					zap.Int("retries", i),
				)
			}
			return fetchedBlocks, nil
		}
		e = err
		f.Logger.Sugar().Infow("failed to fetch blocks for range",
			zap.Uint64("startBlock", startBlockInclusive),
			zap.Uint64("endBlock", endBlockInclusive),
			zap.Int("sleepTime", r),
		)

		time.Sleep(time.Duration(r) * time.Second)
	}
	f.Logger.Sugar().Errorw("failed to fetch blocks for range, exhausted all retries",
		zap.Uint64("startBlock", startBlockInclusive),
		zap.Uint64("endBlock", endBlockInclusive),
		zap.Error(e),
	)
	return nil, e
}
func (f *Fetcher) FetchLogsForContractsForBlockRange(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64, contractAddresses []string) ([]uint64, error) {
	f.Logger.Sugar().Debugw("Fetching logs for contracts",
		zap.Uint64("startBlock", startBlockInclusive),
		zap.Uint64("endBlock", endBlockInclusive),
	)
	logsCollector := make(chan []*ethereum.EthereumEventLog, len(contractAddresses))
	errorCollector := make(chan error, len(contractAddresses))

	wg := &sync.WaitGroup{}
	for _, contractAddress := range contractAddresses {
		wg.Add(1)
		go func(contractAddress string) {
			defer wg.Done()
			f.Logger.Sugar().Debugw("Fetching logs for contract",
				zap.Uint64("startBlock", startBlockInclusive),
				zap.Uint64("endBlock", endBlockInclusive),
				zap.String("contract", contractAddress),
			)
			logs, err := f.EthClient.GetLogs(ctx, contractAddress, startBlockInclusive, endBlockInclusive)
			f.Logger.Sugar().Debugw("Fetched logs for contract",
				zap.Uint64("startBlock", startBlockInclusive),
				zap.Uint64("endBlock", endBlockInclusive),
				zap.String("contract", contractAddress),
				zap.Int("count", len(logs)),
			)
			if err != nil {
				f.Logger.Sugar().Errorw("failed to fetch logs for contracts",
					zap.Uint64("startBlock", startBlockInclusive),
					zap.Uint64("endBlock", endBlockInclusive),
					zap.Strings("contracts", contractAddresses),
					zap.Error(err),
				)
				errorCollector <- err
				return
			}
			logsCollector <- logs
		}(contractAddress)
	}
	wg.Wait()
	close(logsCollector)
	close(errorCollector)
	f.Logger.Sugar().Infow("Finished fetching logs for contracts",
		zap.Uint64("startBlock", startBlockInclusive),
		zap.Uint64("endBlock", endBlockInclusive),
		zap.Strings("contracts", contractAddresses),
	)
	interestingBlockNumbers := make(map[uint64]bool)
	// collectedLogs := make([]*ethereum.EthereumEventLog, 0)
	for logs := range logsCollector {
		// collectedLogs = append(collectedLogs, logs...)
		for _, log := range logs {
			interestingBlockNumbers[log.BlockNumber.Value()] = true
		}
	}
	var err error
	for e := range errorCollector {
		err = e
		return nil, err
	}

	blockNumbers := make([]uint64, 0)
	for blockNumber := range interestingBlockNumbers {
		blockNumbers = append(blockNumbers, blockNumber)
	}
	slices.Sort(blockNumbers)

	return blockNumbers, err
}

func (f *Fetcher) FetchFilteredBlocksWithRetries(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64) ([]*FetchedBlock, error) {
	f.Logger.Sugar().Debugw("Fetching filtered blocks with retries",
		zap.Uint64("startBlock", startBlockInclusive),
		zap.Uint64("endBlock", endBlockInclusive),
	)
	// Fetch logs for all contracts and topics
	contractAddresses, err := f.InterestingLogsProvider.ListInterestingContractAddresses()
	if err != nil {
		f.Logger.Sugar().Errorw("failed to list interesting contract addresses", zap.Error(err))
		return nil, err
	}

	interestingBlockNumbers, err := f.FetchLogsForContractsForBlockRange(ctx, startBlockInclusive, endBlockInclusive, contractAddresses)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to fetch logs for contracts", zap.Error(err))
		return nil, err
	}

	blocks, err := f.FetchBlocks(ctx, startBlockInclusive, endBlockInclusive)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to fetch blocks",
			zap.Uint64("startBlock", startBlockInclusive),
			zap.Uint64("endBlock", endBlockInclusive),
			zap.Error(err),
		)
		return nil, err
	}

	// fetch receipts for only the interesting blocks
	f.Logger.Sugar().Debugw("Fetching receipts for interesting blocks", zap.Int("count", len(interestingBlockNumbers)))

	// pick blocks from the interesting block numbers
	interestingBlocks := utils.Filter(blocks, func(b *ethereum.EthereumBlock) bool {
		return slices.Contains(interestingBlockNumbers, b.Number.Value())
	})

	// get receipts for only the interesting blocks
	receipts := make([]*FetchedBlock, 0)

	if len(interestingBlocks) > 0 {
		receipts, err = f.FetchReceiptsForBlocks(ctx, interestingBlocks)
		if err != nil {
			f.Logger.Sugar().Errorw("failed to fetch receipts for interesting blocks", zap.Error(err))
			return nil, err
		}
	}

	finalBlocks := utils.Map(blocks, func(b *ethereum.EthereumBlock, i uint64) *FetchedBlock {
		if slices.Contains(interestingBlockNumbers, b.Number.Value()) {
			foundBlock := utils.Find(receipts, func(r *FetchedBlock) bool {
				return r.Block.Number.Value() == b.Number.Value()
			})
			if foundBlock != nil {
				return foundBlock
			}
		}
		return &FetchedBlock{
			Block:      b,
			TxReceipts: make(map[string]*ethereum.EthereumTransactionReceipt),
		}
	})

	return finalBlocks, nil
}

func (f *Fetcher) FetchInterestingBlocksAndLogsForContractsForBlockRange(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64, contractAddresses []string) ([]uint64, []*ethereum.EthereumEventLog, error) {
	// Define a reasonable chunk size to avoid HTTP 413 errors
	// This value can be adjusted based on your specific node's limitations
	const chunkSize uint64 = 4000

	numChunks := (endBlockInclusive-startBlockInclusive)/chunkSize + 1

	logsCollector := make(chan []*ethereum.EthereumEventLog, len(contractAddresses)*int(numChunks))
	errorCollector := make(chan error, len(contractAddresses)*int(numChunks))

	wg := &sync.WaitGroup{}

	for _, contractAddress := range contractAddresses {
		for chunkStart := startBlockInclusive; chunkStart <= endBlockInclusive; chunkStart += chunkSize {
			chunkEnd := chunkStart + chunkSize - 1
			if chunkEnd > endBlockInclusive {
				chunkEnd = endBlockInclusive
			}

			wg.Add(1)
			go func(contractAddress string, startBlock, endBlock uint64) {
				defer wg.Done()
				f.Logger.Sugar().Debugw("Fetching logs for contract in chunk",
					zap.Uint64("startBlock", startBlock),
					zap.Uint64("endBlock", endBlock),
					zap.String("contract", contractAddress),
				)

				logs, err := f.EthClient.GetLogs(ctx, contractAddress, startBlock, endBlock)
				f.Logger.Sugar().Debugw("Fetched logs for contract in chunk",
					zap.Uint64("startBlock", startBlock),
					zap.Uint64("endBlock", endBlock),
					zap.String("contract", contractAddress),
					zap.Int("count", len(logs)),
				)

				if err != nil {
					f.Logger.Sugar().Errorw("failed to fetch logs for contract in chunk",
						zap.Uint64("startBlock", startBlock),
						zap.Uint64("endBlock", endBlock),
						zap.String("contract", contractAddress),
						zap.Error(err),
					)
					errorCollector <- err
					return
				}

				logsCollector <- logs
			}(contractAddress, chunkStart, chunkEnd)
		}
	}

	wg.Wait()
	close(logsCollector)
	close(errorCollector)

	f.Logger.Sugar().Debugw("Finished fetching logs for contracts",
		zap.Uint64("startBlock", startBlockInclusive),
		zap.Uint64("endBlock", endBlockInclusive),
		zap.Strings("contracts", contractAddresses),
	)

	interestingBlockNumbers := make(map[uint64]bool)
	collectedLogs := make([]*ethereum.EthereumEventLog, 0)
	for logs := range logsCollector {
		for _, log := range logs {
			collectedLogs = append(collectedLogs, log)
			interestingBlockNumbers[log.BlockNumber.Value()] = true
		}
	}

	var err error
	for e := range errorCollector {
		err = e
		return nil, nil, err
	}

	blockNumbers := make([]uint64, 0)
	for blockNumber := range interestingBlockNumbers {
		blockNumbers = append(blockNumbers, blockNumber)
	}
	slices.Sort(blockNumbers)

	return blockNumbers, collectedLogs, err
}

func (f *Fetcher) FetchBlockList(ctx context.Context, blockNumbers []uint64) ([]*ethereum.EthereumBlock, error) {
	if len(blockNumbers) == 0 {
		return []*ethereum.EthereumBlock{}, nil
	}

	blockRequests := make([]*ethereum.RPCRequest, 0)
	for i, n := range blockNumbers {
		blockRequests = append(blockRequests, ethereum.GetBlockByNumberRequest(n, uint(i)))
	}

	blockResponses, err := f.EthClient.BatchCall(ctx, blockRequests)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to batch call for blocks", zap.Error(err))
		return nil, err
	}

	if len(blockResponses) != len(blockNumbers) {
		f.Logger.Sugar().Errorw("failed to fetch all blocks",
			zap.Int("fetched", len(blockResponses)),
			zap.Int("expected", len(blockNumbers)),
		)
		return nil, errors.New("failed to fetch all blocks")
	}

	blocks := make([]*ethereum.EthereumBlock, 0)
	for _, response := range blockResponses {
		b, err := ethereum.RPCMethod_getBlockByNumber.ResponseParser(response.Result)
		if err != nil {
			f.Logger.Sugar().Errorw("failed to parse block",
				zap.Error(err),
				zap.Uint("response ID", *response.ID),
			)
			return nil, err
		}
		blocks = append(blocks, b)
	}
	if len(blocks) != len(blockNumbers) {
		f.Logger.Sugar().Errorw("failed to fetch all blocks",
			zap.Int("fetched", len(blocks)),
			zap.Int("expected", len(blockNumbers)),
		)
		return nil, err
	}
	return blocks, nil
}

func (f *Fetcher) FetchReceiptsForBlocks(ctx context.Context, blocks []*ethereum.EthereumBlock) ([]*FetchedBlock, error) {
	fetchedBlockResponses := make(chan *FetchedBlock, len(blocks))
	foundErrorsChan := make(chan bool, 1)

	wg := sync.WaitGroup{}
	for _, block := range blocks {
		wg.Add(1)
		go func(b *ethereum.EthereumBlock) {
			defer wg.Done()
			receipts, err := f.GetReceiptsForBlock(ctx, b)
			if err != nil {
				f.Logger.Sugar().Errorw("failed to fetch receipts for block",
					zap.Uint64("blockNumber", b.Number.Value()),
					zap.Error(err),
				)
				foundErrorsChan <- true
				return
			}
			fetchedBlockResponses <- &FetchedBlock{
				Block:      b,
				TxReceipts: receipts,
			}
		}(block)
	}
	wg.Wait()
	close(fetchedBlockResponses)
	close(foundErrorsChan)
	foundErrors := <-foundErrorsChan
	if foundErrors {
		return nil, errors.New("failed to fetch receipts for some blocks")
	}
	fetchedBlocks := make([]*FetchedBlock, 0)
	for fb := range fetchedBlockResponses {
		fetchedBlocks = append(fetchedBlocks, fb)
	}
	if len(fetchedBlocks) != len(blocks) {
		f.Logger.Sugar().Errorw("failed to fetch all blocks",
			zap.Int("fetched", len(fetchedBlocks)),
			zap.Int("expected", len(blocks)),
		)
		return nil, errors.New("failed to fetch all blocks")
	}
	// ensure blocks are sorted ascending
	slices.SortFunc(fetchedBlocks, func(i, j *FetchedBlock) int {
		return int(i.Block.Number.Value() - j.Block.Number.Value())
	})

	f.Logger.Sugar().Debugw("Fetched blocks",
		zap.Int("count", len(fetchedBlocks)),
		zap.Uint64("startBlock", blocks[0].Number.Value()),
		zap.Uint64("endBlock", blocks[len(blocks)-1].Number.Value()),
	)

	return fetchedBlocks, nil
}

// FetchBlocks retrieves a range of blocks and their transaction receipts.
// It uses batch requests to fetch blocks and parallel processing to fetch receipts.
// Returns an array of FetchedBlock objects sorted by block number.
func (f *Fetcher) FetchBlocks(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64) ([]*ethereum.EthereumBlock, error) {
	blockNumbers := make([]uint64, 0)
	for i := startBlockInclusive; i <= endBlockInclusive; i++ {
		blockNumbers = append(blockNumbers, i)
	}

	if len(blockNumbers) == 0 {
		return []*ethereum.EthereumBlock{}, nil
	}

	blockRequests := make([]*ethereum.RPCRequest, 0)
	for i, n := range blockNumbers {
		blockRequests = append(blockRequests, ethereum.GetBlockByNumberRequest(n, uint(i)))
	}
	blockResponses, err := f.EthClient.BatchCall(ctx, blockRequests)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to batch call for blocks", zap.Error(err))
		return nil, err
	}
	if len(blockResponses) != len(blockNumbers) {
		f.Logger.Sugar().Errorw("failed to fetch all blocks",
			zap.Int("fetched", len(blockResponses)),
			zap.Int("expected", len(blockNumbers)),
		)
		return nil, errors.New("failed to fetch all blocks")
	}
	blocks := make([]*ethereum.EthereumBlock, 0)
	for _, response := range blockResponses {
		b, err := ethereum.RPCMethod_getBlockByNumber.ResponseParser(response.Result)
		if err != nil {
			f.Logger.Sugar().Errorw("failed to parse block",
				zap.Error(err),
				zap.Uint("response ID", *response.ID),
			)
			return nil, err
		}
		blocks = append(blocks, b)
	}
	if len(blocks) != len(blockNumbers) {
		f.Logger.Sugar().Errorw("failed to fetch all blocks",
			zap.Int("fetched", len(blocks)),
			zap.Int("expected", len(blockNumbers)),
		)
		return nil, err
	}
	return blocks, nil
}

// FetchBlocksWithReceipts retrieves a range of blocks and their transaction receipts.
// It uses batch requests to fetch blocks and parallel processing to fetch receipts.
// Returns an array of FetchedBlock objects sorted by block number.
func (f *Fetcher) FetchBlocksWithReceipts(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64) ([]*FetchedBlock, error) {
	blocks, err := f.FetchBlocks(ctx, startBlockInclusive, endBlockInclusive)
	if err != nil {
		f.Logger.Sugar().Errorw("failed to fetch blocks",
			zap.Uint64("startBlock", startBlockInclusive),
			zap.Uint64("endBlock", endBlockInclusive),
			zap.Error(err),
		)
		return nil, err
	}

	return f.FetchReceiptsForBlocks(ctx, blocks)
}
