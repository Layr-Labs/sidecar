// Package fetcher provides functionality for retrieving blocks and transaction data
// from Ethereum nodes. It handles batch requests, retries, and parallel processing
// to efficiently fetch blockchain data.
package fetcher

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"slices"
	"sync"
	"time"
)

// Fetcher is responsible for retrieving blockchain data from Ethereum nodes.
// It provides methods for fetching blocks and transaction receipts.
type Fetcher struct {
	// EthClient is used to communicate with Ethereum nodes
	EthClient *ethereum.Client
	// Logger is used for logging fetch operations
	Logger    *zap.Logger
	// Config contains application configuration
	Config    *config.Config
}

// NewFetcher creates a new Fetcher with the provided Ethereum client, configuration, and logger.
func NewFetcher(ethClient *ethereum.Client, cfg *config.Config, l *zap.Logger) *Fetcher {
	return &Fetcher{
		EthClient: ethClient,
		Logger:    l,
		Config:    cfg,
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

	receipts, err := f.FetchReceiptsForBlock(ctx, block)
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

// IsInterestingAddress checks if a contract address is in the list of interesting addresses
// defined in the configuration. This is used to filter which contracts to process.
func (f *Fetcher) IsInterestingAddress(contractAddress string) bool {
	return slices.Contains(f.Config.GetInterestingAddressForConfigEnv(), contractAddress)
}

// FetchBlocksWithRetries attempts to fetch a range of blocks with exponential backoff retries.
// It will retry failed requests with increasing delays before giving up.
// Returns an array of FetchedBlock objects for the requested range.
func (f *Fetcher) FetchBlocksWithRetries(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64) ([]*FetchedBlock, error) {
	retries := []int{1, 2, 4, 8, 16, 32, 64}
	var e error
	for i, r := range retries {
		fetchedBlocks, err := f.FetchBlocks(ctx, startBlockInclusive, endBlockInclusive)
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

// FetchBlocks retrieves a range of blocks and their transaction receipts.
// It uses batch requests to fetch blocks and parallel processing to fetch receipts.
// Returns an array of FetchedBlock objects sorted by block number.
func (f *Fetcher) FetchBlocks(ctx context.Context, startBlockInclusive uint64, endBlockInclusive uint64) ([]*FetchedBlock, error) {
	blockNumbers := make([]uint64, 0)
	for i := startBlockInclusive; i <= endBlockInclusive; i++ {
		blockNumbers = append(blockNumbers, i)
	}

	if len(blockNumbers) == 0 {
		return []*FetchedBlock{}, nil
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

	fetchedBlockResponses := make(chan *FetchedBlock, len(blocks))
	foundErrorsChan := make(chan bool, 1)

	wg := sync.WaitGroup{}
	for _, block := range blocks {
		wg.Add(1)
		go func(b *ethereum.EthereumBlock) {
			defer wg.Done()
			receipts, err := f.FetchReceiptsForBlock(ctx, b)
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
		zap.Uint64("startBlock", startBlockInclusive),
		zap.Uint64("endBlock", endBlockInclusive),
	)

	return fetchedBlocks, nil
}
