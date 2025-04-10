package transactionBackfiller

import (
	"context"
	"fmt"
	"sync"

	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
)

// TransactionBackfillerConfig contains configuration options for the transaction backfiller.
type TransactionBackfillerConfig struct {
	// Workers is the number of concurrent workers processing blocks
	Workers int
}

// BackfillerResponse contains the result of a backfill operation.
type BackfillerResponse struct {
	// Errors is a list of errors encountered during backfilling
	Errors []error
}

// BackfillerMessage represents a request to backfill transaction data for a range of blocks.
type BackfillerMessage struct {
	// StartBlock is the first block to process (inclusive)
	StartBlock uint64
	// EndBlock is the last block to process (inclusive)
	EndBlock uint64
	// Addresses is a list of addresses to process
	Addresses []string
	// TransactionLogHandler is a function called for each interesting log
	TransactionLogHandler func(block *ethereum.EthereumBlock, receipt *ethereum.EthereumTransactionReceipt, log *ethereum.EthereumEventLog) error
	// IsInterestingLog determines if a log should be processed
	IsInterestingLog func(log *ethereum.EthereumEventLog) bool
	// ResponseChan is a channel to send the backfill response
	ResponseChan chan *BackfillerResponse
	// Context is the context for the backfill operation
	Context context.Context
}

// TransactionBackfiller processes transaction logs for specified block ranges.
// It uses a worker pool to concurrently process blocks and extract relevant transaction logs.
type TransactionBackfiller struct {
	logger     *zap.Logger
	config     *TransactionBackfillerConfig
	fetcher    *fetcher.Fetcher
	blockStore storage.BlockStore
	queue      chan *BackfillerMessage
	done       chan struct{}
}

// LogWithMetadata represents a log with additional information needed for processing
type LogWithMetadata struct {
	Log         *ethereum.EthereumEventLog
	BlockNumber uint64
	TxHash      string
}

// blockWithLogs is a struct that wraps a block number with its logs for thread-safe processing
type blockWithLogs struct {
	blockNumber uint64
	logs        []*LogWithMetadata
}

const (
	// queueDepth is the maximum number of backfill messages that can be queued
	queueDepth = 100
	// defaultWorkers is the default number of concurrent workers if not specified
	defaultWorkers = 10
)

// NewTransactionBackfiller creates a new TransactionBackfiller with the provided configuration.
//
// Parameters:
//   - cfg: Configuration for the backfiller
//   - logger: Logger for recording operations
//   - fetcher: Service for fetching blockchain data
//   - bs: Storage for block data
//
// Returns:
//   - *TransactionBackfiller: A configured transaction backfiller
func NewTransactionBackfiller(
	cfg *TransactionBackfillerConfig,
	logger *zap.Logger,
	fetcher *fetcher.Fetcher,
	bs storage.BlockStore,
) *TransactionBackfiller {
	if cfg.Workers == 0 {
		cfg.Workers = defaultWorkers
	}
	return &TransactionBackfiller{
		config:     cfg,
		logger:     logger,
		fetcher:    fetcher,
		blockStore: bs,
		queue:      make(chan *BackfillerMessage, queueDepth),
		done:       make(chan struct{}),
	}
}

// Enqueue adds a backfill message to the processing queue.
// Returns an error if the queue is full.
//
// Parameters:
//   - message: The backfill message to enqueue
//
// Returns:
//   - error: Error if the queue is full, nil otherwise
func (t *TransactionBackfiller) Enqueue(message *BackfillerMessage) error {
	select {
	case t.queue <- message:
		t.logger.Sugar().Infow("Enqueueing backfiller message",
			zap.Uint64("startBlock", message.StartBlock),
			zap.Uint64("endBlock", message.EndBlock))
		return nil
	default:
		return fmt.Errorf("backfiller queue is full, please wait and try again")
	}
}

// EnqueueAndWait adds a backfill message to the queue and waits for its completion.
// It returns the response from processing or an error if the operation times out or fails.
//
// Parameters:
//   - ctx: Context for the operation, which can be used to cancel it
//   - message: The backfill message to process
//
// Returns:
//   - *BackfillerResponse: The result of the backfill operation
//   - error: Any error encountered during the operation
func (t *TransactionBackfiller) EnqueueAndWait(ctx context.Context, message *BackfillerMessage) (*BackfillerResponse, error) {
	responseChan := make(chan *BackfillerResponse, 1)
	message.ResponseChan = responseChan
	message.Context = ctx
	if err := t.Enqueue(message); err != nil {
		return nil, err
	}

	t.logger.Sugar().Infow("Waiting for backfiller response",
		zap.Uint64("startBlock", message.StartBlock),
		zap.Uint64("endBlock", message.EndBlock),
	)

	select {
	case response := <-responseChan:
		t.logger.Sugar().Infow("Received backfiller response")
		return response, nil
	case <-ctx.Done():
		t.logger.Sugar().Infow("Received context.Done()")
		select {
		case response := <-responseChan:
			return response, nil
		default:
			// No response received
		}
		return nil, ctx.Err()
	}
}

// Close shuts down the backfiller by closing the done channel.
func (t *TransactionBackfiller) Close() {
	t.logger.Sugar().Infow("Closing backfiller")
	close(t.done)
}

// Process is the main processing loop for the backfiller.
// It continuously pulls messages from the queue and processes them until closed.
func (t *TransactionBackfiller) Process() {
	for {
		select {
		case <-t.done:
			t.logger.Sugar().Infow("Closing backfiller")
			return
		case msg := <-t.queue:
			t.logger.Sugar().Infow("Processing backfiller message",
				zap.Uint64("startBlock", msg.StartBlock),
				zap.Uint64("endBlock", msg.EndBlock),
			)
			response := t.ProcessBlocks(msg.Context, msg)

			t.logger.Sugar().Infow("Processed backfiller message",
				zap.Uint64("startBlock", msg.StartBlock),
				zap.Uint64("endBlock", msg.EndBlock),
			)

			if msg.ResponseChan != nil {
				select {
				case msg.ResponseChan <- response:
					t.logger.Sugar().Infow("Sent backfiller response")
				default:
					t.logger.Sugar().Infow("No receiver for response, dropping")
				}
			} else {
				t.logger.Sugar().Infow("No response channel, dropping response")
			}
		}

	}
}

// ProcessBlocks processes a range of blocks specified in the backfill message.
// It creates a worker pool to process blocks concurrently.
//
// Parameters:
//   - ctx: Context for the operation
//   - queueMessage: The backfill message containing the block range and handlers
//
// Returns:
//   - *BackfillerResponse: The result of processing the blocks
func (t *TransactionBackfiller) ProcessBlocks(ctx context.Context, queueMessage *BackfillerMessage) *BackfillerResponse {
	response := &BackfillerResponse{}

	// If addresses are specified, use the optimized method
	if len(queueMessage.Addresses) > 0 {
		return t.ProcessBlocksWithAddresses(ctx, queueMessage)
	}

	wg := &sync.WaitGroup{}
	blocksQueue := make(chan uint64, queueMessage.EndBlock-queueMessage.StartBlock+1)

	errorsRecv := make(chan error)
	wg.Add(t.config.Workers)
	t.logger.Sugar().Infow("Using workers", zap.Int("count", t.config.Workers))
	for i := 0; i < t.config.Workers; i++ {
		go func() {
			msg := queueMessage
			err := t.WorkOnBlock(ctx, blocksQueue, wg, msg)
			if err != nil {
				t.logger.Sugar().Errorw("Error processing block", zap.Error(err))
				errorsRecv <- err
			}
		}()
	}

	t.logger.Sugar().Infow("Queueing blocks", zap.Uint64("startBlock", queueMessage.StartBlock), zap.Uint64("endBlock", queueMessage.EndBlock))
	for i := queueMessage.StartBlock; i <= queueMessage.EndBlock; i++ {
		blocksQueue <- i
	}
	close(blocksQueue)
	t.logger.Sugar().Infow("Waiting for workers to finish")
	wg.Wait()

	t.logger.Sugar().Infow("All workers finished, compiling errors")
	close(errorsRecv)
	errors := make([]error, 0)
	for err := range errorsRecv {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		response.Errors = errors
		return response
	}
	return response
}

// WorkOnBlock is a worker function that processes blocks from the queue.
// It's designed to be run in a goroutine as part of a worker pool.
//
// Parameters:
//   - ctx: Context for the operation
//   - blocksQueue: Channel providing block numbers to process
//   - wg: WaitGroup for worker synchronization
//   - message: The backfill message containing processing instructions
//
// Returns:
//   - error: Any error encountered during processing
func (t *TransactionBackfiller) WorkOnBlock(ctx context.Context, blocksQueue <-chan uint64, wg *sync.WaitGroup, message *BackfillerMessage) error {
	defer wg.Done()
	for block := range blocksQueue {
		t.logger.Sugar().Infow("Processing block", zap.Uint64("blockNumber", block))
		err := t.ProcessBlock(ctx, block, message)
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessBlock processes a single block, extracting and handling relevant transaction logs.
//
// Parameters:
//   - ctx: Context for the operation
//   - blockNumber: The number of the block to process
//   - message: The backfill message containing processing instructions
//
// Returns:
//   - error: Any error encountered during processing
func (t *TransactionBackfiller) ProcessBlock(ctx context.Context, blockNumber uint64, message *BackfillerMessage) error {
	t.logger.Sugar().Infow("Fetching block", zap.Uint64("blockNumber", blockNumber))
	block, err := t.fetcher.FetchBlock(ctx, blockNumber)
	if err != nil {
		t.logger.Sugar().Errorw("Error fetching block", zap.Error(err))
		return err
	}
	t.logger.Sugar().Infow("Fetched block",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int("txCount", len(block.TxReceipts)),
	)

	for txHash, receipt := range block.TxReceipts {
		for _, log := range receipt.Logs {
			if message.IsInterestingLog(log) {
				err = message.TransactionLogHandler(block.Block, receipt, log)
				if err != nil {
					t.logger.Sugar().Errorw("Error processing transaction log",
						zap.Error(err),
						zap.String("txHash", txHash),
						zap.Uint64("blockNumber", blockNumber),
					)
					return err
				}
			}
		}
	}
	return nil
}

// ProcessBlocksWithAddresses processes logs for specified addresses within a block range.
// It uses eth_getLogs RPC call to efficiently fetch logs for specific addresses.
//
// Parameters:
//   - ctx: Context for the operation
//   - queueMessage: The backfill message containing the block range, addresses, and handlers
//
// Returns:
//   - *BackfillerResponse: The result of processing the logs
func (t *TransactionBackfiller) ProcessBlocksWithAddresses(ctx context.Context, queueMessage *BackfillerMessage) *BackfillerResponse {
	response := &BackfillerResponse{}

	_, logs, err := t.fetcher.FetchInterestingBlocksAndLogsForContractsForBlockRange(ctx, queueMessage.StartBlock, queueMessage.EndBlock, queueMessage.Addresses)
	if err != nil {
		t.logger.Sugar().Errorw("Error fetching interesting blocks and logs", zap.Error(err))
		return nil
	}

	// Group logs by block number and create blockWithLogs structs directly
	blockLogsMap := make(map[uint64]*blockWithLogs)
	for _, log := range logs {
		blockNum := log.BlockNumber.Value()
		txHash := log.TransactionHash.Value()

		logWithMetadata := &LogWithMetadata{
			Log:         log,
			BlockNumber: blockNum,
			TxHash:      txHash,
		}

		// Create or retrieve blockWithLogs for this block number
		blockLogs, exists := blockLogsMap[blockNum]
		if !exists {
			blockLogs = &blockWithLogs{
				blockNumber: blockNum,
				logs:        make([]*LogWithMetadata, 0),
			}
			blockLogsMap[blockNum] = blockLogs
		}

		// Add this log to the block's logs
		blockLogs.logs = append(blockLogs.logs, logWithMetadata)
	}

	// Create worker pool for block fetching and processing
	wg := &sync.WaitGroup{}
	blockLogsQueue := make(chan blockWithLogs, len(blockLogsMap))
	errorsRecv := make(chan error, len(blockLogsMap))

	// Launch workers
	wg.Add(t.config.Workers)
	for i := 0; i < t.config.Workers; i++ {
		go func() {
			defer wg.Done()
			for blockData := range blockLogsQueue {
				// Fetch the block
				fetchedBlock, err := t.fetcher.FetchBlock(ctx, blockData.blockNumber)
				if err != nil {
					t.logger.Sugar().Errorw("Error fetching block",
						zap.Uint64("blockNumber", blockData.blockNumber),
						zap.Error(err))
					errorsRecv <- err
					continue
				}

				// Process logs for this block
				if err := t.processLogsForBlock(queueMessage, blockData.logs, fetchedBlock); err != nil {
					errorsRecv <- err
					// Stop processing on error to prevent partial processing
					// This avoids inconsistent state that could occur if only some logs are processed
					break
				}
			}
		}()
	}

	// Queue the blocks with their logs
	t.logger.Sugar().Infow("Queueing blocks for processing", zap.Int("count", len(blockLogsMap)))
	for _, blockLogs := range blockLogsMap {
		blockLogsQueue <- *blockLogs
	}
	close(blockLogsQueue)

	t.logger.Sugar().Infow("Waiting for workers to finish")
	wg.Wait()

	t.logger.Sugar().Infow("All workers finished, compiling errors")
	close(errorsRecv)
	errors := make([]error, 0)
	for err := range errorsRecv {
		errors = append(errors, err)
	}

	response.Errors = errors
	return response
}

// processLogsForBlock processes all logs for a specific block
// Returns the first error encountered or nil if all logs are processed successfully
func (t *TransactionBackfiller) processLogsForBlock(
	queueMessage *BackfillerMessage,
	logs []*LogWithMetadata,
	fetchedBlock *fetcher.FetchedBlock,
) error {
	for _, logData := range logs {
		// Get the transaction receipt from the fetched block
		receipt, exists := fetchedBlock.TxReceipts[logData.TxHash]
		if !exists {
			t.logger.Sugar().Warnw("Transaction receipt not found in block",
				zap.String("txHash", logData.TxHash),
				zap.Uint64("blockNumber", fetchedBlock.Block.Number.Value()),
			)
			continue
		}

		// Handle the log
		if err := queueMessage.TransactionLogHandler(fetchedBlock.Block, receipt, logData.Log); err != nil {
			t.logger.Sugar().Errorw("Error processing transaction log",
				zap.Error(err),
				zap.String("txHash", logData.TxHash),
				zap.Uint64("blockNumber", fetchedBlock.Block.Number.Value()),
			)
			return err
		}
	}
	return nil
}
