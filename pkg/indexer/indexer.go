package indexer

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/transactionLogParser"
	"gorm.io/gorm"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
)

// Indexer processes and stores blockchain data, filtering for interesting transactions and events.
type Indexer struct {
	// Logger provides structured logging capabilities
	Logger *zap.Logger
	// MetadataStore persists block metadata information
	MetadataStore storage.BlockStore
	// ContractStore manages contract metadata and ABIs
	ContractStore contractStore.ContractStore
	// ContractManager handles contract deployment and tracking
	ContractManager *contractManager.ContractManager
	// Fetcher retrieves blockchain data from the node
	Fetcher *fetcher.Fetcher
	// EthereumClient provides access to the Ethereum node
	EthereumClient *ethereum.Client
	// Config contains application configuration settings
	Config *config.Config
	// ContractCaller executes read-only contract calls
	ContractCaller contractCaller.IContractCaller
	// transactionLogParser is used to parse transaction logs
	TransactionLogParser *transactionLogParser.TransactionLogParser
	// db is the database connection for persisting data
	db *gorm.DB
}

// IndexErrorType identifies different categories of indexing errors
type IndexErrorType int

// Error type constants for indexing operations
const (
	// IndexError_ReceiptNotFound indicates a transaction receipt could not be retrieved
	IndexError_ReceiptNotFound IndexErrorType = 1
	// IndexError_FailedToParseTransaction indicates a failure to parse transaction data
	IndexError_FailedToParseTransaction IndexErrorType = 2
	// IndexError_FailedToCombineAbis indicates a failure to combine multiple ABIs
	IndexError_FailedToCombineAbis IndexErrorType = 3
	// IndexError_FailedToFindContract indicates a contract could not be found in the store
	IndexError_FailedToFindContract IndexErrorType = 4
	// IndexError_FailedToParseAbi indicates a failure to parse an ABI definition
	IndexError_FailedToParseAbi IndexErrorType = 5
	// IndexError_EmptyAbi indicates an empty or missing ABI
	IndexError_EmptyAbi IndexErrorType = 6
	// IndexError_FailedToDecodeLog indicates a failure to decode a transaction log
	IndexError_FailedToDecodeLog IndexErrorType = 7
)

// IndexError represents a structured error during the indexing process
type IndexError struct {
	// Type categorizes the error
	Type IndexErrorType
	// Err contains the underlying error
	Err error
	// BlockNumber is the block where the error occurred
	BlockNumber uint64
	// TransactionHash is the hash of the transaction where the error occurred
	TransactionHash string
	// LogIndex is the index of the log where the error occurred
	LogIndex uint64
	// Metadata contains additional contextual information about the error
	Metadata map[string]interface{}
	// Message provides a human-readable description of the error
	Message string
}

// Error returns the error message for IndexError
func (e *IndexError) Error() string {
	return fmt.Sprintf("IndexError: %s", e.Err.Error())
}

// NewIndexError creates a new IndexError with the specified type and underlying error
func NewIndexError(t IndexErrorType, err error) *IndexError {
	return &IndexError{
		Type:     t,
		Err:      err,
		Metadata: make(map[string]interface{}),
	}
}

// WithBlockNumber adds the block number to the error and returns the error for chaining
func (e *IndexError) WithBlockNumber(blockNumber uint64) *IndexError {
	e.BlockNumber = blockNumber
	return e
}

// WithTransactionHash adds the transaction hash to the error and returns the error for chaining
func (e *IndexError) WithTransactionHash(txHash string) *IndexError {
	e.TransactionHash = txHash
	return e
}

// WithLogIndex adds the log index to the error and returns the error for chaining
func (e *IndexError) WithLogIndex(logIndex uint64) *IndexError {
	e.LogIndex = logIndex
	return e
}

// WithMetadata adds additional context data to the error and returns the error for chaining
func (e *IndexError) WithMetadata(key string, value interface{}) *IndexError {
	e.Metadata[key] = value
	return e
}

// WithMessage adds a human-readable message to the error and returns the error for chaining
func (e *IndexError) WithMessage(message string) *IndexError {
	e.Message = message
	return e
}

func NewIndexer(
	ms storage.BlockStore,
	cs contractStore.ContractStore,
	cm *contractManager.ContractManager,
	e *ethereum.Client,
	f *fetcher.Fetcher,
	cc contractCaller.IContractCaller,
	grm *gorm.DB,
	l *zap.Logger,
	cfg *config.Config,
) *Indexer {
	i := &Indexer{
		Logger:          l,
		MetadataStore:   ms,
		ContractStore:   cs,
		ContractManager: cm,
		Fetcher:         f,
		EthereumClient:  e,
		ContractCaller:  cc,
		Config:          cfg,
		db:              grm,
	}

	tlp := transactionLogParser.NewTransactionLogParser(l, cm, i)
	i.TransactionLogParser = tlp

	return i
}

// ParseInterestingTransactionsAndLogs processes a block's transactions and logs,
// extracting only those relevant to monitored contracts.
// Returns parsed transactions and any indexing errors encountered.
func (idx *Indexer) ParseInterestingTransactionsAndLogs(ctx context.Context, fetchedBlock *fetcher.FetchedBlock) ([]*parser.ParsedTransaction, error) {
	parsedTransactions := make([]*parser.ParsedTransaction, 0)
	for i, tx := range fetchedBlock.Block.Transactions {
		txReceipt, ok := fetchedBlock.TxReceipts[tx.Hash.Value()]
		if !ok {
			continue
			// idx.Logger.Sugar().Errorw("Receipt not found for transaction",
			// 	zap.String("txHash", tx.Hash.Value()),
			// 	zap.Uint64("block", tx.BlockNumber.Value()),
			// )
			// return nil, NewIndexError(IndexError_ReceiptNotFound, fmt.Errorf("receipt not found for transaction")).
			// 	WithBlockNumber(tx.BlockNumber.Value()).
			// 	WithTransactionHash(tx.Hash.Value())
		}

		parsedTransactionAndLogs, err := idx.ParseTransactionLogs(tx, txReceipt)

		if err != nil {
			idx.Logger.Sugar().Errorw("failed to process transaction logs",
				zap.Error(err),
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
			)
			return nil, err
		}
		if parsedTransactionAndLogs == nil {
			idx.Logger.Sugar().Debugw("Transaction is nil",
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
				zap.Int("logIndex", i),
			)
			continue
		}
		idx.Logger.Sugar().Debugw("Parsed transaction and logs",
			zap.String("txHash", tx.Hash.Value()),
			zap.Uint64("block", tx.BlockNumber.Value()),
			zap.Int("logCount", len(parsedTransactionAndLogs.Logs)),
		)
		// If there are interesting logs or if the tx/receipt is interesting, include it
		if len(parsedTransactionAndLogs.Logs) > 0 || idx.IsInterestingTransaction(tx, txReceipt) {
			parsedTransactions = append(parsedTransactions, parsedTransactionAndLogs)
		}
	}
	return parsedTransactions, nil
}

// IndexFetchedBlock stores a fetched block in the metadata store.
// Returns the stored block, a boolean indicating if the block was already indexed,
// and any error encountered during the process.
func (idx *Indexer) IndexFetchedBlock(fetchedBlock *fetcher.FetchedBlock) (*storage.Block, bool, error) {
	blockNumber := fetchedBlock.Block.Number.Value()
	blockHash := fetchedBlock.Block.Hash.Value()
	parentHash := fetchedBlock.Block.ParentHash.Value()

	foundBlock, err := idx.MetadataStore.GetBlockByNumber(blockNumber)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to get block by number",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
		)
		return nil, false, err
	}
	if foundBlock != nil {
		idx.Logger.Sugar().Debugw(fmt.Sprintf("Block '%d' already indexed", blockNumber))
		return foundBlock, true, nil
	}

	// TODO(seanmcgary): store previous block hash
	insertedBlock, err := idx.MetadataStore.InsertBlockAtHeight(blockNumber, blockHash, parentHash, fetchedBlock.Block.Timestamp.Value())
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to insert block at height",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.String("blockHash", blockHash),
		)
		return nil, false, err
	}

	return insertedBlock, false, nil
}

// IsInterestingAddress determines if an address is included in the configured
// list of interesting addresses to monitor.
func (idx *Indexer) IsInterestingAddress(addr string) bool {
	if addr == "" {
		return false
	}
	addr = strings.ToLower(addr)
	if slices.Contains(idx.Config.GetInterestingAddressForConfigEnv(), addr) {
		return true
	}

	addresses, err := idx.ContractStore.ListInterestingContractAddresses()
	if err != nil {
		return false
	}

	return slices.Contains(addresses, addr)
}

// IsInterestingTransaction determines if a transaction interacts with or creates
// a monitored contract.
func (idx *Indexer) IsInterestingTransaction(txn *ethereum.EthereumTransaction, receipt *ethereum.EthereumTransactionReceipt) bool {
	address := txn.To.Value()
	contractAddress := receipt.ContractAddress.Value()

	if (address != "" && idx.IsInterestingAddress(address)) || (contractAddress != "" && idx.IsInterestingAddress(contractAddress)) {
		return true
	}

	return false
}

// FilterInterestingTransactions extracts transactions from a block that
// interact with monitored contracts.
func (idx *Indexer) FilterInterestingTransactions(
	block *storage.Block,
	fetchedBlock *fetcher.FetchedBlock,
) []*ethereum.EthereumTransaction {
	interestingTransactions := make([]*ethereum.EthereumTransaction, 0)
	for _, tx := range fetchedBlock.Block.Transactions {
		txReceipt, ok := fetchedBlock.TxReceipts[tx.Hash.Value()]
		if !ok {
			idx.Logger.Sugar().Errorw("Receipt not found for transaction",
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
			)
			continue
		}

		hasInterestingLog := false
		if ok {
			for _, log := range txReceipt.Logs {
				if idx.IsInterestingAddress(log.Address.Value()) {
					hasInterestingLog = true
					break
				}
			}
		}

		// Only insert transactions that are interesting:
		// - TX is being sent to an EL contract
		// - TX created an EL contract
		// - TX has logs emitted by an EL contract
		if hasInterestingLog || idx.IsInterestingTransaction(tx, txReceipt) {
			interestingTransactions = append(interestingTransactions, tx)
		}
	}
	return interestingTransactions
}

// IndexTransaction stores a transaction in the metadata store.
// Returns the stored transaction and any error encountered.
func (idx *Indexer) IndexTransaction(
	block *storage.Block,
	tx *ethereum.EthereumTransaction,
	receipt *ethereum.EthereumTransactionReceipt,
) (*storage.Transaction, error) {
	return idx.MetadataStore.InsertBlockTransaction(
		block.Number,
		tx.Hash.Value(),
		tx.Index.Value(),
		tx.From.Value(),
		tx.To.Value(),
		receipt.ContractAddress.Value(),
		receipt.GetBytecodeHash(),
		receipt.GasUsed.Value(),
		receipt.CumulativeGasUsed.Value(),
		receipt.EffectiveGasPrice.Value(),
		false,
	)
}

// IndexLog stores a decoded transaction log in the metadata store.
// Returns the stored log and any error encountered.
func (idx *Indexer) IndexLog(
	ctx context.Context,
	blockNumber uint64,
	txHash string,
	txIndex uint64,
	log *parser.DecodedLog,
) (*storage.TransactionLog, error) {
	insertedLog, err := idx.MetadataStore.InsertTransactionLog(
		txHash,
		txIndex,
		blockNumber,
		log,
		log.OutputData,
		false,
	)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to insert transaction log",
			zap.Error(err),
			zap.String("txHash", txHash),
			zap.Uint64("blockNumber", blockNumber),
		)
		return nil, err
	}

	return insertedLog, nil
}
