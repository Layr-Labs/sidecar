package indexer

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/clients/ethereum"
	"github.com/Layr-Labs/go-sidecar/internal/clients/etherscan"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/contractCaller"
	"github.com/Layr-Labs/go-sidecar/internal/contractManager"
	"github.com/Layr-Labs/go-sidecar/internal/contractStore"
	"github.com/Layr-Labs/go-sidecar/internal/fetcher"
	"github.com/Layr-Labs/go-sidecar/internal/parser"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

type Indexer struct {
	Logger          *zap.Logger
	MetadataStore   storage.BlockStore
	ContractStore   contractStore.ContractStore
	ContractManager *contractManager.ContractManager
	EtherscanClient *etherscan.EtherscanClient
	Fetcher         *fetcher.Fetcher
	EthereumClient  *ethereum.Client
	Config          *config.Config
	ContractCaller  contractCaller.IContractCaller
}

type IndexErrorType int

const (
	IndexError_ReceiptNotFound          IndexErrorType = 1
	IndexError_FailedToParseTransaction IndexErrorType = 2
	IndexError_FailedToCombineAbis      IndexErrorType = 3
	IndexError_FailedToFindContract     IndexErrorType = 4
	IndexError_FailedToParseAbi         IndexErrorType = 5
)

type IndexError struct {
	Type            IndexErrorType
	Err             error
	BlockNumber     uint64
	TransactionHash string
	LogIndex        int
	Metadata        map[string]interface{}
	Message         string
}

func NewIndexError(t IndexErrorType, err error) *IndexError {
	return &IndexError{
		Type:     t,
		Err:      err,
		Metadata: make(map[string]interface{}),
	}
}

func (e *IndexError) WithBlockNumber(blockNumber uint64) *IndexError {
	e.BlockNumber = blockNumber
	return e
}

func (e *IndexError) WithTransactionHash(txHash string) *IndexError {
	e.TransactionHash = txHash
	return e
}

func (e *IndexError) WithLogIndex(logIndex int) *IndexError {
	e.LogIndex = logIndex
	return e
}

func (e *IndexError) WithMetadata(key string, value interface{}) *IndexError {
	e.Metadata[key] = value
	return e
}
func (e *IndexError) WithMessage(message string) *IndexError {
	e.Message = message
	return e
}

func NewIndexer(
	ms storage.BlockStore,
	cs contractStore.ContractStore,
	es *etherscan.EtherscanClient,
	cm *contractManager.ContractManager,
	e *ethereum.Client,
	f *fetcher.Fetcher,
	cc contractCaller.IContractCaller,
	l *zap.Logger,
	cfg *config.Config,
) *Indexer {
	return &Indexer{
		Logger:          l,
		MetadataStore:   ms,
		ContractStore:   cs,
		EtherscanClient: es,
		ContractManager: cm,
		Fetcher:         f,
		EthereumClient:  e,
		ContractCaller:  cc,
		Config:          cfg,
	}
}

func (idx *Indexer) ParseInterestingTransactionsAndLogs(ctx context.Context, fetchedBlock *fetcher.FetchedBlock) ([]*parser.ParsedTransaction, *IndexError) {
	parsedTransactions := make([]*parser.ParsedTransaction, 0)
	for i, tx := range fetchedBlock.Block.Transactions {
		txReceipt, ok := fetchedBlock.TxReceipts[tx.Hash.Value()]
		if !ok {
			idx.Logger.Sugar().Errorw("Receipt not found for transaction",
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
			)
			return nil, NewIndexError(IndexError_ReceiptNotFound, xerrors.Errorf("receipt not found for transaction")).
				WithBlockNumber(tx.BlockNumber.Value()).
				WithTransactionHash(tx.Hash.Value())
		}

		parsedTransactionAndLogs, err := idx.ParseTransactionLogs(tx, txReceipt)
		if err != nil {
			idx.Logger.Sugar().Errorw("failed to process transaction logs",
				zap.Error(err.Err),
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

func (idx *Indexer) ParseAndIndexTransactionLogs(ctx context.Context, fetchedBlock *fetcher.FetchedBlock) *IndexError {
	for i, tx := range fetchedBlock.Block.Transactions {
		txReceipt, ok := fetchedBlock.TxReceipts[tx.Hash.Value()]
		if !ok {
			idx.Logger.Sugar().Errorw("Receipt not found for transaction",
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
			)
			return NewIndexError(IndexError_ReceiptNotFound, xerrors.Errorf("receipt not found for transaction")).
				WithBlockNumber(tx.BlockNumber.Value()).
				WithTransactionHash(tx.Hash.Value())
		}

		parsedTransactionLogs, err := idx.ParseTransactionLogs(tx, txReceipt)
		if err != nil {
			idx.Logger.Sugar().Errorw("failed to process transaction logs",
				zap.Error(err.Err),
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
			)
			continue
		}
		if parsedTransactionLogs == nil {
			idx.Logger.Sugar().Debugw("Log line is nil",
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
				zap.Int("logIndex", i),
			)
			continue
		}

		for _, log := range parsedTransactionLogs.Logs {
			_, err := idx.IndexLog(ctx, fetchedBlock.Block.Number.Value(), time.Unix(int64(fetchedBlock.Block.Timestamp.Value()), 0), tx.Hash.Value(), tx.Index.Value(), log)
			if err != nil {
				idx.Logger.Sugar().Errorw("failed to index log",
					zap.Error(err),
					zap.String("txHash", tx.Hash.Value()),
					zap.Uint64("block", tx.BlockNumber.Value()),
				)
			}
		}

		upgradedLogs := idx.FindContractUpgradedLogs(parsedTransactionLogs.Logs)
		if len(upgradedLogs) > 0 {
			idx.Logger.Sugar().Debugw("Found contract upgrade logs",
				zap.String("txHash", tx.Hash.Value()),
				zap.Uint64("block", tx.BlockNumber.Value()),
				zap.Int("count", len(upgradedLogs)),
			)

			idx.IndexContractUpgrades(fetchedBlock.Block.Number.Value(), upgradedLogs, false)
		}
	}
	return nil
}

func (idx *Indexer) IndexFetchedBlock(fetchedBlock *fetcher.FetchedBlock) (*storage.Block, bool, error) {
	blockNumber := fetchedBlock.Block.Number.Value()
	blockHash := fetchedBlock.Block.Hash.Value()

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

	insertedBlock, err := idx.MetadataStore.InsertBlockAtHeight(blockNumber, blockHash, fetchedBlock.Block.Timestamp.Value())
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

func (idx *Indexer) IsInterestingAddress(addr string) bool {
	return slices.Contains(idx.Config.GetInterestingAddressForConfigEnv(), addr)
}

func (idx *Indexer) IsInterestingTransaction(txn *ethereum.EthereumTransaction, receipt *ethereum.EthereumTransactionReceipt) bool {
	address := txn.To.Value()
	contractAddress := receipt.ContractAddress.Value()

	if (address != "" && idx.IsInterestingAddress(address)) || (contractAddress != "" && idx.IsInterestingAddress(contractAddress)) {
		return true
	}

	return false
}

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

func (idx *Indexer) IndexTransaction(
	block *storage.Block,
	tx *ethereum.EthereumTransaction,
	receipt *ethereum.EthereumTransactionReceipt,
) (*storage.Transaction, error) {
	return idx.MetadataStore.InsertBlockTransaction(
		block.Number,
		block.BlockTime,
		tx.Hash.Value(),
		tx.Index.Value(),
		tx.From.Value(),
		tx.To.Value(),
		receipt.ContractAddress.Value(),
		receipt.GetBytecodeHash(),
	)
}

func (idx *Indexer) FindAndHandleContractCreationForTransactions(
	transactions []*ethereum.EthereumTransaction,
	receipts map[string]*ethereum.EthereumTransactionReceipt,
	contractStorage map[string]string,
	blockNumber uint64,
) {
	for _, tx := range transactions {
		txReceipt, ok := receipts[tx.Hash.Value()]
		if !ok {
			continue
		}

		idx.Logger.Sugar().Debugw("processing transaction", zap.String("txHash", tx.Hash.Value()))
		contractAddress := txReceipt.ContractAddress.Value()
		eip1197StoredValue := ""
		if contractAddress != "" {
			eip1197StoredValue = contractStorage[contractAddress]
		}

		if txReceipt.ContractAddress.Value() != "" {
			idx.handleContractCreation(
				txReceipt.ContractAddress.Value(),
				txReceipt.GetBytecodeHash(),
				eip1197StoredValue,
				blockNumber,
				false,
			)
		}
	}
}

// Handles indexing a contract created by a transaction
// Does NOT include contracts that are part of logs.
func (idx *Indexer) IndexContractsForBlock(
	block *storage.Block,
	fetchedBlock *fetcher.FetchedBlock,
	reindexContract bool,
) {
	for _, tx := range fetchedBlock.Block.Transactions {
		txReceipt, ok := fetchedBlock.TxReceipts[tx.Hash.Value()]
		if !ok {
			continue
		}

		idx.Logger.Sugar().Debug("processing transaction", zap.String("txHash", tx.Hash.Value()))
		contractAddress := txReceipt.ContractAddress.Value()
		eip1967StoredValue := ""
		if contractAddress != "" {
			eip1967StoredValue = fetchedBlock.ContractStorage[contractAddress]
		}

		// If the transaction created a contract, index it
		if txReceipt.ContractAddress.Value() != "" {
			idx.handleContractCreation(
				txReceipt.ContractAddress.Value(),
				txReceipt.GetBytecodeHash(),
				eip1967StoredValue,
				block.Number,
				reindexContract,
			)
		}
	}
}

func (idx *Indexer) handleContractCreation(
	contractAddress string,
	bytecodeHash string,
	eip1197StoredValue string,
	blockNumber uint64,
	reindexContract bool,
) {
	_, err := idx.ContractManager.CreateContract(contractAddress, bytecodeHash, reindexContract)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to get find or create address",
			zap.Error(err),
			zap.String("contractAddress", contractAddress),
		)
	}
	if len(eip1197StoredValue) == 66 {
		idx.ContractManager.HandleProxyContractCreation(contractAddress, eip1197StoredValue, blockNumber, reindexContract)
	}
}

func (idx *Indexer) IndexLog(
	ctx context.Context,
	blockNumber uint64,
	blockTime time.Time,
	txHash string,
	txIndex uint64,
	log *parser.DecodedLog,
) (*storage.TransactionLog, error) {
	insertedLog, err := idx.MetadataStore.InsertTransactionLog(
		txHash,
		txIndex,
		blockNumber,
		blockTime,
		log,
		log.OutputData,
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
