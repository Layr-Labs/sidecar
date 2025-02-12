package indexer

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"gorm.io/gorm"
	"slices"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
)

type Indexer struct {
	Logger          *zap.Logger
	MetadataStore   storage.BlockStore
	ContractStore   contractStore.ContractStore
	ContractManager *contractManager.ContractManager
	Fetcher         *fetcher.Fetcher
	EthereumClient  *ethereum.Client
	Config          *config.Config
	ContractCaller  contractCaller.IContractCaller
	db              *gorm.DB
}

type IndexErrorType int

const (
	IndexError_ReceiptNotFound          IndexErrorType = 1
	IndexError_FailedToParseTransaction IndexErrorType = 2
	IndexError_FailedToCombineAbis      IndexErrorType = 3
	IndexError_FailedToFindContract     IndexErrorType = 4
	IndexError_FailedToParseAbi         IndexErrorType = 5
	IndexError_EmptyAbi                 IndexErrorType = 6
	IndexError_FailedToDecodeLog        IndexErrorType = 7
)

type IndexError struct {
	Type            IndexErrorType
	Err             error
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
	Metadata        map[string]interface{}
	Message         string
}

func (e *IndexError) Error() string {
	return fmt.Sprintf("IndexError: %s", e.Err.Error())
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

func (e *IndexError) WithLogIndex(logIndex uint64) *IndexError {
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
	cm *contractManager.ContractManager,
	e *ethereum.Client,
	f *fetcher.Fetcher,
	cc contractCaller.IContractCaller,
	grm *gorm.DB,
	l *zap.Logger,
	cfg *config.Config,
) *Indexer {
	return &Indexer{
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
			return nil, NewIndexError(IndexError_ReceiptNotFound, fmt.Errorf("receipt not found for transaction")).
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

func (idx *Indexer) IndexContractUpgrade(ctx context.Context, blockNumber uint64, upgradedLog *storage.TransactionLog) error {
	// the new address that the contract points to
	newProxiedAddress := ""

	// Check the arguments for the new address. EIP-1967 contracts include this as an argument.
	// Otherwise, we'll check the storage slot
	for _, arg := range upgradedLog.Arguments {
		if arg.Name == "implementation" && arg.Value != "" && arg.Value != nil {
			newProxiedAddress = arg.Value.(common.Address).String()
		}
	}

	// check the storage slot at the provided block number of the transaction
	if newProxiedAddress == "" {
		storageValue, err := idx.Fetcher.GetContractStorageSlot(context.Background(), upgradedLog.Address, blockNumber)
		if err != nil || storageValue == "" {
			idx.Logger.Sugar().Errorw("Failed to get storage value",
				zap.Error(err),
				zap.Uint64("block", blockNumber),
				zap.String("upgradedLogAddress", upgradedLog.Address)
			)
			return err
		if len(storageValue) != 66 {
			idx.Logger.Sugar().Errorw("Invalid storage value",
				zap.Uint64("block", blockNumber),
				zap.String("storageValue", storageValue)
			)
			return err
		}
		
		newProxiedAddress = storageValue[26:]
	}

	if newProxiedAddress == "" {
		idx.Logger.Sugar().Debugw("No new proxied address found", zap.String("address", upgradedLog.Address))
		return nil
	}

	_, err := idx.ContractManager.CreateProxyContract(upgradedLog.Address, newProxiedAddress, blockNumber, reindexContract)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to create proxy contract", zap.Error(err))
		return err
	}

	idx.Logger.Sugar().Infow("Upgraded proxy contract", zap.String("contractAddress", upgradedLog.Address), zap.String("proxyContractAddress", newProxiedAddress))
	return nil
}

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

func (idx *Indexer) IsInterestingAddress(addr string) bool {
	if addr == "" {
		return false
	}
	return slices.Contains(idx.Config.GetInterestingAddressForConfigEnv(), strings.ToLower(addr))
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
		tx.Hash.Value(),
		tx.Index.Value(),
		tx.From.Value(),
		tx.To.Value(),
		receipt.ContractAddress.Value(),
		receipt.GetBytecodeHash(),
	)
}

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
