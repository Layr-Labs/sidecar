package backfiller

import (
	"context"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
)

type Backfiller struct {
	EthClient     *ethereum.Client
	Indexer       *indexer.Indexer
	Fetcher       *fetcher.Fetcher
	MetadataStore storage.BlockStore
	Logger        *zap.Logger
	Config        *config.Config
}

func NewBackfiller(ethClient *ethereum.Client, indexer *indexer.Indexer, fetcher *fetcher.Fetcher, metadataStore storage.BlockStore, cfg *config.Config, l *zap.Logger) *Backfiller {
	return &Backfiller{
		EthClient:     ethClient,
		Indexer:       indexer,
		Fetcher:       fetcher,
		MetadataStore: metadataStore,
		Logger:        l,
		Config:        cfg,
	}
}

func (b *Backfiller) Backfill(ctx context.Context, blockNumber uint64, addresses []string) ([]*ethereum.EthereumTransactionReceipt, error) {
	block, err := b.MetadataStore.GetBlockByNumber(blockNumber)
	if err != nil {
		b.Logger.Sugar().Errorw("Failed to get block by number",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
		)
		return nil, err
	}

	receipts, err := b.EthClient.GetBlockReceipts(ctx, blockNumber)
	if err != nil {
		b.Logger.Sugar().Errorw("failed to get block receipts", zap.Error(err))
		return nil, err
	}

	indexedTransactions := make([]*storage.Transaction, 0)
	indexedTransactionLogs := make([]*storage.TransactionLog, 0)
	for _, receipt := range receipts {
		contractAddress := receipt.GetTargetAddress()
		// Skip if the contract address is not in the list of addresses
		if !contains(addresses, contractAddress.Value()) {
			continue
		}

		tx, err := b.EthClient.GetTransactionByHash(ctx, receipt.TransactionHash.Value())
		if err != nil {
			b.Logger.Sugar().Errorw("Failed to get transaction by hash",
				zap.Error(err),
				zap.String("transactionHash", receipt.TransactionHash.Value()),
			)
			return nil, err
		}

		indexedTransaction, err := b.Indexer.IndexTransaction(block, tx, receipt)
		if err != nil {
			b.Logger.Sugar().Errorw("Failed to index transaction",
				zap.Uint64("blockNumber", blockNumber),
				zap.String("transactionHash", tx.Hash.Value()),
				zap.Error(err),
			)
			return nil, err
		}
		indexedTransactions = append(indexedTransactions, indexedTransaction)

		b.Logger.Sugar().Debugw("Indexed transaction",
			zap.Uint64("blockNumber", blockNumber),
			zap.String("transactionHash", indexedTransaction.TransactionHash),
		)

		parsedTransactionAndLogs, err := b.Indexer.ParseTransactionLogs(tx, receipt)
		if err != nil {
			b.Logger.Sugar().Errorw("Failed to parse transaction logs",
				zap.Error(err),
				zap.String("transactionHash", receipt.TransactionHash.Value()),
			)
		}

		for _, log := range receipt.Logs {
			indexedLog, err := b.Indexer.IndexLog(
				ctx,
				block.Number,
				receipt.TransactionHash.Value(),
				receipt.TransactionIndex.Value(),
				decodedLog,
			)
			if err != nil {
				b.Logger.Sugar().Errorw("Failed to index log",
					zap.Uint64("blockNumber", blockNumber),
					zap.String("transactionHash", receipt.TransactionHash.Value()),
					zap.Uint64("logIndex", log.LogIndex.Value()),
					zap.Error(err),
				)
				return nil, err
			}
			indexedTransactionLogs = append(indexedTransactionLogs, indexedLog)
			b.Logger.Sugar().Debugw("Indexed log",
				zap.Uint64("blockNumber", blockNumber),
				zap.String("transactionHash", receipt.TransactionHash.Value()),
				zap.Uint64("logIndex", log.LogIndex.Value()),
			)
			// TODO(serichoi): add contract upgrade handling
		}
	}

	return receipts, nil
}

func contains(slice []string, item string) bool {
	for _, i := range slice {
		if i == item {
			return true
		}
	}
	return false
}
