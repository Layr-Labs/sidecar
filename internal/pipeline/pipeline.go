package pipeline

import (
	"context"

	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/fetcher"
	"github.com/Layr-Labs/go-sidecar/internal/indexer"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
)

type Pipeline struct {
	Fetcher      *fetcher.Fetcher
	Indexer      *indexer.Indexer
	BlockStore   storage.BlockStore
	Logger       *zap.Logger
	stateManager *stateManager.EigenStateManager
}

func NewPipeline(
	f *fetcher.Fetcher,
	i *indexer.Indexer,
	bs storage.BlockStore,
	sm *stateManager.EigenStateManager,
	l *zap.Logger,
) *Pipeline {
	return &Pipeline{
		Fetcher:      f,
		Indexer:      i,
		Logger:       l,
		stateManager: sm,
		BlockStore:   bs,
	}
}

func (p *Pipeline) RunForBlock(ctx context.Context, blockNumber uint64) error {
	p.Logger.Sugar().Debugw("Running pipeline for block", zap.Uint64("blockNumber", blockNumber))

	/*
		- Fetch block
		- Index block
		- Index transactions
		- Index logs
		- Check for contract upgrades
	*/

	block, err := p.Fetcher.FetchBlock(ctx, blockNumber)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to fetch block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}

	indexedBlock, found, err := p.Indexer.IndexFetchedBlock(block)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to index block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	if found {
		p.Logger.Sugar().Infow("Block already indexed", zap.Uint64("blockNumber", blockNumber))
	}

	if err := p.stateManager.InitProcessingForBlock(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("Failed to init processing for block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	p.Logger.Sugar().Debugw("Initialized processing for block", zap.Uint64("blockNumber", blockNumber))

	// Parse all transactions and logs for the block.
	// - If a transaction is not calling to a contract, it is ignored
	// - If a transaction has 0 interesting logs and itself is not interesting, it is ignored
	parsedTransactions, ierr := p.Indexer.ParseInterestingTransactionsAndLogs(ctx, block)
	if ierr != nil {
		p.Logger.Sugar().Errorw("Failed to parse transactions and logs",
			zap.Uint64("blockNumber", blockNumber),
			zap.String("transactionHash", ierr.TransactionHash),
			zap.Error(ierr.Err),
		)
		return err
	}
	p.Logger.Sugar().Debugw("Parsed transactions", zap.Uint64("blockNumber", blockNumber), zap.Int("count", len(parsedTransactions)))

	// With only interesting transactions/logs parsed, insert them into the database
	for _, pt := range parsedTransactions {
		indexedTransaction, err := p.Indexer.IndexTransaction(indexedBlock, pt.Transaction, pt.Receipt)
		if err != nil {
			p.Logger.Sugar().Errorw("Failed to index transaction",
				zap.Uint64("blockNumber", blockNumber),
				zap.String("transactionHash", pt.Transaction.Hash.Value()),
				zap.Error(err),
			)
			return err
		}
		p.Logger.Sugar().Debugw("Indexed transaction", zap.Uint64("blockNumber", blockNumber), zap.String("transactionHash", indexedTransaction.TransactionHash))

		for _, log := range pt.Logs {
			indexedLog, err := p.Indexer.IndexLog(
				ctx,
				indexedBlock.Number,
				indexedTransaction.BlockTime,
				indexedTransaction.TransactionHash,
				indexedTransaction.TransactionIndex,
				log,
			)
			if err != nil {
				p.Logger.Sugar().Errorw("Failed to index log",
					zap.Uint64("blockNumber", blockNumber),
					zap.String("transactionHash", pt.Transaction.Hash.Value()),
					zap.Uint64("logIndex", log.LogIndex),
					zap.Error(err),
				)
				return err
			}
			p.Logger.Sugar().Debugw("Indexed log",
				zap.Uint64("blockNumber", blockNumber),
				zap.String("transactionHash", indexedTransaction.TransactionHash),
				zap.Uint64("logIndex", log.LogIndex),
			)

			if err := p.stateManager.HandleLogStateChange(indexedLog); err != nil {
				p.Logger.Sugar().Errorw("Failed to handle log state change",
					zap.Uint64("blockNumber", blockNumber),
					zap.String("transactionHash", pt.Transaction.Hash.Value()),
					zap.Uint64("logIndex", log.LogIndex),
					zap.Error(err),
				)
				return err
			}
		}
		// Check the logs for any contract upgrades.
		upgradedLogs := p.Indexer.FindContractUpgradedLogs(pt.Logs)
		if len(upgradedLogs) > 0 {
			p.Logger.Sugar().Debugw("Found contract upgrade logs",
				zap.String("txHash", pt.Transaction.Hash.Value()),
				zap.Uint64("block", pt.Transaction.BlockNumber.Value()),
				zap.Int("count", len(upgradedLogs)),
			)

			p.Indexer.IndexContractUpgrades(block.Block.Number.Value(), upgradedLogs, false)
		}
	}

	interestingTransactions := p.Indexer.FilterInterestingTransactions(indexedBlock, block)
	if len(interestingTransactions) > 0 {
		// If we have interesting transactions, check for contract creations.
		// Really though this probably should never get reached since we only care about interesting transactions
		// which are hard coded and any implementations of proxies would get get processed above as part of the upgrade check
		p.Indexer.FindAndHandleContractCreationForTransactions(interestingTransactions, block.TxReceipts, block.ContractStorage, blockNumber)
	}

	if block.Block.Number.Value()%3600 == 0 {
		p.Logger.Sugar().Infow("Indexing OperatorRestakedStrategies", zap.Uint64("blockNumber", block.Block.Number.Value()))
		if err := p.Indexer.ProcessRestakedStrategiesForBlock(ctx, block.Block.Number.Value()); err != nil {
			p.Logger.Sugar().Errorw("Failed to process restaked strategies", zap.Uint64("blockNumber", block.Block.Number.Value()), zap.Error(err))
			return err
		}
	}

	if err := p.stateManager.CommitFinalState(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("Failed to commit final state", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}

	stateRoot, err := p.stateManager.GenerateStateRoot(blockNumber, block.Block.Hash.Value())
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to generate state root", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}

	sr, err := p.stateManager.WriteStateRoot(blockNumber, block.Block.Hash.Value(), stateRoot)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to write state root", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
	} else {
		p.Logger.Sugar().Debugw("Wrote state root", zap.Uint64("blockNumber", blockNumber), zap.Any("stateRoot", sr))
	}

	_ = p.stateManager.CleanupProcessedStateForBlock(blockNumber)

	return err
}
