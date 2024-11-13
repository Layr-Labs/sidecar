package pipeline

import (
	"context"
	"errors"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"slices"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"go.uber.org/zap"
)

type Pipeline struct {
	Fetcher           *fetcher.Fetcher
	Indexer           *indexer.Indexer
	BlockStore        storage.BlockStore
	Logger            *zap.Logger
	stateManager      *stateManager.EigenStateManager
	rewardsCalculator *rewards.RewardsCalculator
	globalConfig      *config.Config
}

func NewPipeline(
	f *fetcher.Fetcher,
	i *indexer.Indexer,
	bs storage.BlockStore,
	sm *stateManager.EigenStateManager,
	rc *rewards.RewardsCalculator,
	gc *config.Config,
	l *zap.Logger,
) *Pipeline {
	return &Pipeline{
		Fetcher:           f,
		Indexer:           i,
		Logger:            l,
		stateManager:      sm,
		rewardsCalculator: rc,
		BlockStore:        bs,
		globalConfig:      gc,
	}
}

func (p *Pipeline) RunForFetchedBlock(ctx context.Context, block *fetcher.FetchedBlock, isBackfill bool) error {
	blockNumber := block.Block.Number.Value()

	totalRunTime := time.Now()
	blockFetchTime := time.Now()

	indexedBlock, found, err := p.Indexer.IndexFetchedBlock(block)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to index block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	if found {
		p.Logger.Sugar().Infow("Block already indexed", zap.Uint64("blockNumber", blockNumber))
	}
	p.Logger.Sugar().Debugw("Indexed block",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
	)

	blockFetchTime = time.Now()

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
	p.Logger.Sugar().Debugw("Parsed transactions",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int("count", len(parsedTransactions)),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
	)

	if err := p.stateManager.InitProcessingForBlock(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("Failed to init processing for block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	p.Logger.Sugar().Debugw("Initialized processing for block", zap.Uint64("blockNumber", blockNumber))

	p.Logger.Sugar().Debugw("Handling parsed transactions", zap.Int("count", len(parsedTransactions)), zap.Uint64("blockNumber", blockNumber))

	// With only interesting transactions/logs parsed, insert them into the database
	blockFetchTime = time.Now()
	for _, pt := range parsedTransactions {
		transactionTime := time.Now()

		indexedTransaction, err := p.Indexer.IndexTransaction(indexedBlock, pt.Transaction, pt.Receipt)
		if err != nil {
			p.Logger.Sugar().Errorw("Failed to index transaction",
				zap.Uint64("blockNumber", blockNumber),
				zap.String("transactionHash", pt.Transaction.Hash.Value()),
				zap.Error(err),
			)
			return err
		}
		p.Logger.Sugar().Debugw("Indexed transaction",
			zap.Uint64("blockNumber", blockNumber),
			zap.String("transactionHash", indexedTransaction.TransactionHash),
		)

		for _, log := range pt.Logs {
			indexedLog, err := p.Indexer.IndexLog(
				ctx,
				indexedBlock.Number,
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
		p.Logger.Sugar().Debugw("Handled log state changes",
			zap.Uint64("blockNumber", blockNumber),
			zap.String("transactionHash", indexedTransaction.TransactionHash),
			zap.Duration("indexTime", time.Since(transactionTime)),
		)
	}
	p.Logger.Sugar().Debugw("Handled all log state changes",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
	)

	if block.Block.Number.Value()%3600 == 0 {
		p.Logger.Sugar().Infow("Indexing OperatorRestakedStrategies", zap.Uint64("blockNumber", block.Block.Number.Value()))
		if err := p.Indexer.ProcessRestakedStrategiesForBlock(ctx, block.Block.Number.Value()); err != nil {
			p.Logger.Sugar().Errorw("Failed to process restaked strategies", zap.Uint64("blockNumber", block.Block.Number.Value()), zap.Error(err))
			return err
		}
	}

	blockFetchTime = time.Now()
	if err := p.stateManager.CommitFinalState(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("Failed to commit final state", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	p.Logger.Sugar().Debugw("Committed final state", zap.Uint64("blockNumber", blockNumber), zap.Duration("indexTime", time.Since(blockFetchTime)))

	p.Logger.Sugar().Debugw("Checking for rewards to validate", zap.Uint64("blockNumber", blockNumber))

	distributionRoots, err := p.stateManager.GetSubmittedDistributionRoots(blockNumber)
	if err == nil && distributionRoots != nil {
		for _, rs := range distributionRoots {
			rewardStartTime := time.Now()

			// first check to see if the root was disabled. If it was, it's possible we introduced changes that
			// would make the root impossible to re-create
			rewardsRoot, err := p.Indexer.ContractCaller.GetDistributionRootByIndex(ctx, rs.RootIndex)
			if err != nil {
				p.Logger.Sugar().Errorw("Failed to get rewards root by index",
					zap.Uint64("blockNumber", blockNumber),
					zap.Uint64("rootIndex", rs.RootIndex),
					zap.Error(err),
				)
				return err
			}
			if rewardsRoot.Disabled {
				p.Logger.Sugar().Warnw("Root is disabled, skipping rewards validation",
					zap.Uint64("blockNumber", blockNumber),
					zap.Uint64("rootIndex", rs.RootIndex),
					zap.String("root", rs.Root),
				)
				continue
			}

			// The RewardsCalculationEnd date is the max(snapshot) from the gold table at the time, NOT the exclusive
			// cutoff date that was actually used to generate the rewards. To get that proper cutoff date, we need
			// to add 1 day to the RewardsCalculationEnd date.
			//
			// For example, the first mainnet root has a rewardsCalculationEnd of 2024-08-01 00:00:00, but
			// the cutoff date used to generate that data is actually 2024-08-02 00:00:00.
			snapshotDate := rs.RewardsCalculationEnd.UTC().Add(time.Hour * 24).Format(time.DateOnly)

			p.Logger.Sugar().Infow("Calculating rewards for snapshot date",
				zap.String("snapshotDate", snapshotDate),
				zap.Uint64("blockNumber", blockNumber),
			)

			if err = p.rewardsCalculator.CalculateRewardsForSnapshotDate(snapshotDate); err != nil {
				p.Logger.Sugar().Errorw("Failed to calculate rewards for snapshot date",
					zap.String("snapshotDate", snapshotDate), zap.Error(err),
					zap.Uint64("blockNumber", blockNumber),
					zap.Any("distributionRoot", rs),
				)
				return err
			}

			p.Logger.Sugar().Infow("Merkelizing rewards for snapshot date",
				zap.String("snapshotDate", snapshotDate),
				zap.Uint64("blockNumber", blockNumber),
			)
			accountTree, _, err := p.rewardsCalculator.MerkelizeRewardsForSnapshot(snapshotDate)
			if err != nil {
				p.Logger.Sugar().Errorw("Failed to merkelize rewards for snapshot date",
					zap.String("snapshotDate", snapshotDate), zap.Error(err),
					zap.Uint64("blockNumber", blockNumber),
				)
				return err
			}
			root := utils.ConvertBytesToString(accountTree.Root())

			rewardsTotalTimeMs := time.Since(rewardStartTime).Milliseconds()

			// nolint:all
			if strings.ToLower(root) != strings.ToLower(rs.Root) {
				if !p.globalConfig.CanIgnoreIncorrectRewardsRoot(blockNumber) {
					p.Logger.Sugar().Errorw("Roots do not match",
						zap.String("snapshotDate", snapshotDate),
						zap.Uint64("blockNumber", blockNumber),
						zap.String("expectedRoot", rs.Root),
						zap.String("actualRoot", root),
						zap.Int64("rewardsTotalTimeMs", rewardsTotalTimeMs),
					)
					return errors.New("roots do not match")
				}
				p.Logger.Sugar().Warnw("Roots do not match, but allowed to ignore",
					zap.String("snapshotDate", snapshotDate),
					zap.Uint64("blockNumber", blockNumber),
					zap.String("expectedRoot", rs.Root),
					zap.String("actualRoot", root),
					zap.Int64("rewardsTotalTimeMs", rewardsTotalTimeMs),
				)
			} else {
				p.Logger.Sugar().Infow("Roots match", zap.String("snapshotDate", snapshotDate), zap.Uint64("blockNumber", blockNumber))
			}
		}
	}

	blockFetchTime = time.Now()
	stateRoot, err := p.stateManager.GenerateStateRoot(blockNumber, block.Block.Hash.Value())
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to generate state root", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	p.Logger.Sugar().Debugw("Generated state root", zap.Duration("indexTime", time.Since(blockFetchTime)))

	blockFetchTime = time.Now()
	sr, err := p.stateManager.WriteStateRoot(blockNumber, block.Block.Hash.Value(), stateRoot)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to write state root", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
	} else {
		p.Logger.Sugar().Debugw("Wrote state root", zap.Uint64("blockNumber", blockNumber), zap.Any("stateRoot", sr))
	}
	p.Logger.Sugar().Debugw("Finished processing block",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
		zap.Int64("totalTime", time.Since(totalRunTime).Milliseconds()),
	)

	// Push cleanup to the background since it doesnt need to be blocking
	go func() {
		_ = p.stateManager.CleanupProcessedStateForBlock(blockNumber)
	}()

	return err
}

func (p *Pipeline) RunForBlock(ctx context.Context, blockNumber uint64, isBackfill bool) error {
	p.Logger.Sugar().Debugw("Running pipeline for block", zap.Uint64("blockNumber", blockNumber))

	blockFetchTime := time.Now()
	block, err := p.Fetcher.FetchBlock(ctx, blockNumber)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to fetch block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		return err
	}
	p.Logger.Sugar().Debugw("Fetched block",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int64("fetchTime", time.Since(blockFetchTime).Milliseconds()),
	)

	return p.RunForFetchedBlock(ctx, block, isBackfill)
}

func (p *Pipeline) RunForBlockBatch(ctx context.Context, startBlock uint64, endBlock uint64, isBackfill bool) error {
	p.Logger.Sugar().Debugw("Running pipeline for block batch",
		zap.Uint64("startBlock", startBlock),
		zap.Uint64("endBlock", endBlock),
	)

	fetchedBlocks, err := p.Fetcher.FetchBlocksWithRetries(ctx, startBlock, endBlock)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to fetch blocks", zap.Uint64("startBlock", startBlock), zap.Uint64("endBlock", endBlock), zap.Error(err))
		return err
	}

	// sort blocks ascending
	slices.SortFunc(fetchedBlocks, func(b1, b2 *fetcher.FetchedBlock) int {
		return int(b1.Block.Number.Value() - b2.Block.Number.Value())
	})

	for _, block := range fetchedBlocks {
		if err := p.RunForFetchedBlock(ctx, block, isBackfill); err != nil {
			p.Logger.Sugar().Errorw("Failed to run pipeline for fetched block", zap.Uint64("blockNumber", block.Block.Number.Value()), zap.Error(err))
			return err
		}
	}

	return nil
}
