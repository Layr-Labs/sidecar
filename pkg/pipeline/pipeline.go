package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/eventBus/eventBusTypes"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/metrics/metricsTypes"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"go.uber.org/zap"
	ddTracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// Pipeline orchestrates the entire data processing workflow, coordinating between
// fetching blockchain data, indexing it, managing state, calculating rewards,
// and storing results. It serves as the central component that ties together
// all the different subsystems.
type Pipeline struct {
	Fetcher           *fetcher.Fetcher
	Indexer           *indexer.Indexer
	BlockStore        storage.BlockStore
	contractStore     contractStore.ContractStore
	contractManager   *contractManager.ContractManager
	Logger            *zap.Logger
	stateManager      *stateManager.EigenStateManager
	metaStateManager  *metaStateManager.MetaStateManager
	rewardsCalculator *rewards.RewardsCalculator
	rcq               *rewardsCalculatorQueue.RewardsCalculatorQueue
	globalConfig      *config.Config
	metricsSink       *metrics.MetricsSink
	eventBus          eventBusTypes.IEventBus
}

// NewPipeline creates and initializes a new Pipeline instance with all required dependencies.
//
// Parameters:
//   - f: Fetcher for retrieving blockchain data
//   - i: Indexer for processing and storing blockchain data
//   - bs: BlockStore for persistent storage of block data
//   - sm: EigenStateManager for managing blockchain state
//   - msm: MetaStateManager for managing metadata state
//   - rc: RewardsCalculator for computing rewards
//   - rcq: RewardsCalculatorQueue for scheduling reward calculations
//   - gc: Global configuration
//   - ms: Metrics sink for reporting metrics
//   - eb: Event bus for publishing events
//   - l: Logger for logging
//
// Returns:
//   - *Pipeline: A fully initialized Pipeline instance
func NewPipeline(
	f *fetcher.Fetcher,
	i *indexer.Indexer,
	bs storage.BlockStore,
	cs contractStore.ContractStore,
	cm *contractManager.ContractManager,
	sm *stateManager.EigenStateManager,
	msm *metaStateManager.MetaStateManager,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	gc *config.Config,
	ms *metrics.MetricsSink,
	eb eventBusTypes.IEventBus,
	l *zap.Logger,
) *Pipeline {
	return &Pipeline{
		Fetcher:           f,
		Indexer:           i,
		Logger:            l,
		contractStore:     cs,
		contractManager:   cm,
		stateManager:      sm,
		metaStateManager:  msm,
		rewardsCalculator: rc,
		rcq:               rcq,
		BlockStore:        bs,
		globalConfig:      gc,
		metricsSink:       ms,
		eventBus:          eb,
	}
}

// RunForFetchedBlock processes a block that has already been fetched from the blockchain.
// This method handles the core processing logic, including indexing the block, parsing
// transactions and logs, updating state, validating rewards, and generating state roots.
//
// Parameters:
//   - ctx: Context for propagating cancellation and timeouts
//   - block: The pre-fetched block to process
//   - isBackfill: Whether this processing is part of a historical backfill operation
//
// Returns:
//   - error: Any error encountered during processing
func (p *Pipeline) RunForFetchedBlock(ctx context.Context, block *fetcher.FetchedBlock, isBackfill bool) error {
	blockNumber := block.Block.Number.Value()

	// Create a span for the entire block processing
	span, ctx := ddTracer.StartSpanFromContext(ctx, "pipeline.RunForFetchedBlock")
	span.SetTag("block_number", blockNumber)
	span.SetTag("is_backfill", isBackfill)
	span.SetTag("timestamp", block.Block.Timestamp.Value())
	defer span.Finish()

	totalRunTime := time.Now()
	calculatedRewards := false
	hasError := false
	blockFetchTime := time.Now()

	defer func() {
		_ = p.metricsSink.Timing(metricsTypes.Metric_Timing_BlockProcessDuration, time.Since(totalRunTime), []metricsTypes.MetricsLabel{
			{Name: "rewardsCalculated", Value: strconv.FormatBool(calculatedRewards)},
			{Name: "hasError", Value: strconv.FormatBool(hasError)},
		})
		span.SetTag("total_duration_ms", time.Since(totalRunTime).Milliseconds())
		span.SetTag("rewards_calculated", calculatedRewards)
		span.SetTag("has_error", hasError)
	}()

	// Check if we should calculate daily rewards
	if p.globalConfig.Rewards.CalculateRewardsDaily && !isBackfill {
		rewardsSpan, rewardsCtx := ddTracer.StartSpanFromContext(ctx, "pipeline.CalculateRewardsDaily")
		rewardsSpan.SetTag("block_number", blockNumber)

		err := p.CalculateRewardsDaily(rewardsCtx, block)
		if err != nil {
			p.Logger.Sugar().Errorw("Failed to calculate daily rewards", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
			hasError = true
			rewardsSpan.SetTag("error", true)
			rewardsSpan.SetTag("error.message", err.Error())
			rewardsSpan.Finish()
			span.SetTag("error", true)
			span.SetTag("error.message", err.Error())
			return err
		}
		rewardsSpan.Finish()
	}

	// Create a span for indexing the block
	indexSpan, _ := ddTracer.StartSpanFromContext(ctx, "pipeline.IndexFetchedBlock")
	indexSpan.SetTag("block_number", blockNumber)
	indexStartTime := time.Now()

	indexedBlock, found, err := p.Indexer.IndexFetchedBlock(block)

	indexDuration := time.Since(indexStartTime)
	indexSpan.SetTag("duration_ms", indexDuration.Milliseconds())
	indexSpan.SetTag("already_found", found)

	if err != nil {
		p.Logger.Sugar().Errorw("Failed to index block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		indexSpan.SetTag("error", true)
		indexSpan.SetTag("error.message", err.Error())
		indexSpan.Finish()
		span.SetTag("error", true)
		span.SetTag("error.message", err.Error())
		return err
	}
	indexSpan.Finish()

	if found {
		p.Logger.Sugar().Infow("Block already indexed", zap.Uint64("blockNumber", blockNumber))
		span.SetTag("already_indexed", true)
	}
	p.Logger.Sugar().Debugw("Indexed block",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
	)

	blockFetchTime = time.Now()

	// Create a span for parsing transactions
	parseSpan, parseCtx := ddTracer.StartSpanFromContext(ctx, "pipeline.ParseInterestingTransactionsAndLogs")
	parseSpan.SetTag("block_number", blockNumber)
	parseStartTime := time.Now()

	// Parse all transactions and logs for the block.
	// - If a transaction is not calling to a contract, it is ignored
	// - If a transaction has 0 interesting logs and itself is not interesting, it is ignored
	parsedTransactions, err := p.Indexer.ParseInterestingTransactionsAndLogs(parseCtx, block)

	parseDuration := time.Since(parseStartTime)
	parseSpan.SetTag("duration_ms", parseDuration.Milliseconds())
	parseSpan.SetTag("transaction_count", len(parsedTransactions))

	if err != nil {
		p.Logger.Sugar().Errorw("Failed to parse transactions and logs",
			zap.Uint64("blockNumber", blockNumber),
			zap.Error(err),
		)
		hasError = true
		parseSpan.SetTag("error", true)
		parseSpan.SetTag("error.message", err.Error())
		parseSpan.Finish()
		span.SetTag("error", true)
		span.SetTag("error.message", err.Error())
		return err
	}
	parseSpan.Finish()

	p.Logger.Sugar().Debugw("Parsed transactions",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int("count", len(parsedTransactions)),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
	)

	if err := p.stateManager.InitProcessingForBlock(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("Failed to init processing for block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		span.SetTag("error", true)
		span.SetTag("error.message", err.Error())
		return err
	}
	if err := p.metaStateManager.InitProcessingForBlock(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("MetaStateManager: Failed to init processing for block", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		span.SetTag("error", true)
		span.SetTag("error.message", err.Error())
		return err
	}

	p.Logger.Sugar().Debugw("Initialized processing for block", zap.Uint64("blockNumber", blockNumber))

	p.Logger.Sugar().Debugw("Handling parsed transactions", zap.Int("count", len(parsedTransactions)), zap.Uint64("blockNumber", blockNumber))

	// With only interesting transactions/logs parsed, insert them into the database
	indexedTransactions := make([]*storage.Transaction, 0)
	indexedTransactionLogs := make([]*storage.TransactionLog, 0)
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
			hasError = true
			return err
		}
		indexedTransactions = append(indexedTransactions, indexedTransaction)

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
				hasError = true
				return err
			}
			indexedTransactionLogs = append(indexedTransactionLogs, indexedLog)
			p.Logger.Sugar().Debugw("Indexed log",
				zap.Uint64("blockNumber", blockNumber),
				zap.String("transactionHash", indexedTransaction.TransactionHash),
				zap.Uint64("logIndex", log.LogIndex),
			)

			if err := p.stateManager.HandleLogStateChange(indexedLog, true); err != nil {
				p.Logger.Sugar().Errorw("Failed to handle log state change",
					zap.Uint64("blockNumber", blockNumber),
					zap.String("transactionHash", pt.Transaction.Hash.Value()),
					zap.Uint64("logIndex", log.LogIndex),
					zap.Error(err),
				)
				hasError = true
				return err
			}

			if err := p.metaStateManager.HandleTransactionLog(indexedLog); err != nil {
				p.Logger.Sugar().Errorw("MetaStateManager: Failed to handle log state change",
					zap.Uint64("blockNumber", blockNumber),
					zap.String("transactionHash", pt.Transaction.Hash.Value()),
					zap.Uint64("logIndex", log.LogIndex),
					zap.Error(err),
				)
				hasError = true
				return err
			}

			contract, err := p.contractStore.GetContractForAddress(strings.ToLower(log.Address))
			if err != nil {
				p.Logger.Sugar().Errorw("Failed to get contract for address", zap.String("address", log.Address), zap.Error(err))
				return err
			}

			if log.EventName == "Upgraded" && contract != nil && contract.ContractType == contractStore.ContractType_External {
				if err := p.contractManager.HandleContractUpgrade(ctx, blockNumber, log); err != nil {
					p.Logger.Sugar().Errorw("Failed to handle contract upgrade",
						zap.Uint64("blockNumber", blockNumber),
						zap.String("transactionHash", pt.Transaction.Hash.Value()),
						zap.Uint64("logIndex", log.LogIndex),
						zap.Error(err),
					)
					return err
				}
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
			hasError = true
			return err
		}
	}

	if err = p.stateManager.RunPrecommitProcessors(blockNumber); err != nil {
		p.Logger.Sugar().Errorw("Failed to run precommit processors", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		return err
	}

	blockFetchTime = time.Now()
	committedState, err := p.stateManager.CommitFinalState(blockNumber, false)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to commit final state", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		return err
	}
	_, err = p.metaStateManager.CommitFinalState(blockNumber)
	if err != nil {
		p.Logger.Sugar().Errorw("MetaStateManager: Failed to commit final state", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
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
				hasError = true
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

			if !p.globalConfig.Rewards.ValidateRewardsRoot {
				p.Logger.Sugar().Warnw("Rewards validation is disabled, skipping rewards validation",
					zap.Uint64("blockNumber", blockNumber),
					zap.Uint64("rootIndex", rs.RootIndex),
					zap.String("root", rs.Root),
				)
				continue
			}
			calculatedRewards = true

			// The RewardsCalculationEnd date is the max(snapshot) from the gold table at the time, NOT the exclusive
			// cutoff date that was actually used to generate the rewards. To get that proper cutoff date, we need
			// to add 1 day to the RewardsCalculationEnd date.
			//
			// For example, the first mainnet root has a rewardsCalculationEnd of 2024-08-01 00:00:00, but
			// the cutoff date used to generate that data is actually 2024-08-02 00:00:00.
			rewardsCalculationEnd := time.Unix(int64(rewardsRoot.RewardsCalculationEndTimestamp), 0).UTC().Format(time.DateOnly)

			cutoffDate := time.Unix(int64(rewardsRoot.RewardsCalculationEndTimestamp), 0).UTC().Add(time.Hour * 24).Format(time.DateOnly)

			p.Logger.Sugar().Infow("Calculating rewards for snapshot date",
				zap.String("cutoffDate", cutoffDate),
				zap.String("rewardsCalculationEnd", rewardsCalculationEnd),
				zap.Uint64("blockNumber", blockNumber),
			)

			msg := rewardsCalculatorQueue.RewardsCalculationData{
				CalculationType: rewardsCalculatorQueue.RewardsCalculationType_CalculateRewards,
				CutoffDate:      cutoffDate,
			}
			if _, err = p.rcq.EnqueueAndWait(ctx, msg); err != nil {
				p.Logger.Sugar().Errorw("Failed to calculate rewards for snapshot date",
					zap.String("cutoffDate", cutoffDate), zap.Error(err),
					zap.Uint64("blockNumber", blockNumber),
					zap.Any("distributionRoot", rs),
				)
				hasError = true
				return err
			}

			p.Logger.Sugar().Infow("Merkelizing rewards for snapshot date",
				zap.String("cutoffDate", cutoffDate),
				zap.Uint64("blockNumber", blockNumber),
			)
			accountTree, _, _, err := p.rewardsCalculator.MerkelizeRewardsForSnapshot(rewardsCalculationEnd)
			if err != nil {
				p.Logger.Sugar().Errorw("Failed to merkelize rewards for snapshot date",
					zap.String("cutoffDate", cutoffDate), zap.Error(err),
					zap.Uint64("blockNumber", blockNumber),
				)
				_ = p.metricsSink.Gauge(metricsTypes.Metric_Incr_RewardsMerkelizationFailed, 1, []metricsTypes.MetricsLabel{
					{
						Name:  "block_number",
						Value: fmt.Sprintf("%d", blockNumber),
					},
				})
				hasError = true
				return err
			}
			root := utils.ConvertBytesToString(accountTree.Root())

			rewardsTotalTimeMs := time.Since(rewardStartTime).Milliseconds()

			_ = p.metricsSink.Gauge(metricsTypes.Metric_Gauge_LastDistributionRootBlockHeight, float64(blockNumber), nil)

			// nolint:all
			if strings.ToLower(root) != strings.ToLower(rs.Root) {
				if !p.globalConfig.CanIgnoreIncorrectRewardsRoot(blockNumber) {
					p.Logger.Sugar().Errorw("Roots do not match",
						zap.String("cutoffDate", cutoffDate),
						zap.Uint64("blockNumber", blockNumber),
						zap.String("postedRoot", rs.Root),
						zap.String("computedRoot", root),
						zap.Int64("rewardsTotalTimeMs", rewardsTotalTimeMs),
					)
					hasError = true
					_ = p.metricsSink.Gauge(metricsTypes.Metric_Incr_RewardsRootInvalid, 1, []metricsTypes.MetricsLabel{
						{
							Name:  "block_number",
							Value: fmt.Sprintf("%d", blockNumber),
						},
					})
					return errors.New("roots do not match")
				}
				p.Logger.Sugar().Warnw("Roots do not match, but allowed to ignore",
					zap.String("cutoffDate", cutoffDate),
					zap.Uint64("blockNumber", blockNumber),
					zap.String("postedRoot", rs.Root),
					zap.String("computedRoot", root),
					zap.Int64("rewardsTotalTimeMs", rewardsTotalTimeMs),
				)
			} else {
				_ = p.metricsSink.Gauge(metricsTypes.Metric_Incr_RewardsRootVerified, 1, []metricsTypes.MetricsLabel{
					{
						Name:  "block_number",
						Value: fmt.Sprintf("%d", blockNumber),
					},
				})
				p.Logger.Sugar().Infow("Roots match", zap.String("cutoffDate", cutoffDate), zap.Uint64("blockNumber", blockNumber))
			}
		}
	}

	blockFetchTime = time.Now()
	stateRoot, err := p.stateManager.GenerateStateRoot(blockNumber, block.Block.Hash.Value())
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to generate state root", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		return err
	}
	p.Logger.Sugar().Debugw("Generated state root", zap.Duration("indexTime", time.Since(blockFetchTime)))

	blockFetchTime = time.Now()
	sr, err := p.stateManager.WriteStateRoot(blockNumber, block.Block.Hash.Value(), stateRoot)
	if err != nil {
		p.Logger.Sugar().Errorw("Failed to write state root", zap.Uint64("blockNumber", blockNumber), zap.Error(err))
		hasError = true
		return err
	} else {
		p.Logger.Sugar().Debugw("Wrote state root", zap.Uint64("blockNumber", blockNumber), zap.Any("stateRoot", sr))
	}
	p.Logger.Sugar().Debugw("Finished processing block",
		zap.Uint64("blockNumber", blockNumber),
		zap.Int64("indexTime", time.Since(blockFetchTime).Milliseconds()),
		zap.Int64("totalTime", time.Since(totalRunTime).Milliseconds()),
	)
	_ = p.metricsSink.Incr(metricsTypes.Metric_Incr_BlockProcessed, nil, 1)
	_ = p.metricsSink.Gauge(metricsTypes.Metric_Gauge_CurrentBlockHeight, float64(blockNumber), nil)
	go p.HandleBlockProcessedHook(indexedBlock, indexedTransactions, indexedTransactionLogs, sr, committedState)

	// Push cleanup to the background since it doesnt need to be blocking
	go func() {
		_ = p.stateManager.CleanupProcessedStateForBlock(blockNumber)
		_ = p.metaStateManager.CleanupProcessedStateForBlock(blockNumber)
	}()

	span.SetTag("transactions_processed", len(parsedTransactions))
	return nil
}

// RunForBlock fetches and processes a single block by its block number. This method
// handles both the fetching and processing steps, coordinating the entire pipeline
// workflow for a specific block.
//
// Parameters:
//   - ctx: Context for propagating cancellation and timeouts
//   - blockNumber: The number of the block to fetch and process
//   - isBackfill: Whether this processing is part of a historical backfill operation
//
// Returns:
//   - error: Any error encountered during fetching or processing
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

// RunForBlockBatch fetches and processes a range of blocks from startBlock to endBlock (inclusive).
// This method is useful for processing multiple blocks in a single operation, such as during
// backfilling or catching up after downtime.
//
// Parameters:
//   - ctx: Context for propagating cancellation and timeouts
//   - startBlock: The first block number in the range to process
//   - endBlock: The last block number in the range to process
//   - isBackfill: Whether this processing is part of a historical backfill operation
//
// Returns:
//   - error: Any error encountered during fetching or processing
func (p *Pipeline) RunForBlockBatch(ctx context.Context, startBlock uint64, endBlock uint64, isBackfill bool) error {
	// Create a span for the entire batch operation
	span, ctx := ddTracer.StartSpanFromContext(ctx, "pipeline.RunForBlockBatch")
	span.SetTag("start_block", startBlock)
	span.SetTag("end_block", endBlock)
	span.SetTag("block_count", endBlock-startBlock+1)
	span.SetTag("is_backfill", isBackfill)
	defer span.Finish()

	batchStartTime := time.Now()

	p.Logger.Sugar().Debugw("Running pipeline for block batch",
		zap.Uint64("startBlock", startBlock),
		zap.Uint64("endBlock", endBlock),
	)

	// Create a span for the fetch operation
	fetchSpan, fetchCtx := ddTracer.StartSpanFromContext(ctx, "pipeline.FetchBlocksWithRetries")
	fetchSpan.SetTag("start_block", startBlock)
	fetchSpan.SetTag("end_block", endBlock)
	fetchStartTime := time.Now()

	fetchedBlocks, err := p.Fetcher.FetchBlocksWithRetries(fetchCtx, startBlock, endBlock)
	fetchDuration := time.Since(fetchStartTime)
	fetchSpan.SetTag("duration_ms", fetchDuration.Milliseconds())

	if err != nil {
		p.Logger.Sugar().Errorw("Failed to fetch blocks", zap.Uint64("startBlock", startBlock), zap.Uint64("endBlock", endBlock), zap.Error(err))
		fetchSpan.SetTag("error", true)
		fetchSpan.SetTag("error.message", err.Error())
		span.SetTag("error", true)
		span.SetTag("error.message", err.Error())
		fetchSpan.Finish()
		return err
	}

	fetchSpan.SetTag("blocks_fetched", len(fetchedBlocks))
	fetchSpan.Finish()

	// sort blocks ascending
	slices.SortFunc(fetchedBlocks, func(b1, b2 *fetcher.FetchedBlock) int {
		return int(b1.Block.Number.Value() - b2.Block.Number.Value())
	})

	processStartTime := time.Now()
	successfulBlocks := 0

	for i, block := range fetchedBlocks {
		blockNumber := block.Block.Number.Value()

		// Create a span for processing each block
		blockSpan, blockCtx := ddTracer.StartSpanFromContext(ctx, "pipeline.ProcessBlock")
		blockSpan.SetTag("block_number", blockNumber)
		blockSpan.SetTag("block_index", i)
		blockSpan.SetTag("is_backfill", isBackfill)
		blockStartTime := time.Now()

		if err := p.RunForFetchedBlock(blockCtx, block, isBackfill); err != nil {
			p.Logger.Sugar().Errorw("Failed to run pipeline for fetched block",
				zap.Uint64("blockNumber", blockNumber),
				zap.Error(err),
			)
			blockSpan.SetTag("error", true)
			blockSpan.SetTag("error.message", err.Error())
			blockSpan.SetTag("duration_ms", time.Since(blockStartTime).Milliseconds())
			blockSpan.Finish()
			span.SetTag("error", true)
			span.SetTag("error.message", err.Error())
			span.SetTag("failed_block", blockNumber)
			return err
		}

		successfulBlocks++
		blockDuration := time.Since(blockStartTime)
		blockSpan.SetTag("duration_ms", blockDuration.Milliseconds())
		blockSpan.Finish()
	}

	totalDuration := time.Since(batchStartTime)
	processDuration := time.Since(processStartTime)

	span.SetTag("blocks_processed", successfulBlocks)
	span.SetTag("total_duration_ms", totalDuration.Milliseconds())
	span.SetTag("process_duration_ms", processDuration.Milliseconds())
	span.SetTag("fetch_duration_ms", fetchDuration.Milliseconds())

	if successfulBlocks > 0 {
		span.SetTag("avg_block_time_ms", float64(processDuration.Milliseconds())/float64(successfulBlocks))
	}

	return nil
}

// CalculateRewardsDaily checks if the current block crosses a day boundary (UTC)
// and if so, queues a reward calculation request. This allows one rewards calculation
// per day, even when processing multiple days in a batch.
func (p *Pipeline) CalculateRewardsDaily(ctx context.Context, block *fetcher.FetchedBlock) error {
	blockNumber := block.Block.Number.Value()

	blockTime := time.Unix(int64(block.Block.Timestamp), 0).UTC()

	// Fast path: Only blocks between midnight and 1 minute past midnight
	// have a reasonable chance of crossing day boundaries
	if blockTime.Hour() != 0 || blockTime.Minute() >= 1 {
		// Most blocks will take this path and exit quickly
		return nil
	}

	// Get previous block
	prevBlock, err := p.Fetcher.FetchBlock(ctx, blockNumber-1)
	if err != nil {
		p.Logger.Sugar().Debugw("Failed to fetch previous block for rewards check",
			zap.Uint64("blockNumber", blockNumber-1),
			zap.Error(err))
		return err
	}

	// Get previous block time in UTC
	prevBlockTime := time.Unix(int64(prevBlock.Block.Timestamp), 0).UTC()

	// Check if we've crossed a day boundary
	if prevBlockTime.Day() != blockTime.Day() {

		// Format today's date as cutoff date (YYYY-MM-DD)
		cutoffDate := blockTime.Format(time.DateOnly)

		p.Logger.Sugar().Infow("Day boundary crossed, queueing daily rewards calculation",
			zap.Uint64("blockNumber", blockNumber),
			zap.String("prevBlockTime", prevBlockTime.Format(time.RFC3339)),
			zap.String("blockTime", blockTime.Format(time.RFC3339)),
			zap.String("cutoffDate", cutoffDate),
		)

		// Queue up a rewards generation request (non-blocking)
		p.rcq.Enqueue(&rewardsCalculatorQueue.RewardsCalculationMessage{
			Data: rewardsCalculatorQueue.RewardsCalculationData{
				CalculationType: rewardsCalculatorQueue.RewardsCalculationType_CalculateRewards,
				CutoffDate:      cutoffDate,
			},
			ResponseChan: make(chan *rewardsCalculatorQueue.RewardsCalculatorResponse),
		})
	}

	return nil
}
