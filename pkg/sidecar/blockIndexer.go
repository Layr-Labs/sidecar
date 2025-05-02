package sidecar

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"

	"github.com/syndtr/goleveldb/leveldb/errors"

	"go.uber.org/zap"
)

func (s *Sidecar) GetLastIndexedBlock() (int64, error) {
	block, err := s.Storage.GetLatestBlock()
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to get last indexed block", zap.Error(err))
		return 0, err
	}
	return int64(block.Number), nil
}

func (s *Sidecar) StartIndexing(ctx context.Context) {
	// Start indexing from the given block number
	// Once at tip, begin listening for new blocks
	if err := s.IndexFromCurrentToTip(ctx); err != nil {
		s.Logger.Sugar().Fatalw("Failed to index from current to tip", zap.Error(err))
	}

	s.Logger.Sugar().Info("Backfill complete, transitioning to listening for new blocks")

	if err := s.ProcessNewBlocks(ctx); err != nil {
		s.Logger.Sugar().Fatalw("Failed to process new blocks", zap.Error(err))
	}
}

const BLOCK_POLL_INTERVAL = 6 * time.Second

func (s *Sidecar) ProcessNewBlocks(ctx context.Context) error {
	blockType := s.GlobalConfig.GetBlockType()

	s.Logger.Sugar().Infow("Processing new blocks",
		zap.String("blockType", string(blockType)))

	for {
		if s.shouldShutdown.Load() {
			s.Logger.Sugar().Infow("Shutting down block listener...")
			return nil
		}

		// Get the latest block stored in the db
		latestIndexedBlock, err := s.GetLastIndexedBlock()
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to get last indexed block", zap.Error(err))
			return errors.New("Failed to get last indexed block")
		}

		// Get the latest safe/latest block from the Ethereum node
		latestTip, err := s.EthereumClient.GetLatestBlock(ctx)
		if err != nil {
			s.Logger.Sugar().Fatalw("Failed to get latest tip", zap.Error(err))
			return errors.New("Failed to get latest tip")
		}

		if latestTip < uint64(latestIndexedBlock) {
			// If the latest tip is behind what we have indexed, this could be due to a reorg
			// We should check if we need to revert some blocks
			if blockType == config.BlockType_Latest {
				s.Logger.Sugar().Warnw("Latest tip is behind latest indexed block, possible reorg detected",
					zap.Uint64("latestTip", latestTip),
					zap.Int64("latestIndexedBlock", latestIndexedBlock))

				// Delete the corrupted states
				if err := s.DeleteCorruptedStates(ctx, latestTip+1, uint64(latestIndexedBlock)); err != nil {
					s.Logger.Sugar().Errorw("Failed to delete corrupted states during reorg handling", zap.Error(err))
					return err
				}

				// Update latestIndexedBlock to the latest valid block
				latestIndexedBlock = int64(latestTip)
				s.Logger.Sugar().Infow("Reorg handled, reset latest indexed block",
					zap.Int64("newLatestIndexedBlock", latestIndexedBlock))
			} else {
				s.Logger.Sugar().Debugw("Latest safe block is behind latest indexed block, sleeping for a bit")
				time.Sleep(BLOCK_POLL_INTERVAL)
				continue
			}
		}

		// If the latest tip is equal to the latest indexed block, sleep for a bit
		if latestTip == uint64(latestIndexedBlock) {
			s.Logger.Sugar().Debugw("Latest tip is equal to latest indexed block, sleeping for a bit")
			time.Sleep(BLOCK_POLL_INTERVAL)
			continue
		}

		// Handle new potential blocks. This is likely to be a single block, but in the event that we
		// had to pause for a few minutes to reconstitute a rewards root, this may be more than one block
		// and we'll have to catch up.
		blockDiff := latestTip - uint64(latestIndexedBlock)
		s.Logger.Sugar().Infow(fmt.Sprintf("%d new blocks detected, processing", blockDiff))

		for i := uint64(latestIndexedBlock + 1); i <= latestTip; i++ {
			if err := s.Pipeline.RunForBlock(ctx, i, false); err != nil {
				s.Logger.Sugar().Errorw("Failed to run pipeline for block",
					zap.Uint64("blockNumber", i),
					zap.Error(err),
				)
				return err
			}
		}
		s.Logger.Sugar().Infow("Processed new blocks, sleeping for a bit")
		time.Sleep(BLOCK_POLL_INTERVAL)
	}
}

type Progress struct {
	StartBlock         uint64
	LastBlockProcessed uint64
	CurrentTip         *atomic.Uint64
	AvgPerBlockMs      float64
	StartTime          time.Time
	TotalDurationMs    int64
	logger             *zap.Logger
}

func NewProgress(startBlock uint64, currentTip *atomic.Uint64, l *zap.Logger) *Progress {
	return &Progress{
		StartBlock:         startBlock,
		LastBlockProcessed: startBlock,
		CurrentTip:         currentTip,
		AvgPerBlockMs:      0,
		StartTime:          time.Now(),
		logger:             l,
	}
}

func (p *Progress) UpdateAndPrintProgress(lastBlockProcessed uint64) {
	p.LastBlockProcessed = lastBlockProcessed

	blocksProcessed := lastBlockProcessed - p.StartBlock
	currentTip := p.CurrentTip.Load()
	totalBlocksToProcess := currentTip - p.StartBlock
	blocksRemaining := currentTip - lastBlockProcessed

	if blocksProcessed == 0 || totalBlocksToProcess == 0 {
		return
	}

	pctComplete := (float64(blocksProcessed) / float64(totalBlocksToProcess)) * 100

	runningAvg := time.Since(p.StartTime).Milliseconds() / int64(blocksProcessed)

	estTimeRemainingHours := float64(runningAvg*int64(blocksRemaining)) / 1000 / 60 / 60

	p.logger.Sugar().Infow("Progress",
		zap.String("percentComplete", fmt.Sprintf("%.2f", pctComplete)),
		zap.Uint64("blocksRemaining", blocksRemaining),
		zap.Float64("estimatedTimeRemaining (hrs)", estTimeRemainingHours),
		zap.Float64("avgBlockProcessTime (ms)", float64(runningAvg)),
		zap.Uint64("currentBlock", uint64(lastBlockProcessed)),
	)
}

func (s *Sidecar) DeleteCorruptedState(startBlock, endBlock uint64) error {
	if err := s.StateManager.DeleteCorruptedState(startBlock, endBlock); err != nil {
		s.Logger.Sugar().Errorw("Failed to delete corrupted state", zap.Error(err))
		return err
	}
	if err := s.RewardsCalculator.DeleteCorruptedRewardsFromBlockHeight(startBlock); err != nil {
		s.Logger.Sugar().Errorw("Failed to purge corrupted rewards", zap.Error(err))
		return err
	}
	if err := s.Storage.DeleteCorruptedState(startBlock, endBlock); err != nil {
		s.Logger.Sugar().Errorw("Failed to delete corrupted state", zap.Error(err))
		return err
	}
	return nil
}

func (s *Sidecar) IndexFromCurrentToTip(ctx context.Context) error {
	lastIndexedBlock, err := s.GetLastIndexedBlock()
	if err != nil {
		return err
	}

	latestStateRoot, err := s.StateManager.GetLatestStateRoot()
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to get latest state root", zap.Error(err))
		return err
	}

	if latestStateRoot == nil {
		s.Logger.Sugar().Infow("No state roots found, starting from EL genesis")
		lastIndexedBlock = 0
	}

	if lastIndexedBlock == 0 {
		s.Logger.Sugar().Infow("No blocks indexed, starting from genesis block", zap.Uint64("genesisBlock", s.Config.GenesisBlockNumber))
		lastIndexedBlock = int64(s.Config.GenesisBlockNumber)
	} else {
		// if the latest state root is behind the latest block, delete the corrupted state and set the
		// latest block to the latest state root + 1
		if latestStateRoot != nil && latestStateRoot.EthBlockNumber < uint64(lastIndexedBlock) {
			s.Logger.Sugar().Infow("Latest state root is behind latest block, deleting corrupted state",
				zap.Uint64("latestStateRoot", latestStateRoot.EthBlockNumber),
				zap.Int64("lastIndexedBlock", lastIndexedBlock),
			)
			if err := s.DeleteCorruptedState(latestStateRoot.EthBlockNumber+1, uint64(lastIndexedBlock)); err != nil {
				s.Logger.Sugar().Errorw("Failed to delete corrupted state", zap.Error(err))
				return err
			}
			lastIndexedBlock = int64(latestStateRoot.EthBlockNumber + 1)
			s.Logger.Sugar().Infow("Deleted corrupted state, starting from latest state root + 1",
				zap.Uint64("latestStateRoot", latestStateRoot.EthBlockNumber),
				zap.Int64("lastIndexedBlock", lastIndexedBlock),
			)
		} else {
			// This should tehcnically never happen, but if the latest state root is ahead of the latest block,
			// something is very wrong and we should fail.
			if latestStateRoot.EthBlockNumber > uint64(lastIndexedBlock) {
				return fmt.Errorf("Latest state root (%d) is ahead of latest stored block (%d), which should never happen, so something is very wrong", latestStateRoot.EthBlockNumber, lastIndexedBlock)
			}
			if latestStateRoot.EthBlockNumber == uint64(lastIndexedBlock) {
				s.Logger.Sugar().Infow("Latest block and latest state root are in sync, starting from latest block + 1",
					zap.Int64("latestBlock", lastIndexedBlock),
					zap.Uint64("latestStateRootBlock", latestStateRoot.EthBlockNumber),
				)
			}
			lastIndexedBlock++
		}
	}

	retryCount := 0
	var latestSafeBlockNumber uint64

	for retryCount < 3 {
		// Get the latest safe block as a starting point
		latestSafe, err := s.EthereumClient.GetLatestBlock(ctx)
		if err != nil {
			s.Logger.Sugar().Fatalw("Failed to get current tip", zap.Error(err))
		}
		s.Logger.Sugar().Infow("Current tip", zap.Uint64("currentTip", latestSafe))

		if latestSafe >= uint64(lastIndexedBlock) {
			s.Logger.Sugar().Infow("Current tip is greater than latest block, starting indexing process", zap.Uint64("currentTip", latestSafe))
			latestSafeBlockNumber = latestSafe
			break
		}

		if latestSafe < uint64(lastIndexedBlock) {
			if retryCount == 2 {
				s.Logger.Sugar().Fatalw("Current tip is less than latest block, but retry count is 2, exiting")
				return errors.New("Current tip is less than latest block, but retry count is 2, exiting")
			}
			s.Logger.Sugar().Infow("Current tip is less than latest block sleeping for 7 minutes to allow for the node to catch up")
			time.Sleep(7 * time.Minute)
		}
		retryCount++
	}

	s.Logger.Sugar().Infow("Indexing from current to tip",
		zap.Uint64("currentTip", latestSafeBlockNumber),
		zap.Int64("lastIndexedBlock", lastIndexedBlock),
		zap.Uint64("difference", latestSafeBlockNumber-uint64(lastIndexedBlock)),
	)

	// Use an atomic variable to track the current tip
	currentTip := atomic.Uint64{}
	currentTip.Store(latestSafeBlockNumber)

	indexComplete := atomic.Bool{}
	indexComplete.Store(false)
	defer indexComplete.Store(true)

	// Every 10 seconds, check to see if the current tip has changed while the backfill/sync
	// process is still running. If it has changed, update the value which will extend the loop
	// to include the newly discovered blocks.
	go func() {
		for {
			time.Sleep(time.Second * 30)
			if s.shouldShutdown.Load() {
				s.Logger.Sugar().Infow("Shutting down block listener...")
				return
			}
			if indexComplete.Load() {
				s.Logger.Sugar().Infow("Indexing complete, shutting down tip listener")
				return
			}
			latestTip, err := s.EthereumClient.GetLatestBlock(ctx)
			if err != nil {
				s.Logger.Sugar().Errorw("Failed to get latest tip", zap.Error(err))
				continue
			}
			ct := currentTip.Load()
			if latestTip > ct {
				s.Logger.Sugar().Infow("New tip found, updating",
					zap.Uint64("latestTip", latestTip),
					zap.Uint64("currentTip", ct),
				)
				currentTip.Store(latestTip)
			}
		}
	}()

	//nolint:all
	currentBlock := lastIndexedBlock

	progress := NewProgress(uint64(currentBlock), &currentTip, s.Logger)

	s.Logger.Sugar().Infow("Starting indexing process", zap.Int64("currentBlock", currentBlock), zap.Uint64("currentTip", currentTip.Load()))

	for uint64(currentBlock) <= currentTip.Load() {
		if s.shouldShutdown.Load() {
			s.Logger.Sugar().Infow("Shutting down block processor")
			return nil
		}
		tip := currentTip.Load()

		batchEndBlock := int64(currentBlock + 100)
		if batchEndBlock > int64(tip) {
			batchEndBlock = int64(tip)
		}
		if err := s.Pipeline.RunForBlockBatch(ctx, uint64(currentBlock), uint64(batchEndBlock), true); err != nil {
			s.Logger.Sugar().Errorw("Failed to run pipeline for block batch",
				zap.Error(err),
				zap.Uint64("startBlock", uint64(currentBlock)),
				zap.Int64("batchEndBlock", batchEndBlock),
			)
			return err
		}
		progress.UpdateAndPrintProgress(uint64(batchEndBlock))

		currentBlock = batchEndBlock + 1
	}

	return nil
}

// DeleteCorruptedStates deletes all corrupted state between the given block range
func (s *Sidecar) DeleteCorruptedStates(ctx context.Context, startBlock uint64, endBlock uint64) (err error) {
	// Use defer to capture any error and ensure proper logging
	defer func() {
		if err != nil {
			s.Logger.Sugar().Errorw("Failed to complete deletion of corrupted states - operation was aborted",
				zap.Error(err),
				zap.Uint64("startBlock", startBlock),
				zap.Uint64("endBlock", endBlock))
		} else {
			s.Logger.Sugar().Infow("Successfully deleted all corrupted states",
				zap.Uint64("startBlock", startBlock),
				zap.Uint64("endBlock", endBlock))
		}
	}()

	// StateManager
	stateManagerErr := s.StateManager.DeleteCorruptedState(startBlock, endBlock)
	if stateManagerErr != nil {
		s.Logger.Sugar().Errorw("Failed to delete corrupted state from StateManager",
			zap.Error(stateManagerErr),
			zap.Uint64("startBlock", startBlock),
			zap.Uint64("endBlock", endBlock))
		err = fmt.Errorf("failed to delete corrupted state from StateManager: %w", stateManagerErr)
		return
	}

	// RewardsCalculator
	rewardsErr := s.RewardsCalculator.DeleteCorruptedRewardsFromBlockHeight(startBlock)
	if rewardsErr != nil {
		s.Logger.Sugar().Errorw("Failed to delete corrupted rewards",
			zap.Error(rewardsErr),
			zap.Uint64("startBlock", startBlock))
		err = fmt.Errorf("failed to delete corrupted rewards: %w", rewardsErr)
		return
	}

	// Storage
	storageErr := s.Storage.DeleteCorruptedState(startBlock, endBlock)
	if storageErr != nil {
		s.Logger.Sugar().Errorw("Failed to delete corrupted state from Storage",
			zap.Error(storageErr),
			zap.Uint64("startBlock", startBlock),
			zap.Uint64("endBlock", endBlock))
		err = fmt.Errorf("failed to delete corrupted state from Storage: %w", storageErr)
		return
	}

	return nil
}
