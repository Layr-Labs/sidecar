package sidecar

import (
	"context"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"sync/atomic"
	"time"

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

		// Get the latest safe block from the Ethereum node
		latestTip, err := s.EthereumClient.GetLatestSafeBlock(ctx)
		if err != nil {
			s.Logger.Sugar().Fatalw("Failed to get latest tip", zap.Error(err))
			return errors.New("Failed to get latest tip")
		}

		// If the latest tip is behind what we have indexed, sleep for a bit
		if latestTip < uint64(latestIndexedBlock) {
			s.Logger.Sugar().Debugw("Latest tip is behind latest indexed block, sleeping for a bit")
			time.Sleep(BLOCK_POLL_INTERVAL)
			continue
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

func (s *Sidecar) IndexFromCurrentToTip(ctx context.Context) error {
	latestBlock, err := s.GetLastIndexedBlock()
	if err != nil {
		return err
	}

	latestStateRoot, err := s.StateManager.GetLatestStateRoot()
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to get latest state root", zap.Error(err))
		return err
	}

	if latestStateRoot != nil {
		s.Logger.Sugar().Infow("Comparing latest block and latest state root",
			zap.Int64("latestBlock", latestBlock),
			zap.Uint64("latestStateRootBlock", latestStateRoot.EthBlockNumber),
		)
	} else {
		s.Logger.Sugar().Infow("No state roots found, starting from EL genesis")
		latestBlock = 0
	}

	if latestBlock == 0 {
		s.Logger.Sugar().Infow("No blocks indexed, starting from genesis block", zap.Uint64("genesisBlock", s.Config.GenesisBlockNumber))
		latestBlock = int64(s.Config.GenesisBlockNumber)
	} else {
		// if the latest state root is behind the latest block, delete the corrupted state and set the
		// latest block to the latest state root + 1
		if latestStateRoot != nil && latestStateRoot.EthBlockNumber < uint64(latestBlock) {
			s.Logger.Sugar().Infow("Latest state root is behind latest block, deleting corrupted state",
				zap.Uint64("latestStateRoot", latestStateRoot.EthBlockNumber),
				zap.Int64("latestBlock", latestBlock),
			)
			if err := s.StateManager.DeleteCorruptedState(latestStateRoot.EthBlockNumber+1, uint64(latestBlock)); err != nil {
				s.Logger.Sugar().Errorw("Failed to delete corrupted state", zap.Error(err))
				return err
			}
			if err := s.RewardsCalculator.DeleteCorruptedRewardsFromBlockHeight(uint64(latestStateRoot.EthBlockNumber + 1)); err != nil {
				s.Logger.Sugar().Errorw("Failed to purge corrupted rewards", zap.Error(err))
				return err
			}
			if err := s.Storage.DeleteCorruptedState(uint64(latestStateRoot.EthBlockNumber+1), uint64(latestBlock)); err != nil {
				s.Logger.Sugar().Errorw("Failed to delete corrupted state", zap.Error(err))
				return err
			}
			latestBlock = int64(latestStateRoot.EthBlockNumber + 1)
			s.Logger.Sugar().Infow("Deleted corrupted state, starting from latest state root + 1",
				zap.Uint64("latestStateRoot", latestStateRoot.EthBlockNumber),
				zap.Int64("latestBlock", latestBlock),
			)
		} else {
			// otherwise, start from the latest block + 1
			latestBlock++
		}
	}

	// Get the latest safe block as a starting point
	blockNumber, err := s.EthereumClient.GetLatestSafeBlock(ctx)
	if err != nil {
		s.Logger.Sugar().Fatalw("Failed to get current tip", zap.Error(err))
	}

	if blockNumber < uint64(latestBlock) {
		return errors.New("Current tip is less than latest block. Please make sure your node is synced to tip.")
	}

	s.Logger.Sugar().Infow("Indexing from current to tip",
		zap.Uint64("currentTip", blockNumber),
		zap.Int64("latestBlock", latestBlock),
		zap.Uint64("difference", blockNumber-uint64(latestBlock)),
	)

	// Use an atomic variable to track the current tip
	currentTip := atomic.Uint64{}
	currentTip.Store(blockNumber)

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
			latestTip, err := s.EthereumClient.GetLatestSafeBlock(ctx)
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
	// Keep some metrics during the indexing process
	blocksProcessed := int64(0)
	runningAvg := float64(0)
	totalDurationMs := int64(0)

	//nolint:all
	lastBlockParsed := latestBlock

	s.Logger.Sugar().Infow("Starting indexing process", zap.Int64("latestBlock", latestBlock), zap.Uint64("currentTip", currentTip.Load()))

	for uint64(latestBlock) <= currentTip.Load() {
		if s.shouldShutdown.Load() {
			s.Logger.Sugar().Infow("Shutting down block processor")
			return nil
		}
		tip := currentTip.Load()
		blocksRemaining := tip - uint64(latestBlock)
		pctComplete := (float64(blocksProcessed) / float64(blocksRemaining)) * 100
		estTimeRemainingMs := runningAvg * float64(blocksRemaining)
		estTimeRemainingHours := float64(estTimeRemainingMs) / 1000 / 60 / 60

		startTime := time.Now()
		endBlock := int64(latestBlock + 100)
		if endBlock > int64(tip) {
			endBlock = int64(tip)
		}
		if err := s.Pipeline.RunForBlockBatch(ctx, uint64(latestBlock), uint64(endBlock), true); err != nil {
			s.Logger.Sugar().Errorw("Failed to run pipeline for block batch",
				zap.Error(err),
				zap.Uint64("startBlock", uint64(latestBlock)),
				zap.Int64("endBlock", endBlock),
			)
			return err
		}

		lastBlockParsed = int64(endBlock)
		delta := time.Since(startTime).Milliseconds()
		blocksProcessed += (endBlock - latestBlock)

		totalDurationMs += delta
		runningAvg = float64(totalDurationMs / blocksProcessed)

		s.Logger.Sugar().Infow("Progress",
			zap.String("percentComplete", fmt.Sprintf("%.2f", pctComplete)),
			zap.Uint64("blocksRemaining", blocksRemaining),
			zap.Float64("estimatedTimeRemaining (hrs)", estTimeRemainingHours),
			zap.Float64("avgBlockProcessTime (ms)", float64(runningAvg)),
			zap.Uint64("lastBlockParsed", uint64(lastBlockParsed)),
		)
		latestBlock = endBlock + 1
	}

	return nil
}
