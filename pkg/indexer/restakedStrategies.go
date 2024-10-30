package indexer

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/pkg/contractCaller"
	"github.com/Layr-Labs/go-sidecar/pkg/postgres"
	"github.com/Layr-Labs/go-sidecar/pkg/storage"
	"sync"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"go.uber.org/zap"
)

func (idx *Indexer) ProcessRestakedStrategiesForBlock(ctx context.Context, blockNumber uint64) error {
	idx.Logger.Sugar().Info(fmt.Sprintf("Processing restaked strategies for block: %v", blockNumber))

	block, err := idx.MetadataStore.GetBlockByNumber(blockNumber)
	if err != nil {
		idx.Logger.Sugar().Errorw(fmt.Sprintf("Failed to fetch block: %v", blockNumber), zap.Error(err))
		return err
	}
	if block == nil {
		idx.Logger.Sugar().Errorw(fmt.Sprintf("Block not found: %v", blockNumber))
		return nil
	}

	addresses := make([]string, 0)

	if idx.Config.Chain == config.Chain_Preprod || idx.Config.Chain == config.Chain_Holesky {
		addresses = append(addresses, config.AVSDirectoryAddresses[config.Chain_Preprod])
		addresses = append(addresses, config.AVSDirectoryAddresses[config.Chain_Holesky])
	} else {
		addresses = append(addresses, config.AVSDirectoryAddresses[config.Chain_Mainnet])
	}

	for _, avsDirectoryAddress := range addresses {
		if err := idx.ProcessRestakedStrategiesForBlockAndAvsDirectoryMulticall(ctx, block, avsDirectoryAddress); err != nil {
			idx.Logger.Sugar().Errorw("Failed to process restaked strategies", zap.Error(err))
			return err
		}
	}
	return nil
}

func (idx *Indexer) getRestakedStrategiesForAvsOperator(
	ctx context.Context,
	avsDirectoryAddress string,
	avsOperator *storage.ActiveAvsOperator,
	block *storage.Block,
) error {
	blockNumber := block.Number
	operator := avsOperator.Operator
	avs := avsOperator.Avs

	idx.Logger.Sugar().Infow("Fetching restaked strategies for operator",
		zap.String("operator", operator),
		zap.String("avs", avs),
		zap.String("avsDirectoryAddress", avsDirectoryAddress),
		zap.Uint64("blockNumber", blockNumber),
	)
	restakedStrategies, err := idx.ContractCaller.GetOperatorRestakedStrategies(ctx, avs, operator, blockNumber)

	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to get operator restaked strategies",
			zap.Error(err),
			zap.String("operator", operator),
			zap.String("avs", avs),
			zap.String("avsDirectoryAddress", avsDirectoryAddress),
			zap.Uint64("blockNumber", blockNumber),
		)
		return err
	}
	idx.Logger.Sugar().Infow("Fetched restaked strategies for operator",
		zap.Error(err),
		zap.String("operator", operator),
		zap.String("avs", avs),
		zap.String("avsDirectoryAddress", avsDirectoryAddress),
		zap.Uint64("blockNumber", blockNumber),
	)

	for _, restakedStrategy := range restakedStrategies {
		_, err := idx.MetadataStore.InsertOperatorRestakedStrategies(avsDirectoryAddress, blockNumber, block.BlockTime, operator, avs, restakedStrategy.String())

		if err != nil && !postgres.IsDuplicateKeyError(err) {
			idx.Logger.Sugar().Errorw("Failed to save restaked strategy",
				zap.Error(err),
				zap.String("restakedStrategy", restakedStrategy.String()),
				zap.String("operator", operator),
				zap.String("avs", avs),
				zap.String("avsDirectoryAddress", avsDirectoryAddress),
				zap.Uint64("blockNumber", blockNumber),
			)
			return err
		} else if err == nil {
			idx.Logger.Sugar().Infow("Inserted restaked strategy",
				zap.String("restakedStrategy", restakedStrategy.String()),
				zap.String("operator", operator),
				zap.String("avs", avs),
				zap.String("avsDirectoryAddress", avsDirectoryAddress),
				zap.Uint64("blockNumber", blockNumber),
			)
		}
	}
	return nil
}

func (idx *Indexer) getRestakedStrategiesWorker(
	ctx context.Context,
	jobs <-chan *storage.ActiveAvsOperator,
	avsDirectoryAddress string,
	block *storage.Block,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for avsOperator := range jobs {
		err := idx.getRestakedStrategiesForAvsOperator(ctx, avsDirectoryAddress, avsOperator, block)
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to get restaked strategies for operator",
				zap.Error(err),
				zap.String("operator", avsOperator.Operator),
				zap.String("avs", avsOperator.Avs),
				zap.String("avsDirectoryAddress", avsDirectoryAddress),
				zap.Uint64("blockNumber", block.Number),
			)
		}
	}
}

func (idx *Indexer) getAndInsertRestakedStrategiesWithMulticall(
	ctx context.Context,
	avsOperators []*storage.ActiveAvsOperator,
	avsDirectoryAddress string,
	block *storage.Block,
) error {
	blockNumber := block.Number
	pairs := make([]*contractCaller.OperatorRestakedStrategy, 0)
	for _, avsOperator := range avsOperators {
		if avsOperator == nil || avsOperator.Operator == "" || avsOperator.Avs == "" {
			return fmt.Errorf("Invalid AVS operator - %v", avsOperator)
		}
		pairs = append(pairs, &contractCaller.OperatorRestakedStrategy{
			Operator: avsOperator.Operator,
			Avs:      avsOperator.Avs,
		})
	}

	results, err := idx.ContractCaller.GetOperatorRestakedStrategiesMulticall(ctx, pairs, blockNumber)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to get operator restaked strategies",
			zap.Error(err),
			zap.String("avsDirectoryAddress", avsDirectoryAddress),
			zap.Uint64("blockNumber", blockNumber),
		)
		return err
	}

	for _, result := range results {
		avs := result.Avs
		operator := result.Operator
		for _, restakedStrategy := range result.Results {
			_, err := idx.MetadataStore.InsertOperatorRestakedStrategies(avsDirectoryAddress, blockNumber, block.BlockTime, operator, avs, restakedStrategy.String())

			if err != nil && !postgres.IsDuplicateKeyError(err) {
				idx.Logger.Sugar().Errorw("Failed to save restaked strategy",
					zap.Error(err),
					zap.String("restakedStrategy", restakedStrategy.String()),
					zap.String("operator", operator),
					zap.String("avs", avs),
					zap.String("avsDirectoryAddress", avsDirectoryAddress),
					zap.Uint64("blockNumber", blockNumber),
				)
				return err
			} else if err == nil {
				idx.Logger.Sugar().Infow("Inserted restaked strategy",
					zap.String("restakedStrategy", restakedStrategy.String()),
					zap.String("operator", operator),
					zap.String("avs", avs),
					zap.String("avsDirectoryAddress", avsDirectoryAddress),
					zap.Uint64("blockNumber", blockNumber),
				)
			}
		}
	}
	return nil
}

func (idx *Indexer) ProcessRestakedStrategiesForBlockAndAvsDirectoryMulticall(ctx context.Context, block *storage.Block, avsDirectoryAddress string) error {
	idx.Logger.Sugar().Infow("Using avs directory address", zap.String("avsDirectoryAddress", avsDirectoryAddress))

	blockNumber := block.Number

	avsOperators, err := idx.MetadataStore.GetLatestActiveAvsOperators(blockNumber, avsDirectoryAddress)
	if err != nil {
		idx.Logger.Sugar().Errorw(fmt.Sprintf("Failed to fetch avsOperators: %v", blockNumber), zap.Error(err))
		return err
	}

	idx.Logger.Sugar().Infow(fmt.Sprintf("Found %d active AVS operators", len(avsOperators)))

	return idx.getAndInsertRestakedStrategiesWithMulticall(ctx, avsOperators, avsDirectoryAddress, block)
}

func (idx *Indexer) ProcessRestakedStrategiesForBlockAndAvsDirectory(ctx context.Context, block *storage.Block, avsDirectoryAddress string) error {
	idx.Logger.Sugar().Infow("Using avs directory address", zap.String("avsDirectoryAddress", avsDirectoryAddress))

	blockNumber := block.Number

	avsOperators, err := idx.MetadataStore.GetLatestActiveAvsOperators(blockNumber, avsDirectoryAddress)
	if err != nil {
		idx.Logger.Sugar().Errorw(fmt.Sprintf("Failed to fetch avsOperators: %v", blockNumber), zap.Error(err))
		return err
	}

	idx.Logger.Sugar().Infow(fmt.Sprintf("Found %d active AVS operators", len(avsOperators)))

	wg := sync.WaitGroup{}
	jobs := make(chan *storage.ActiveAvsOperator, len(avsOperators))
	numWorkers := 20
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go idx.getRestakedStrategiesWorker(ctx, jobs, avsDirectoryAddress, block, &wg)
	}

	for _, avsOperator := range avsOperators {
		jobs <- avsOperator
	}
	close(jobs)
	wg.Wait()

	return nil
}
