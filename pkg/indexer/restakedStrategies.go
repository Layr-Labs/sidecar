package indexer

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"strings"
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
		if err := idx.ProcessRestakedStrategiesForBlockAndAvsDirectory(ctx, block, avsDirectoryAddress); err != nil {
			idx.Logger.Sugar().Errorw("Failed to process restaked strategies", zap.Error(err))
			return err
		}
	}
	return nil
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

	return idx.getAndInsertRestakedStrategies(ctx, avsOperators, avsDirectoryAddress, block)
}

func (idx *Indexer) getAndInsertRestakedStrategies(
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

	results, err := idx.ContractCaller.GetAllOperatorRestakedStrategies(ctx, pairs, blockNumber)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to get operator restaked strategies",
			zap.Error(err),
			zap.String("avsDirectoryAddress", avsDirectoryAddress),
			zap.Uint64("blockNumber", blockNumber),
		)
		return err
	}

	idx.Logger.Sugar().Infow("Got operator restaked strategies",
		zap.Int("count", len(results)),
		zap.Uint64("blockNumber", blockNumber),
	)
	strategiesToInsert := make([]*storage.OperatorRestakedStrategies, 0)
	for _, result := range results {
		avs := result.Avs
		operator := result.Operator

		for _, restakedStrategy := range result.Results {
			strategiesToInsert = append(strategiesToInsert, &storage.OperatorRestakedStrategies{
				AvsDirectoryAddress: strings.ToLower(avsDirectoryAddress),
				BlockNumber:         blockNumber,
				BlockTime:           block.BlockTime,
				Operator:            strings.ToLower(operator),
				Avs:                 strings.ToLower(avs),
				Strategy:            restakedStrategy.String(),
			})
		}
	}
	inserted, err := idx.MetadataStore.BulkInsertOperatorRestakedStrategies(strategiesToInsert)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to save restaked strategies",
			zap.Error(err),
			zap.String("avsDirectoryAddress", avsDirectoryAddress),
			zap.Uint64("blockNumber", blockNumber),
		)
		return err
	}
	idx.Logger.Sugar().Infow("Inserted restaked strategies",
		zap.Int("insertedCount", len(inserted)),
		zap.Int("inputCount", len(strategiesToInsert)),
	)
	return nil
}

func (idx *Indexer) ReprocessAllOperatorRestakedStrategies(ctx context.Context) error {
	idx.Logger.Sugar().Info("Reprocessing all operator restaked strategies")

	var endBlockNumber uint64
	query := `select max(eth_block_number) from state_roots`
	res := idx.db.Raw(query).Scan(&endBlockNumber)
	if res.Error != nil {
		idx.Logger.Sugar().Errorw("Failed to get max block number", zap.Error(res.Error))
		return res.Error
	}

	currentBlock := idx.Config.GetOperatorRestakedStrategiesStartBlock()

	for currentBlock <= endBlockNumber {
		if currentBlock%3600 == 0 {
			if err := idx.ProcessRestakedStrategiesForBlock(ctx, currentBlock); err != nil {
				idx.Logger.Sugar().Errorw("Failed to process restaked strategies", zap.Error(err))
				return err
			}
		}
		currentBlock++
	}
	return nil
}
