package baseDataService

import (
	"context"
	"errors"
	"fmt"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"gorm.io/gorm"
)

type BaseDataService struct {
	DB *gorm.DB
}

func (b *BaseDataService) GetCurrentBlockHeightIfNotPresent(ctx context.Context, blockHeight uint64) (uint64, error) {
	if blockHeight == 0 {
		var currentBlock *storage.Block
		res := b.DB.Model(&storage.Block{}).Order("number desc").First(&currentBlock)
		if res.Error != nil {
			return 0, res.Error
		}
		blockHeight = currentBlock.Number
	}
	return blockHeight, nil
}

func (b *BaseDataService) GetBlock(ctx context.Context, blockHeight uint64) (*storage.Block, error) {
	var block *storage.Block
	res := b.DB.Model(&storage.Block{}).Where("number = ?", blockHeight).First(&block)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return block, nil
}

func (b *BaseDataService) GetLatestConfirmedBlock(ctx context.Context) (*storage.Block, error) {
	var stateRoot *stateManager.StateRoot
	res := b.DB.Model(&stateManager.StateRoot{}).Order("eth_block_number desc").First(&stateRoot)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("no state root found")
		}
		return nil, res.Error
	}

	return b.GetBlock(ctx, stateRoot.EthBlockNumber)
}

func (b *BaseDataService) GetBlockHeightForSnapshotDate(ctx context.Context, snapshotDate string) (uint64, error) {
	var block storage.Block
	res := b.DB.Model(&storage.Block{}).
		Where("to_char(block_time, 'YYYY-MM-DD') = ?", snapshotDate).
		Order("number ASC").
		First(&block)

	if res.Error != nil {
		return 0, res.Error
	}
	return block.Number, nil
}
