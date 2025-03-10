package backfiller

import (
	"context"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"go.uber.org/zap"
)

type Backfiller struct {
	EthClient *ethereum.Client
	Logger    *zap.Logger
	Config    *config.Config
}

func NewBackfiller(ethClient *ethereum.Client, cfg *config.Config, l *zap.Logger) *Backfiller {
	return &Backfiller{
		EthClient: ethClient,
		Logger:    l,
		Config:    cfg,
	}
}

func (b *Backfiller) Backfill(ctx context.Context, blockNumber uint64) ([]*ethereum.EthereumTransactionReceipt, error) {
	receipts, err := b.EthClient.GetBlockReceipts(ctx, blockNumber)
	if err != nil {
		b.Logger.Sugar().Errorw("failed to get block receipts", zap.Error(err))
		return nil, err
	}

	for _, receipt := range receipts {
		b.Logger.Sugar().Debugf("Processing receipt %s", receipt.TransactionHash)
	}

	return receipts, nil
}
