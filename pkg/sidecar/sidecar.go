package sidecar

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/pipeline"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"sync/atomic"
)

type SidecarConfig struct {
	GenesisBlockNumber uint64
}

type Sidecar struct {
	Logger                 *zap.Logger
	Config                 *SidecarConfig
	GlobalConfig           *config.Config
	Storage                storage.BlockStore
	Pipeline               *pipeline.Pipeline
	EthereumClient         *ethereum.Client
	StateManager           *stateManager.EigenStateManager
	RewardsCalculator      *rewards.RewardsCalculator
	RewardsCalculatorQueue *rewardsCalculatorQueue.RewardsCalculatorQueue
	ShutdownChan           chan bool
	shouldShutdown         *atomic.Bool
}

func NewSidecar(
	cfg *SidecarConfig,
	gCfg *config.Config,
	s storage.BlockStore,
	p *pipeline.Pipeline,
	em *stateManager.EigenStateManager,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	l *zap.Logger,
	ethClient *ethereum.Client,
) *Sidecar {
	shouldShutdown := &atomic.Bool{}
	shouldShutdown.Store(false)
	return &Sidecar{
		Logger:                 l,
		Config:                 cfg,
		GlobalConfig:           gCfg,
		Storage:                s,
		Pipeline:               p,
		EthereumClient:         ethClient,
		RewardsCalculator:      rc,
		RewardsCalculatorQueue: rcq,
		StateManager:           em,
		ShutdownChan:           make(chan bool),
		shouldShutdown:         shouldShutdown,
	}
}

func (s *Sidecar) Start(ctx context.Context) {
	s.Logger.Info("Starting sidecar")

	// Spin up a goroutine that listens on a channel for a shutdown signal.
	// When the signal is received, set shouldShutdown to true and return.
	go func() {
		for range s.ShutdownChan {
			s.Logger.Sugar().Infow("Received shutdown signal")
			s.shouldShutdown.Store(true)
		}
	}()

	s.StartIndexing(ctx)
	/*
		Main loop:

		- Get current indexed block
			- If no blocks, start from the genesis block
			- If some blocks, start from last indexed block
		- Once at tip, begin listening for new blocks
	*/
}
