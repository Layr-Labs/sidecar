package sidecar

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/pipeline"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/runtime"
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
	MetaStateManager       *metaStateManager.MetaStateManager
	RewardsCalculator      *rewards.RewardsCalculator
	RewardsCalculatorQueue *rewardsCalculatorQueue.RewardsCalculatorQueue
	RewardProofs           *proofs.RewardsProofsStore
	ShutdownChan           chan bool
	shouldShutdown         *atomic.Bool
	sidecarRuntime         *runtime.SidecarRuntime
}

func NewSidecar(
	cfg *SidecarConfig,
	gCfg *config.Config,
	s storage.BlockStore,
	p *pipeline.Pipeline,
	em *stateManager.EigenStateManager,
	msm *metaStateManager.MetaStateManager,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	rp *proofs.RewardsProofsStore,
	l *zap.Logger,
	ethClient *ethereum.Client,
) *Sidecar {
	shouldShutdown := &atomic.Bool{}
	shouldShutdown.Store(false)

	rt := runtime.NewSidecarRuntime(em.DB, gCfg, l)

	return &Sidecar{
		Logger:                 l,
		Config:                 cfg,
		GlobalConfig:           gCfg,
		Storage:                s,
		Pipeline:               p,
		EthereumClient:         ethClient,
		RewardsCalculator:      rc,
		RewardsCalculatorQueue: rcq,
		RewardProofs:           rp,
		MetaStateManager:       msm,
		StateManager:           em,
		ShutdownChan:           make(chan bool),
		shouldShutdown:         shouldShutdown,
		sidecarRuntime:         rt,
	}
}

func (s *Sidecar) Start(ctx context.Context) {
	s.Logger.Info("Starting sidecar")

	if err := s.sidecarRuntime.ValidateAndUpdateSidecarVersion(version.GetVersion()); err != nil {
		s.Logger.Sugar().Fatalw("Failed to validate and update sidecar version", zap.Error(err))
	}

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
