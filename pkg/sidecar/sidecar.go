package sidecar

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/pipeline"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/startupJobs"
	_202505092016_fixRewardsClaimedTransactions "github.com/Layr-Labs/sidecar/pkg/startupJobs/202505092016_fixRewardsClaimedTransactions"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
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

	db *gorm.DB
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
	grm *gorm.DB,
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
		RewardProofs:           rp,
		MetaStateManager:       msm,
		StateManager:           em,
		ShutdownChan:           make(chan bool),
		shouldShutdown:         shouldShutdown,
		db:                     grm,
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

	if err := s.RunStartupJobs(ctx); err != nil {
		s.Logger.Sugar().Fatalw("Failed to run startup jobs", zap.Error(err))
	}

	s.StartIndexing(ctx)
	/*
		Main loop:

		- Get current indexed block
			- If no blocks, start from the genesis block
			- If some blocks, start from last indexed block
		- Once at tip, begin listening for new blocks
	*/
}

func (s *Sidecar) getStartupJob(name string) (*storage.StartupJob, error) {
	var job *storage.StartupJob
	res := s.db.Model(&storage.StartupJob{}).Where("name = ?", name).First(&job)
	if res.Error != nil {
		if res.Error == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, res.Error
	}
	return job, nil
}

func (s *Sidecar) RunStartupJobs(ctx context.Context) error {
	startupJobs := []IStartupJob{
		&_202505092016_fixRewardsClaimedTransactions.StartupJob{},
	}

	for _, job := range startupJobs {
		jobRecord, err := s.getStartupJob(job.Name())
		if err != nil {
			return fmt.Errorf("Failed to get startup job record: %w", err)
		}
		if jobRecord != nil {
			s.Logger.Sugar().Debugw("Job already run", zap.String("job", job.Name()))
			continue
		}

		s.Logger.Sugar().Infow("Running startup job", "job", job.Name())

		err = job.Run(ctx, s.GlobalConfig, s.EthereumClient, s.Pipeline.Indexer, s.Pipeline.Fetcher, s.db, s, s.Logger)
		if err != nil {
			return fmt.Errorf("Failed to run startup job: %w", err)
		}

		s.Logger.Sugar().Infow("Finished startup job", "job", job.Name())

		res := s.db.Model(&storage.StartupJob{}).Create(&storage.StartupJob{Name: job.Name()})
		if res.Error != nil {
			return fmt.Errorf("Failed to create startup job record: %w", res.Error)
		}
	}
	return nil
}

type IStartupJob interface {
	Run(
		ctx context.Context,
		cfg *config.Config,
		ethClient *ethereum.Client,
		indexer *indexer.Indexer,
		fetcher *fetcher.Fetcher,
		grm *gorm.DB,
		s startupJobs.ISidecar,
		logger *zap.Logger,
	) error
	Name() string
}
