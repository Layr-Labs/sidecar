package rewards

import (
	"errors"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/submittedDistributionRoots"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

type RewardsCalculator struct {
	logger        *zap.Logger
	bs            storage.BlockStore
	grm           *gorm.DB
	globalConfig  *config.Config
	calculationDB *gorm.DB
}

func NewRewardsCalculator(
	l *zap.Logger,
	bs storage.BlockStore,
	grm *gorm.DB,
	cfg *config.Config,
) (*RewardsCalculator, error) {
	inMemSqlite := sqlite.NewSqlite(sqlite.SqliteInMemoryPath, l)
	db, err := sqlite.NewGormSqliteFromSqlite(inMemSqlite)
	if err != nil {
		l.Sugar().Fatalw("Failed to create in memory sqlite", zap.Error(err))
		return nil, err
	}
	return &RewardsCalculator{
		logger:        l,
		bs:            bs,
		grm:           grm,
		globalConfig:  cfg,
		calculationDB: db,
	}, nil
}

// CalculateRewardsForSnapshot calculates the rewards for a given snapshot date.
//
// Rewards are calculated for the period between the last snapshot published on-chain
// via the DistributionRootSubmitted event and the desired snapshot date (exclusive).
//
// If there is no previous DistributionRoot, the rewards are calculated from EigenLayer Genesis.
func (rc *RewardsCalculator) CalculateRewardsForSnapshot(desiredSnapshotDate uint64) error {
	// First make sure that the snapshot date is valid as provided.
	// The time should be at 00:00:00 UTC. and should be in the past.
	snapshotDate := time.Unix(int64(desiredSnapshotDate), 0).UTC()

	if !rc.isValidSnapshotDate(snapshotDate) {
		return errors.New(fmt.Sprintf("invalid snapshot date '%s'", snapshotDate.String()))
	}

	// Get the last snapshot date published on-chain.
	distributionRoot, err := rc.getMostRecentDistributionRoot()
	if err != nil {
		rc.logger.Error("Failed to get the most recent distribution root", zap.Error(err))
		return err
	}

	var lowerBoundBlockNumber uint64
	if distributionRoot != nil {
		lowerBoundBlockNumber = distributionRoot.BlockNumber
	} else {
		lowerBoundBlockNumber = rc.globalConfig.GetGenesisBlockNumber()
	}

	rc.logger.Sugar().Infow("Calculating rewards for snapshot date",
		zap.String("snapshot_date", snapshotDate.String()),
		zap.Uint64("lowerBoundBlockNumber", lowerBoundBlockNumber),
	)

	// Determine the period for which rewards need to be calculated.
	lowerBoundBlock, err := rc.bs.GetBlockByNumber(lowerBoundBlockNumber)
	if err != nil {
		rc.logger.Error("Failed to get the lower bound block", zap.Error(err))
		return err
	}

	// Calculate the rewards for the period.
	return rc.calculateRewards(lowerBoundBlock, snapshotDate)
}

func (rc *RewardsCalculator) isValidSnapshotDate(snapshotDate time.Time) bool {
	// Check if the snapshot date is in the past.
	// The snapshot date should be at 00:00:00 UTC.
	if snapshotDate.After(time.Now().UTC()) {
		rc.logger.Error("Snapshot date is in the future")
		return false
	}

	if snapshotDate.Hour() != 0 || snapshotDate.Minute() != 0 || snapshotDate.Second() != 0 {
		rc.logger.Error("Snapshot date is not at 00:00:00 UTC")
		return false
	}

	return true
}

func (rc *RewardsCalculator) getMostRecentDistributionRoot() (*submittedDistributionRoots.SubmittedDistributionRoots, error) {
	var distributionRoot *submittedDistributionRoots.SubmittedDistributionRoots
	res := rc.grm.Model(&submittedDistributionRoots.SubmittedDistributionRoots{}).Order("block_number desc").First(&distributionRoot)
	if res != nil {
		return nil, res.Error
	}
	return distributionRoot, nil
}

func (rc *RewardsCalculator) calculateRewards(lowerBoundBlock *storage.Block, snapshotDate time.Time) error {
	return nil
}
