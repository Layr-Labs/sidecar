package rewards

import (
	"fmt"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/submittedDistributionRoots"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RewardsCalculator struct {
	logger       *zap.Logger
	grm          *gorm.DB
	globalConfig *config.Config
}

func NewRewardsCalculator(
	l *zap.Logger,
	grm *gorm.DB,
	cfg *config.Config,
) (*RewardsCalculator, error) {
	rc := &RewardsCalculator{
		logger:       l,
		grm:          grm,
		globalConfig: cfg,
	}

	if err := rc.initializeRewardsSchema(); err != nil {
		l.Sugar().Errorw("Failed to initialize rewards schema", zap.Error(err))
		return nil, err
	}
	return rc, nil
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
		return fmt.Errorf("invalid snapshot date '%s'", snapshotDate.String())
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

	// Calculate the rewards for the period.
	// TODO(seanmcgary): lower bound should be either 0 (i.e. 1970-01-01) or the snapshot of the previously calculated rewards.
	return rc.calculateRewards("", snapshotDate)
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

func (rc *RewardsCalculator) getMostRecentDistributionRoot() (*submittedDistributionRoots.SubmittedDistributionRoot, error) {
	var distributionRoot *submittedDistributionRoots.SubmittedDistributionRoot
	res := rc.grm.Model(&submittedDistributionRoots.SubmittedDistributionRoot{}).Order("block_number desc").First(&distributionRoot)
	if res != nil {
		return nil, res.Error
	}
	return distributionRoot, nil
}

func (rc *RewardsCalculator) initializeRewardsSchema() error {
	funcs := []func() error{
		rc.CreateOperatorAvsRegistrationSnapshotsTable,
		rc.CreateOperatorAvsStrategySnapshotsTable,
		rc.CreateOperatorSharesSnapshotsTable,
		rc.CreateStakerShareSnapshotsTable,
		rc.CreateStakerDelegationSnapshotsTable,
		rc.CreateCombinedRewardsTable,

		// Gold tables
		rc.CreateGold1ActiveRewardsTable,
		rc.CreateGold2RewardAmountsTable,
		rc.CreateGold3OperatorRewardsTable,
		rc.CreateGold4RewardsForAllTable,
		rc.CreateGold5RfaeStakersTable,
		rc.CreateGold6RfaeOperatorsTable,
		rc.CreateGold7StagingTable,
		rc.Create8GoldTable,
	}
	for _, f := range funcs {
		err := f()
		if err != nil {
			return err
		}
	}
	return nil
}

func (rc *RewardsCalculator) generateSnapshotData(startDate string, snapshotDate string) error {
	var err error

	if err = rc.GenerateAndInsertCombinedRewards(startDate, snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate combined rewards", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated combined rewards")

	if err = rc.GenerateAndInsertOperatorAvsRegistrationSnapshots(startDate, snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator AVS registration snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator AVS registration snapshots")

	if err = rc.GenerateAndInsertOperatorAvsStrategySnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator AVS strategy snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator AVS strategy snapshots")

	if err = rc.GenerateAndInsertOperatorShareSnapshots(startDate, snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator share snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator share snapshots")

	if err = rc.GenerateAndInsertStakerShareSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker share snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated staker share snapshots")

	if err = rc.GenerateAndInsertStakerDelegationSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker delegation snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated staker delegation snapshots")

	return nil
}

func (rc *RewardsCalculator) calculateRewards(previousSnapshotDate string, snapshotDate time.Time) error {

	return nil
}
