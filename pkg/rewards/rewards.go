package rewards

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"sync/atomic"

	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	gethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/wealdtech/go-merkletree/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"slices"
	"strings"
)

type RewardsCalculator struct {
	logger       *zap.Logger
	grm          *gorm.DB
	blockStore   storage.BlockStore
	sog          *stakerOperators.StakerOperatorsGenerator
	globalConfig *config.Config

	isGenerating atomic.Bool
}

func NewRewardsCalculator(
	cfg *config.Config,
	grm *gorm.DB,
	bs storage.BlockStore,
	sog *stakerOperators.StakerOperatorsGenerator,
	l *zap.Logger,
) (*RewardsCalculator, error) {
	rc := &RewardsCalculator{
		logger:       l,
		grm:          grm,
		blockStore:   bs,
		sog:          sog,
		globalConfig: cfg,
	}

	return rc, nil
}

func (rc *RewardsCalculator) GetIsGenerating() bool {
	return rc.isGenerating.Load()
}

func (rc *RewardsCalculator) acquireGenerationLock() {
	rc.isGenerating.Store(true)
}

func (rc *RewardsCalculator) releaseGenerationLock() {
	rc.isGenerating.Store(false)
}

type ErrRewardsCalculationInProgress struct{}

func (e *ErrRewardsCalculationInProgress) Error() string {
	return "rewards calculation already in progress"
}

// CalculateRewardsForSnapshotDate calculates the rewards for a given snapshot date.
//
// @param snapshotDate: The date for which to calculate rewards, formatted as "YYYY-MM-DD".
//
// If there is no previous DistributionRoot, the rewards are calculated from EigenLayer Genesis.
func (rc *RewardsCalculator) calculateRewardsForSnapshotDate(snapshotDate string) error {
	if rc.GetIsGenerating() {
		err := &ErrRewardsCalculationInProgress{}
		rc.logger.Sugar().Infow(err.Error())
		return err
	}
	rc.acquireGenerationLock()
	rc.logger.Sugar().Infow("Acquired rewards generation lock", zap.String("snapshotDate", snapshotDate))
	defer rc.releaseGenerationLock()

	// First make sure that the snapshot date is valid as provided.
	// The time should be at 00:00:00 UTC. and should be in the past.
	snapshotDateTime, err := time.Parse(time.DateOnly, snapshotDate)
	if err != nil {
		return fmt.Errorf("invalid snapshot date format: %w", err)
	}

	if !rc.isValidSnapshotDate(snapshotDateTime) {
		return fmt.Errorf("invalid snapshot date '%s'", snapshotDate)
	}

	status, err := rc.GetRewardSnapshotStatus(snapshotDate)
	if err != nil {
		return err
	}
	if status != nil {
		if status.Status == storage.RewardSnapshotStatusCompleted.String() {
			rc.logger.Sugar().Infow("Rewards already calculated for snapshot date", zap.String("snapshotDate", snapshotDate))
			// since the rewards are already calculated, simply return nil
			return nil
		}
		if status.Status == storage.RewardSnapshotStatusProcessing.String() {
			msg := "Rewards calculation already in progress for snapshot date"
			rc.logger.Sugar().Errorw(msg, zap.String("snapshotDate", snapshotDate))
			return errors.New(msg)
		}
		if status.Status == storage.RewardSnapshotStatusFailed.String() {
			msg := "Snapshot was already calculated and previously failed"
			rc.logger.Sugar().Errorw(msg, zap.String("snapshotDate", snapshotDate))
			return errors.New(msg)
		}
		msg := "Rewards calculation failed for snapshot date - unknown status"
		rc.logger.Sugar().Errorw(msg, zap.String("snapshotDate", snapshotDate), zap.Any("status", status))
		return errors.New(msg)
	}

	latestBlock, err := rc.blockStore.GetLatestBlock()
	if err != nil {
		return err
	}
	if latestBlock == nil {
		return errors.New("no blocks found in blockStore")
	}

	// Check if the latest block is before the snapshot date.
	if latestBlock.BlockTime.Before(snapshotDateTime) {
		return fmt.Errorf("latest block is before the snapshot date")
	}

	rc.logger.Sugar().Infow("Calculating rewards for snapshot date",
		zap.String("snapshot_date", snapshotDate),
	)

	// Calculate the rewards for the period.
	return rc.calculateRewards(snapshotDate)
}

func (rc *RewardsCalculator) CalculateRewardsForSnapshotDate(snapshotDate string) error {
	err := rc.calculateRewardsForSnapshotDate(snapshotDate)
	return err
}

func (rc *RewardsCalculator) CalculateRewardsForLatestSnapshot() (string, error) {
	snapshotDate := GetSnapshotFromCurrentDateTime()

	return snapshotDate, rc.CalculateRewardsForSnapshotDate(snapshotDate)
}

func GetSnapshotFromCurrentDateTime() string {
	snapshotDateTime := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour)
	return snapshotDateTime.Format(time.DateOnly)
}

func (rc *RewardsCalculator) CreateRewardSnapshotStatus(snapshotDate string) (*storage.GeneratedRewardsSnapshots, error) {
	r := &storage.GeneratedRewardsSnapshots{
		SnapshotDate: snapshotDate,
		Status:       storage.RewardSnapshotStatusProcessing.String(),
	}

	res := rc.grm.Model(&storage.GeneratedRewardsSnapshots{}).Clauses(clause.Returning{}).Create(r)
	if res.Error != nil {
		return nil, res.Error
	}
	return r, nil
}

func (rc *RewardsCalculator) UpdateRewardSnapshotStatus(snapshotDate string, status storage.RewardSnapshotStatus) error {
	res := rc.grm.Model(&storage.GeneratedRewardsSnapshots{}).Where("snapshot_date = ?", snapshotDate).Update("status", status.String())
	return res.Error
}

func (rc *RewardsCalculator) GetRewardSnapshotStatus(snapshotDate string) (*storage.GeneratedRewardsSnapshots, error) {
	var r = &storage.GeneratedRewardsSnapshots{}
	res := rc.grm.Model(&storage.GeneratedRewardsSnapshots{}).Where("snapshot_date = ?", snapshotDate).First(&r)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return r, nil
}

func (rc *RewardsCalculator) MerkelizeRewardsForSnapshot(snapshotDate string) (*merkletree.MerkleTree, map[gethcommon.Address]*merkletree.MerkleTree, error) {
	rewards, err := rc.fetchRewardsForSnapshot(snapshotDate)
	if err != nil {
		return nil, nil, err
	}

	distro := distribution.NewDistribution()

	earnerLines := make([]*distribution.EarnerLine, 0)
	for _, r := range rewards {
		earnerLines = append(earnerLines, &distribution.EarnerLine{
			Earner:           r.Earner,
			Token:            r.Token,
			CumulativeAmount: r.CumulativeAmount,
		})
	}

	rc.logger.Sugar().Infow("Loaded earner lines", "count", len(earnerLines))

	if err := distro.LoadLines(earnerLines); err != nil {
		rc.logger.Error("Failed to load lines", zap.Error(err))
		return nil, nil, err
	}

	accountTree, tokenTree, err := distro.Merklize()

	return accountTree, tokenTree, err
}

func (rc *RewardsCalculator) GetMaxSnapshotDateForCutoffDate(cutoffDate string) (string, error) {
	goldStagingTableName := rewardsUtils.GetGoldTableNames(cutoffDate)[rewardsUtils.Table_11_GoldStaging]

	var maxSnapshotStr string
	query := fmt.Sprintf(`select to_char(max(snapshot), 'YYYY-MM-DD') as snapshot from %s`, goldStagingTableName)
	res := rc.grm.Raw(query).Scan(&maxSnapshotStr)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get max snapshot date", "error", res.Error)
		return "", res.Error
	}
	return maxSnapshotStr, nil
}

func (rc *RewardsCalculator) BackfillAllStakerOperators() error {
	var generatedSnapshots []storage.GeneratedRewardsSnapshots
	query := `select * from generated_rewards_snapshots where status = 'complete' order by snapshot_date asc`
	res := rc.grm.Raw(query).Scan(&generatedSnapshots)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get generated snapshots", "error", res.Error)
		return res.Error
	}

	// First acquire a lock. If we cant, return and let the caller retry.
	rc.logger.Sugar().Infow("Acquiring rewards generation lock for staker operator backfill")
	if rc.GetIsGenerating() {
		err := &ErrRewardsCalculationInProgress{}
		rc.logger.Sugar().Infow(err.Error())
		return err
	}
	rc.acquireGenerationLock()
	defer rc.releaseGenerationLock()

	// take the largest snapshot date and generate the snapshot tables, which will be all-inclusive
	latestSnapshotDate := generatedSnapshots[len(generatedSnapshots)-1].SnapshotDate

	rc.logger.Sugar().Infow("Generating snapshot data for backfill", "snapshotDate", latestSnapshotDate)
	if err := rc.generateSnapshotData(latestSnapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate snapshot data", "error", err)
		return err
	}

	// iterate over each snapshot and generate the staker operators table data for each
	for _, snapshot := range generatedSnapshots {
		rc.logger.Sugar().Infow("Generating staker operators table for snapshot", "snapshotDate", snapshot.SnapshotDate)
		if err := rc.sog.GenerateStakerOperatorsTable(snapshot.SnapshotDate); err != nil {
			rc.logger.Sugar().Errorw("Failed to generate staker operators table", "error", err)
			return err
		}
	}
	return nil
}

// GenerateStakerOperatorsTableForPastSnapshot generates the staker operators table for a past snapshot date, OR
// generates the rewards and the related staker-operator table data if the snapshot is greater than the latest snapshot.
func (rc *RewardsCalculator) GenerateStakerOperatorsTableForPastSnapshot(cutoffDate string) error {
	// find the first snapshot that is >= to the provided cutoff date
	var generatedSnapshot storage.GeneratedRewardsSnapshots
	query := `select * from generated_rewards_snapshots where snapshot_date >= ? and status = 'complete' order by snapshot_date asc limit 1`
	res := rc.grm.Raw(query, cutoffDate).Scan(&generatedSnapshot)
	if res.Error != nil && errors.Is(res.Error, gorm.ErrRecordNotFound) {
		rc.logger.Sugar().Errorw("Failed to get generated snapshot", "error", res.Error)
		return res.Error
	}
	if res.RowsAffected == 0 || errors.Is(res.Error, gorm.ErrRecordNotFound) {
		rc.logger.Sugar().Infow("No snapshot found for cutoff date, rewards need to be calculated", "cutoffDate", cutoffDate)
		return rc.CalculateRewardsForSnapshotDate(cutoffDate)
	}

	// since rewards are already calculated and the corresponding tables are tied to the snapshot date,
	// we need to use the snapshot date from the generated snapshot to generate the staker operators table.
	//
	// Since this date is larger, and the insert into the staker-operators table discards duplicates,
	// this should be safe to do.
	cutoffDate = generatedSnapshot.SnapshotDate

	// Since this was a previous calculation, we have the date-suffixed gold tables, but not necessarily the snapshot tables.
	// In order for our calculations to work, we need to generate the snapshot tables for the cutoff date.
	//
	// First check to see if there is already a rewards generation in progress. If there is, return an error and let the caller try again.
	if rc.GetIsGenerating() {
		err := &ErrRewardsCalculationInProgress{}
		rc.logger.Sugar().Infow(err.Error())
		return err
	}

	// Acquire the generation lock and proceed with generating snapshot tables and then the staker operators table.
	rc.acquireGenerationLock()
	defer rc.releaseGenerationLock()

	rc.logger.Sugar().Infow("Acquired rewards generation lock", "cutoffDate", cutoffDate)

	if err := rc.generateSnapshotData(cutoffDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate snapshot data", "error", err)
		return err
	}

	if err := rc.sog.GenerateStakerOperatorsTable(cutoffDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker operators table", "error", err)
		return err
	}
	return nil
}

func (rc *RewardsCalculator) findGeneratedRewardSnapshotByBlock(blockHeight uint64) (*storage.GeneratedRewardsSnapshots, error) {
	distributionRootsQuery := `
		select
			block_number,
			arguments #>> '{2, Value}' as rewards_calculation_end_timestamp
		from transaction_logs
		where
			address = @rewardsCoordinatorAddress
			and event_name = 'DistributionRootSubmitted'
			and block_number >= @blockHeight
		order by block_number asc
	`
	type DistributionRoot struct {
		BlockNumber                    uint64
		RewardsCalculationEndTimestamp int64
	}

	rewardsCoordinatorAddress := rc.globalConfig.GetContractsMapForChain().RewardsCoordinator
	rows := make([]DistributionRoot, 0)
	res := rc.grm.Raw(distributionRootsQuery,
		sql.Named("rewardsCoordinatorAddress", rewardsCoordinatorAddress),
		sql.Named("blockHeight", blockHeight),
	).Scan(&rows)
	if res.Error != nil {
		return nil, res.Error
	}

	if len(rows) == 0 {
		return nil, nil
	}

	firstRow := rows[0]
	snapshotDate := time.Unix(firstRow.RewardsCalculationEndTimestamp, 0).UTC().Add(time.Hour * 24).Format(time.DateOnly)

	var generatedRewardSnapshots storage.GeneratedRewardsSnapshots
	res = rc.grm.Model(&storage.GeneratedRewardsSnapshots{}).Where("snapshot_date = ?", snapshotDate).First(&generatedRewardSnapshots)
	if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return nil, res.Error
	}

	// nothing found
	if res.RowsAffected == 0 || errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	return &generatedRewardSnapshots, nil
}

func (rc *RewardsCalculator) findRewardsTablesBySnapshotDate(snapshotDate string) ([]string, error) {
	schemaName := rc.globalConfig.DatabaseConfig.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}
	snakeCaseSnapshotDate := strings.ReplaceAll(snapshotDate, "-", "_")
	var rewardsTables []string
	query := `select table_name from information_schema.tables where table_schema = @tableSchema and table_name like @tableNamePattern`
	res := rc.grm.Raw(query,
		sql.Named("tableSchema", schemaName),
		sql.Named("tableNamePattern", fmt.Sprintf("gold_%%%s", snakeCaseSnapshotDate)),
	).Scan(&rewardsTables)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get rewards tables", "error", res.Error)
		return nil, res.Error
	}
	return rewardsTables, nil
}

func (rc *RewardsCalculator) DeleteCorruptedRewardsFromBlockHeight(blockHeight uint64) error {
	generatedSnapshot, err := rc.findGeneratedRewardSnapshotByBlock(blockHeight)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to find generated snapshot", "error", err)
		return err
	}
	if generatedSnapshot == nil {
		rc.logger.Sugar().Infow("No generated snapshot found that are gte provided blockHeight", "blockHeight", blockHeight)
		return nil
	}

	// find all generated snapshots that are, or were created after, the generated snapshot
	var snapshotsToDelete []*storage.GeneratedRewardsSnapshots
	res := rc.grm.Model(&storage.GeneratedRewardsSnapshots{}).Where("id >= ?", generatedSnapshot.Id).Find(&snapshotsToDelete)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to find generated snapshots", "error", res.Error)
		return res.Error
	}

	// if the target snapshot is '2024-12-01', then we need to find the one that came before it to delete everything that came after
	var lowerBoundSnapshot *storage.GeneratedRewardsSnapshots
	res = rc.grm.Model(&storage.GeneratedRewardsSnapshots{}).Where("snapshot_date < ?", generatedSnapshot.SnapshotDate).Order("snapshot_date desc").First(&lowerBoundSnapshot)
	if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
		rc.logger.Sugar().Errorw("Failed to find lower bound snapshot", "error", res.Error)
		return res.Error
	}
	if res.RowsAffected == 0 || errors.Is(res.Error, gorm.ErrRecordNotFound) {
		lowerBoundSnapshot = nil
	}

	snapshotDates := make([]string, 0)
	for _, snapshot := range snapshotsToDelete {
		snapshotDates = append(snapshotDates, snapshot.SnapshotDate)
		tableNames, err := rc.findRewardsTablesBySnapshotDate(snapshot.SnapshotDate)
		if err != nil {
			rc.logger.Sugar().Errorw("Failed to find rewards tables", "error", err)
			return err
		}
		// drop tables
		for _, tableName := range tableNames {
			rc.logger.Sugar().Infow("Dropping rewards table", "tableName", tableName)
			dropQuery := fmt.Sprintf(`drop table %s`, tableName)
			res := rc.grm.Exec(dropQuery)
			if res.Error != nil {
				rc.logger.Sugar().Errorw("Failed to drop rewards table", "error", res.Error)
				return res.Error
			}
		}

		// delete from generated_rewards_snapshots
		res = rc.grm.Delete(&storage.GeneratedRewardsSnapshots{}, snapshot.Id)
		if res.Error != nil {
			rc.logger.Sugar().Errorw("Failed to delete generated snapshot", "error", res.Error)
			return res.Error
		}
	}

	// sort all snapshot dates in ascending order to purge from gold table
	slices.SortFunc(snapshotDates, func(i, j string) int {
		return strings.Compare(i, j)
	})

	// purge from gold table
	if lowerBoundSnapshot != nil {
		rc.logger.Sugar().Infow("Purging rewards from gold table where snapshot >=", "snapshotDate", lowerBoundSnapshot.SnapshotDate)
		res = rc.grm.Exec(`delete from gold_table where snapshot >= @snapshotDate`, sql.Named("snapshotDate", lowerBoundSnapshot.SnapshotDate))
	} else {
		// if the lower bound is nil, ther we're deleting everything
		rc.logger.Sugar().Infow("Purging all rewards from gold table")
		res = rc.grm.Exec(`delete from gold_table`)
	}

	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to delete rewards from gold table", "error", res.Error)
		return res.Error
	}
	if lowerBoundSnapshot != nil {
		rc.logger.Sugar().Infow("Deleted rewards from gold table",
			zap.String("snapshotDate", lowerBoundSnapshot.SnapshotDate),
			zap.Int64("recordsDeleted", res.RowsAffected),
		)
	} else {
		rc.logger.Sugar().Infow("Deleted rewards from gold table",
			zap.Int64("recordsDeleted", res.RowsAffected),
		)
	}
	return nil
}

type Reward struct {
	Earner           string
	Token            string
	Snapshot         string
	CumulativeAmount string
}

func (rc *RewardsCalculator) fetchRewardsForSnapshot(snapshotDate string) ([]*Reward, error) {
	var goldRows []*Reward
	query, err := rewardsUtils.RenderQueryTemplate(`
		select
			earner,
			token,
			max(snapshot) as snapshot,
			cast(sum(amount) as varchar) as cumulative_amount
		from gold_table
		where snapshot <= date '{{.snapshotDate}}'
		group by 1, 2
		order by snapshot desc
    `, map[string]interface{}{"snapshotDate": snapshotDate})

	if err != nil {
		return nil, err
	}
	res := rc.grm.Raw(query).Scan(&goldRows)
	if res.Error != nil {
		return nil, res.Error
	}
	return goldRows, nil
}

func (rc *RewardsCalculator) calculateRewards(snapshotDate string) error {
	_, err := rc.CreateRewardSnapshotStatus(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to create reward snapshot status", "error", err)
		return err
	}

	if err = rc.generateSnapshotData(snapshotDate); err != nil {
		_ = rc.UpdateRewardSnapshotStatus(snapshotDate, storage.RewardSnapshotStatusFailed)
		rc.logger.Sugar().Errorw("Failed to generate snapshot data", "error", err)
		return err
	}

	if err = rc.generateGoldTables(snapshotDate); err != nil {
		_ = rc.UpdateRewardSnapshotStatus(snapshotDate, storage.RewardSnapshotStatusFailed)
		rc.logger.Sugar().Errorw("Failed to generate gold tables", "error", err)
		return err
	}

	if err = rc.sog.GenerateStakerOperatorsTable(snapshotDate); err != nil {
		_ = rc.UpdateRewardSnapshotStatus(snapshotDate, storage.RewardSnapshotStatusFailed)
		rc.logger.Sugar().Errorw("Failed to generate staker operators table", "error", err)
		return err
	}

	if err = rc.UpdateRewardSnapshotStatus(snapshotDate, storage.RewardSnapshotStatusCompleted); err != nil {
		rc.logger.Sugar().Errorw("Failed to update reward snapshot status", "error", err)
		return err
	}

	return nil
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

func (rc *RewardsCalculator) generateSnapshotData(snapshotDate string) error {
	var err error

	if err = rc.GenerateAndInsertCombinedRewards(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate combined rewards", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated combined rewards")

	if err = rc.GenerateAndInsertStakerShares(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker shares", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated staker shares")

	if err = rc.GenerateAndInsertOperatorShares(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator shares", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator shares")

	if err = rc.GenerateAndInsertOperatorAvsRegistrationSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator AVS registration snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator AVS registration snapshots")

	if err = rc.GenerateAndInsertOperatorAvsStrategySnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator AVS strategy snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator AVS strategy snapshots")

	if err = rc.GenerateAndInsertOperatorShareSnapshots(snapshotDate); err != nil {
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

	// ------------------------------------------------------------------------
	// Rewards V2 snapshots
	// ------------------------------------------------------------------------
	if err = rc.GenerateAndInsertOperatorDirectedRewards(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator directed rewards", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator directed rewards")
	if err = rc.GenerateAndInsertOperatorAvsSplitSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator avs split snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator avs split snapshots")

	if err = rc.GenerateAndInsertOperatorPISplitSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator pi snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator pi snapshots")

	return nil
}

func (rc *RewardsCalculator) generateGoldTables(snapshotDate string) error {
	forks, err := rc.globalConfig.GetForkDates()
	if err != nil {
		return err
	}
	if err := rc.Generate1ActiveRewards(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate active rewards", "error", err)
		return err
	}

	if err := rc.GenerateGold2StakerRewardAmountsTable(snapshotDate, forks); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold3OperatorRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold4RewardsForAllTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate rewards for all", "error", err)
		return err
	}

	if err := rc.GenerateGold5RfaeStakersTable(snapshotDate, forks); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate RFAE stakers", "error", err)
		return err
	}

	if err := rc.GenerateGold6RfaeOperatorsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate RFAE operators", "error", err)
		return err
	}

	if err := rc.Generate7ActiveODRewards(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate active od rewards", "error", err)
		return err
	}

	if err := rc.GenerateGold8OperatorODRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator od reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold9StakerODRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker od reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold10AvsODRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate avs od reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold11StagingTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate gold staging", "error", err)
		return err
	}

	if err := rc.GenerateGold12FinalTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate final table", "error", err)
		return err
	}

	return nil
}

func (rc *RewardsCalculator) generateAndInsertFromQuery(
	tableName string,
	query string,
	variables []interface{},
) error {
	return rewardsUtils.GenerateAndInsertFromQuery(
		rc.grm,
		tableName,
		query,
		variables,
		rc.logger,
	)
}
