package rewards

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/utils"

	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/metrics/metricsTypes"
	"github.com/Layr-Labs/sidecar/pkg/rewards/rewardsTypes"

	"sync/atomic"

	"slices"
	"strings"

	"strconv"

	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	serviceTypes "github.com/Layr-Labs/sidecar/pkg/service/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	gethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/wealdtech/go-merkletree/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type RewardsCalculator struct {
	logger       *zap.Logger
	grm          *gorm.DB
	blockStore   storage.BlockStore
	sog          *stakerOperators.StakerOperatorsGenerator
	globalConfig *config.Config
	metricsSink  *metrics.MetricsSink

	isGenerating atomic.Bool
}

func NewRewardsCalculator(
	cfg *config.Config,
	grm *gorm.DB,
	bs storage.BlockStore,
	sog *stakerOperators.StakerOperatorsGenerator,
	ms *metrics.MetricsSink,
	l *zap.Logger,
) (*RewardsCalculator, error) {
	rc := &RewardsCalculator{
		logger:       l,
		grm:          grm,
		blockStore:   bs,
		sog:          sog,
		globalConfig: cfg,
		metricsSink:  ms,
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
	startTime := time.Now()
	defer func() {
		_ = rc.metricsSink.Timing(metricsTypes.Metric_Timing_RewardsCalcDuration, time.Since(startTime), []metricsTypes.MetricsLabel{
			{Name: "snapshotDate", Value: snapshotDate},
		})
	}()
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
	rc.logger.Sugar().Infow("fetched reward snapshot status",
		zap.String("snapshotDate", snapshotDate),
		zap.Any("status", status),
	)
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
	var r *storage.GeneratedRewardsSnapshots
	query := `
		select
			*
		from generated_rewards_snapshots
		where
			snapshot_date >= @snapshotDate
		order by snapshot_date asc
		limit 1
	`

	res := rc.grm.Raw(query, sql.Named("snapshotDate", snapshotDate)).Scan(&r)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return r, nil
}

func (rc *RewardsCalculator) MerkelizeRewardsForSnapshot(snapshotDate string) (
	*merkletree.MerkleTree,
	map[gethcommon.Address]*merkletree.MerkleTree,
	*distribution.Distribution,
	error,
) {
	rewards, err := rc.FetchRewardsForSnapshot(snapshotDate, nil, nil, nil)
	if err != nil {
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}

	accountTree, tokenTree, err := distro.Merklize()

	return accountTree, tokenTree, distro, err
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

func (rc *RewardsCalculator) FetchRewardsForSnapshot(snapshotDate string, earners []string, tokens []string, pagination *serviceTypes.Pagination) ([]*rewardsTypes.Reward, error) {
	snapshotDateTime, err := time.Parse(time.DateOnly, snapshotDate)
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot date format: %w", err)
	}
	cutoffDate := snapshotDateTime.Add(time.Hour * 24).Format(time.DateOnly)

	var goldRows []*rewardsTypes.Reward
	query := `
		with all_combined_rewards as (
			select
				distinct(reward_hash) as reward_hash
			from (
				select reward_hash from combined_rewards where block_time <= TIMESTAMP '{{.cutoffDate}}'
				union all
				select reward_hash from operator_directed_rewards where block_time <= TIMESTAMP '{{.cutoffDate}}'
				union all
				select
					odosrs.reward_hash
				from operator_directed_operator_set_reward_submissions as odosrs
				-- operator_directed_operator_set_reward_submissions lacks a block_time column, so we need to join blocks
				join blocks as b on (b.number = odosrs.block_number)
				where
					b.block_time::timestamp(6) <= TIMESTAMP '{{.cutoffDate}}'
			) as t
		)
		select
			earner,
			token,
			max(snapshot) as snapshot,
			cast(sum(amount) as varchar) as cumulative_amount
		from gold_table
		where
		    snapshot <= date '{{.snapshotDate}}'
		    and reward_hash in (select reward_hash from all_combined_rewards)
		{{ if .filterEarners }}
			and earner in @earners
		{{ end }}
		{{ if .filterTokens }}
			and token in @tokens
		{{ end }}
		group by 1, 2
		order by snapshot desc
		{{ if .pagination }}
			limit @limit
			offset @offset
		{{ end }}
    `
	templateArgs := map[string]interface{}{
		"snapshotDate":  snapshotDate,
		"filterEarners": false,
		"filterTokens":  false,
		"cutoffDate":    cutoffDate,
		"pagination":    pagination != nil,
	}

	queryArgs := []interface{}{}
	if pagination != nil {
		queryArgs = append(queryArgs, sql.Named("limit", pagination.PageSize))
		queryArgs = append(queryArgs, sql.Named("offset", pagination.Page))
	}

	if len(earners) > 0 {
		templateArgs["filterEarners"] = true
		queryArgs = append(queryArgs, sql.Named("earners", lowercaseAddressList(earners)))
	}

	if len(tokens) > 0 {
		templateArgs["filterTokens"] = true
		queryArgs = append(queryArgs, sql.Named("tokens", lowercaseAddressList(tokens)))
	}

	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, templateArgs)

	if err != nil {
		return nil, err
	}
	res := rc.grm.Raw(renderedQuery, queryArgs...).Scan(&goldRows)
	if res.Error != nil {
		return nil, res.Error
	}
	return goldRows, nil
}

func lowercaseAddressList(addresses []string) []string {
	return utils.Map(addresses, func(token string, i uint64) string {
		return strings.ToLower(token)
	})
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

	if err = rc.GenerateAndInsertDefaultOperatorSplitSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate default operator split snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated default operator split snapshots")

	// ------------------------------------------------------------------------
	// Rewards V2.1 snapshots
	// ------------------------------------------------------------------------
	if err = rc.GenerateAndInsertOperatorDirectedOperatorSetRewards(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator directed operator set rewards", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator directed operator set rewards")

	if err = rc.GenerateAndInsertOperatorSetSplitSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator set split snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator set split snapshots")

	if err = rc.GenerateAndInsertOperatorSetOperatorRegistrationSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator set operator registration snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator set operator registration snapshots")

	if err = rc.GenerateAndInsertOperatorSetStrategyRegistrationSnapshots(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator set strategy registration snapshots", "error", err)
		return err
	}
	rc.logger.Sugar().Debugw("Generated operator set strategy registration snapshots")

	return nil
}

func (rc *RewardsCalculator) generateGoldTables(snapshotDate string) error {
	forks, err := rc.globalConfig.GetRewardsSqlForkDates()
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

	if err := rc.GenerateGold8OperatorODRewardAmountsTable(snapshotDate, forks); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator od reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold9StakerODRewardAmountsTable(snapshotDate, forks); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker od reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold10AvsODRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate avs od reward amounts", "error", err)
		return err
	}

	if err := rc.GenerateGold11ActiveODOperatorSetRewards(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate active od operator set rewards", "error", err)
		return err
	}

	if err := rc.GenerateGold12OperatorODOperatorSetRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator od operator set rewards", "error", err)
		return err
	}

	if err := rc.GenerateGold13StakerODOperatorSetRewardAmountsTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate staker od operator set rewards", "error", err)
		return err
	}

	if err := rc.GenerateGold14AvsODOperatorSetRewardAmountsTable(snapshotDate, forks); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate avs od operator set rewards", "error", err)
		return err
	}

	if err := rc.GenerateGold15StagingTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate gold staging", "error", err)
		return err
	}

	if err := rc.GenerateGold16FinalTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate final table", "error", err)
		return err
	}

	return nil
}

func (rc *RewardsCalculator) FindClaimableDistributionRoot(rootIndex int64) (*types.SubmittedDistributionRoot, error) {
	query := `
		select
			*
		from submitted_distribution_roots as sdr
		left join disabled_distribution_roots as ddr on (sdr.root_index = ddr.root_index)
		where
			ddr.root_index is null
		{{ if eq .rootIndex "-1" }}
			and activated_at <= now()
		{{ else }}
			and sdr.root_index = {{.rootIndex}}
		{{ end }}
		order by sdr.root_index desc
		limit 1
	`
	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"rootIndex": strconv.Itoa(int(rootIndex)),
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return nil, err
	}

	var submittedDistributionRoot *types.SubmittedDistributionRoot
	res := rc.grm.Raw(renderedQuery).Scan(&submittedDistributionRoot)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			rc.logger.Sugar().Errorw("No active distribution root found by root_index",
				zap.Int64("rootIndex", rootIndex),
				zap.Error(res.Error),
			)
			return nil, res.Error
		}
		rc.logger.Sugar().Errorw("Failed to find most recent claimable distribution root", "error", res.Error)
		return nil, res.Error
	}

	return submittedDistributionRoot, nil
}

func (rc *RewardsCalculator) GetGeneratedRewardsForSnapshotDate(snapshotDate string) (*storage.GeneratedRewardsSnapshots, error) {
	query, err := rewardsUtils.RenderQueryTemplate(`
		select
			*
		from generated_rewards_snapshots as grs
		where
			status = 'complete'
			and grs.snapshot_date::timestamp(6) >= '{{.snapshotDate}}'::timestamp(6)			
		order by grs.snapshot_date asc
		limit 1
	`, map[string]interface{}{"snapshotDate": snapshotDate})

	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return nil, err
	}

	var generatedRewardsSnapshot *storage.GeneratedRewardsSnapshots
	res := rc.grm.Raw(query).Scan(&generatedRewardsSnapshot)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get generated rewards snapshots", "error", res.Error)
		return nil, res.Error
	}
	return generatedRewardsSnapshot, nil
}

type DistributionRoot struct {
	types.SubmittedDistributionRoot
	Disabled bool
}

// ListDistributionRoots returns a list of submitted distribution roots. If a non-zero blockHeight is provided,
// DistributionRoots for only that blockHeight will be returned
func (rc *RewardsCalculator) ListDistributionRoots(blockHeight uint64) ([]*DistributionRoot, error) {
	query := `
		select
			sdr.*,
			case when ddr.root_index is not null then true else false end as disabled
		from submitted_distribution_roots as sdr
		left join disabled_distribution_roots as ddr on (sdr.root_index = ddr.root_index)
	`
	args := make([]interface{}, 0)
	if blockHeight > 0 {
		query += `
			where sdr.block_number = @blockHeight
		`
		args = append(args, sql.Named("blockHeight", blockHeight))
	}
	query += ` order by root_index desc`

	var submittedDistributionRoots []*DistributionRoot
	res := rc.grm.Raw(query, args...).Scan(&submittedDistributionRoots)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list submitted distribution roots", "error", res.Error)
		return nil, res.Error
	}
	return submittedDistributionRoots, nil
}

func (rc *RewardsCalculator) PurgeCorruptRewardsGeneration() error {
	query := `
		select
			*
		from generated_rewards_snapshots
		order by id desc
		limit 1
	`
	var lastGeneratedSnapshot *storage.GeneratedRewardsSnapshots
	res := rc.grm.Raw(query).Scan(&lastGeneratedSnapshot)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			rc.logger.Sugar().Infow("No generated rewards snapshots found")
			return nil
		}
		rc.logger.Sugar().Errorw("Failed to get last generated rewards snapshot", "error", res.Error)
		return res.Error
	}
	if lastGeneratedSnapshot == nil {
		rc.logger.Sugar().Infow("No generated rewards snapshots found")
		return nil
	}

	if lastGeneratedSnapshot.Status == storage.RewardSnapshotStatusCompleted.String() {
		rc.logger.Sugar().Infow("Last generated rewards snapshot is already completed, no need to purge")
		return nil
	}

	rc.logger.Sugar().Infow("found corrupt generated rewards snapshot, purging",
		zap.Any("generatedSnapshot", lastGeneratedSnapshot),
	)

	tableNames, err := rc.findRewardsTablesBySnapshotDate(lastGeneratedSnapshot.SnapshotDate)
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
	res = rc.grm.Delete(&storage.GeneratedRewardsSnapshots{}, lastGeneratedSnapshot.Id)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to delete generated snapshot", "error", res.Error)
		return res.Error
	}
	rc.logger.Sugar().Infow("purged corrupt generated rewards snapshot",
		zap.Any("generatedSnapshot", lastGeneratedSnapshot),
	)
	return nil
}
