package rewards

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	gethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/wealdtech/go-merkletree/v2"
	"gorm.io/gorm/clause"

	"text/template"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RewardsCalculator struct {
	logger       *zap.Logger
	grm          *gorm.DB
	blockStore   storage.BlockStore
	globalConfig *config.Config
}

func NewRewardsCalculator(
	cfg *config.Config,
	grm *gorm.DB,
	bs storage.BlockStore,
	l *zap.Logger,
) (*RewardsCalculator, error) {
	rc := &RewardsCalculator{
		logger:       l,
		grm:          grm,
		blockStore:   bs,
		globalConfig: cfg,
	}

	return rc, nil
}

// CalculateRewardsForSnapshotDate calculates the rewards for a given snapshot date.
//
// @param snapshotDate: The date for which to calculate rewards, formatted as "YYYY-MM-DD".
//
// If there is no previous DistributionRoot, the rewards are calculated from EigenLayer Genesis.
func (rc *RewardsCalculator) CalculateRewardsForSnapshotDate(snapshotDate string) error {
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
			rc.logger.Sugar().Infow("Rewards calculation already in progress for snapshot date", zap.String("snapshotDate", snapshotDate))
			return errors.New("rewards calculation already in progress for snapshot date")
		}
		rc.logger.Sugar().Infow("Rewards calculation failed for snapshot date", zap.String("snapshotDate", snapshotDate))
		return errors.New("rewards calculation failed for snapshot date")
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
	goldStagingTableName := getGoldTableNames(cutoffDate)[Table_7_GoldStaging]

	var maxSnapshotStr string
	query := fmt.Sprintf(`select to_char(max(snapshot), 'YYYY-MM-DD') as snapshot from %s`, goldStagingTableName)
	res := rc.grm.Raw(query).Scan(&maxSnapshotStr)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get max snapshot date", "error", res.Error)
		return "", res.Error
	}
	return maxSnapshotStr, nil
}

type Reward struct {
	Earner           string
	Token            string
	Snapshot         string
	CumulativeAmount string
}

func (rc *RewardsCalculator) fetchRewardsForSnapshot(snapshotDate string) ([]*Reward, error) {
	var goldRows []*Reward
	query, err := renderQueryTemplate(`
		select
			earner,
			token,
			max(snapshot) as snapshot,
			cast(sum(amount) as varchar) as cumulative_amount
		from gold_table
		where snapshot <= date '{{.snapshotDate}}'
		group by 1, 2
		order by snapshot desc
    `, map[string]string{"snapshotDate": snapshotDate})

	if err != nil {
		return nil, err
	}
	res := rc.grm.Debug().Raw(query).Scan(&goldRows)
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

	if err := rc.GenerateGold7StagingTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate gold staging", "error", err)
		return err
	}

	if err := rc.GenerateGold8FinalTable(snapshotDate); err != nil {
		rc.logger.Sugar().Errorw("Failed to generate final table", "error", err)
		return err
	}

	return nil
}

func formatTableName(tableName string, snapshotDate string) string {
	return fmt.Sprintf("%s_%s", tableName, utils.SnakeCase(snapshotDate))
}

func (rc *RewardsCalculator) generateAndInsertFromQuery(
	tableName string,
	query string,
	variables map[string]interface{},
) error {
	tmpTableName := fmt.Sprintf("%s_tmp", tableName)

	queryWithInsert := fmt.Sprintf("CREATE TABLE %s AS %s", tmpTableName, query)

	_, err := helpers.WrapTxAndCommit(func(tx *gorm.DB) (interface{}, error) {
		queries := []string{
			queryWithInsert,
			fmt.Sprintf(`drop table if exists %s`, tableName),
			fmt.Sprintf(`alter table %s rename to %s`, tmpTableName, tableName),
		}
		for i, query := range queries {
			var res *gorm.DB
			if i == 0 && variables != nil {
				res = tx.Exec(query, variables)
			} else {
				res = tx.Exec(query)
			}
			if res.Error != nil {
				rc.logger.Sugar().Errorw("Failed to execute query", "query", query, "error", res.Error)
				return nil, res.Error
			}
		}
		return nil, nil
	}, rc.grm, nil)

	return err
}

var (
	Table_1_ActiveRewards         = "gold_1_active_rewards"
	Table_2_StakerRewardAmounts   = "gold_2_staker_reward_amounts"
	Table_3_OperatorRewardAmounts = "gold_3_operator_reward_amounts"
	Table_4_RewardsForAll         = "gold_4_rewards_for_all"
	Table_5_RfaeStakers           = "gold_5_rfae_stakers"
	Table_6_RfaeOperators         = "gold_6_rfae_operators"
	Table_7_GoldStaging           = "gold_7_staging"
	Table_8_GoldTable             = "gold_table"
)

var goldTableBaseNames = map[string]string{
	Table_1_ActiveRewards:         "gold_1_active_rewards",
	Table_2_StakerRewardAmounts:   "gold_2_staker_reward_amounts",
	Table_3_OperatorRewardAmounts: "gold_3_operator_reward_amounts",
	Table_4_RewardsForAll:         "gold_4_rewards_for_all",
	Table_5_RfaeStakers:           "gold_5_rfae_stakers",
	Table_6_RfaeOperators:         "gold_6_rfae_operators",
	Table_7_GoldStaging:           "gold_7_staging",
	Table_8_GoldTable:             "gold_table",
}

func getGoldTableNames(snapshotDate string) map[string]string {
	tableNames := make(map[string]string)
	for key, baseName := range goldTableBaseNames {
		tableNames[key] = formatTableName(baseName, snapshotDate)
	}
	return tableNames
}

func renderQueryTemplate(query string, variables map[string]string) (string, error) {
	queryTmpl := template.Must(template.New("").Parse(query))

	var dest bytes.Buffer
	if err := queryTmpl.Execute(&dest, variables); err != nil {
		return "", err
	}
	return dest.String(), nil
}
