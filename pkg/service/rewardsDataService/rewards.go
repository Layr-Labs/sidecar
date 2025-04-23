package rewardsDataService

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	eigenStateTypes "github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewards/rewardsTypes"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"github.com/Layr-Labs/sidecar/pkg/service/baseDataService"
	serviceTypes "github.com/Layr-Labs/sidecar/pkg/service/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	errors2 "github.com/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"reflect"
	"strings"
	"sync"
	"time"
)

type RewardsDataService struct {
	baseDataService.BaseDataService
	db                *gorm.DB
	logger            *zap.Logger
	globalConfig      *config.Config
	rewardsCalculator *rewards.RewardsCalculator
}

func NewRewardsDataService(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	rc *rewards.RewardsCalculator,
) *RewardsDataService {
	return &RewardsDataService{
		BaseDataService: baseDataService.BaseDataService{
			DB: db,
		},
		db:                db,
		logger:            logger,
		globalConfig:      globalConfig,
		rewardsCalculator: rc,
	}
}

func (rds *RewardsDataService) GetRewardsForSnapshot(ctx context.Context, snapshot string, earners []string) ([]*rewardsTypes.Reward, error) {
	return rds.rewardsCalculator.FetchRewardsForSnapshot(snapshot, earners)
}

func (rds *RewardsDataService) GetRewardsForDistributionRoot(ctx context.Context, rootIndex uint64) ([]*rewardsTypes.Reward, error) {
	root, err := rds.getDistributionRootByRootIndex(rootIndex)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("no distribution root found for root index '%d'", rootIndex)
	}
	return rds.rewardsCalculator.FetchRewardsForSnapshot(root.GetSnapshotDate(), nil)
}

type TotalClaimedReward struct {
	Earner string
	Token  string
	Amount string
}

func lowercaseTokenList(tokens []string) []string {
	return utils.Map(tokens, func(token string, i uint64) string {
		return strings.ToLower(token)
	})
}

func (rds *RewardsDataService) GetTotalClaimedRewards(ctx context.Context, earner string, tokens []string, blockHeight uint64) ([]*TotalClaimedReward, error) {
	blockHeight, err := rds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	if earner == "" {
		return nil, fmt.Errorf("earner is required")
	}
	earner = strings.ToLower(earner)
	tokens = lowercaseTokenList(tokens)

	query := `
		select
			earner,
			token,
			coalesce(sum(claimed_amount), 0) as amount
		from rewards_claimed as rc
		where
			earner = @earner
			and block_number <= @blockHeight
	`
	args := []interface{}{
		sql.Named("earner", earner),
		sql.Named("blockHeight", blockHeight),
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		formattedTokens := utils.Map(tokens, func(token string, i uint64) string {
			return strings.ToLower(token)
		})
		args = append(args, sql.Named("tokens", formattedTokens))
	}

	query += " group by earner, token"

	claimedAmounts := make([]*TotalClaimedReward, 0)
	res := rds.db.Raw(query, args...).Scan(&claimedAmounts)

	if res.Error != nil {
		return nil, res.Error
	}
	return claimedAmounts, nil
}

// ListClaimedRewardsByBlockRange returns a list of claimed rewards for a given earner within a block range.
//
// If earner is an empty string, all claimed rewards within the block range are returned.
func (rds *RewardsDataService) ListClaimedRewardsByBlockRange(
	ctx context.Context,
	earner string,
	startBlockHeight uint64,
	endBlockHeight uint64,
	tokens []string,
) ([]*types.RewardsClaimed, error) {
	if endBlockHeight == 0 {
		return nil, fmt.Errorf("endBlockHeight must be greater than 0")
	}
	if endBlockHeight < startBlockHeight {
		return nil, fmt.Errorf("endBlockHeight must be greater than or equal to startBlockHeight")
	}

	query := `
		select
		    rc.root,
			rc.earner,
			rc.claimer,
			rc.recipient,
			rc.token,
			rc.claimed_amount,
			rc.transaction_hash,
			rc.block_number,
			rc.log_index
		from rewards_claimed as rc
		where
			block_number >= @startBlockHeight
			and block_number <= @endBlockHeight
	`
	args := []interface{}{
		sql.Named("startBlockHeight", startBlockHeight),
		sql.Named("endBlockHeight", endBlockHeight),
	}
	if earner != "" {
		query += " and earner = @earner"
		args = append(args, sql.Named("earner", strings.ToLower(earner)))
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		tokens = lowercaseTokenList(tokens)
		args = append(args, sql.Named("tokens", tokens))
	}
	query += " order by block_number, log_index"

	claimedRewards := make([]*types.RewardsClaimed, 0)
	res := rds.db.Raw(query, args...).Scan(&claimedRewards)

	if res.Error != nil {
		return nil, res.Error
	}
	return claimedRewards, nil
}

type RewardAmount struct {
	Token  string
	Amount string
}

// GetTotalRewardsForEarner returns the total earned rewards for a given earner at a given block height.
func (rds *RewardsDataService) GetTotalRewardsForEarner(
	ctx context.Context,
	earner string,
	tokens []string,
	blockHeight uint64,
	claimable bool,
) ([]*RewardAmount, error) {
	if earner == "" {
		return nil, fmt.Errorf("earner is required")
	}
	earner = strings.ToLower(earner)

	snapshot, err := rds.findDistributionRootClosestToBlockHeight(blockHeight, claimable)
	if err != nil {
		return nil, err
	}

	if snapshot == nil {
		return nil, fmt.Errorf("no distribution root found for blockHeight '%d'", blockHeight)
	}

	query := `
		with token_snapshots as (
			select
				token,
				amount
			from gold_table as gt
			where
				earner = @earner
				and snapshot <= @snapshot
		)
		select
			token,
			coalesce(sum(amount), 0) as amount
		from token_snapshots
		group by 1
	`
	args := []interface{}{
		sql.Named("earner", earner),
		sql.Named("snapshot", snapshot.GetSnapshotDate()),
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		tokens = lowercaseTokenList(tokens)
		args = append(args, sql.Named("tokens", tokens))
	}

	rewardAmounts := make([]*RewardAmount, 0)
	res := rds.db.Raw(query, args...).Scan(&rewardAmounts)

	if res.Error != nil {
		return nil, res.Error
	}

	return rewardAmounts, nil
}

// GetClaimableRewardsForEarner returns the rewards that are claimable for a given earner at a given block height (totalActiveRewards - claimed)
func (rds *RewardsDataService) GetClaimableRewardsForEarner(
	ctx context.Context,
	earner string,
	tokens []string,
	blockHeight uint64,
) (
	[]*RewardAmount,
	*eigenStateTypes.SubmittedDistributionRoot,
	error,
) {
	if earner == "" {
		return nil, nil, fmt.Errorf("earner is required")
	}
	earner = strings.ToLower(earner)

	blockHeight, err := rds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, nil, err
	}

	snapshot, err := rds.findDistributionRootClosestToBlockHeight(blockHeight, true)
	if err != nil {
		return nil, nil, err
	}
	if snapshot == nil {
		return nil, nil, fmt.Errorf("no distribution root found for blockHeight '%d'", blockHeight)
	}
	snapshotDateTime, err := time.Parse(time.DateOnly, snapshot.GetSnapshotDate())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse snapshot date '%s' for block height '%d'", snapshot.GetSnapshotDate(), blockHeight)
	}
	cutoffDate := snapshotDateTime.Add(time.Hour * 24).Format(time.DateOnly)

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
		),
		earner_rewards as (
			select
				earner,
				token,
				sum(amount) as amount
			from gold_table
			where
				snapshot <= date '{{.snapshotDate}}'
				and reward_hash in (select reward_hash from all_combined_rewards)
				and earner = @earner
			group by 1, 2
		),
		claimed_tokens as (
			select
				earner,
				token,
				sum(claimed_amount) as amount
			from rewards_claimed as rc
			where
				earner = @earner
				and block_number <= @blockNumber
			group by 1, 2
		)
		select
			er.token,
			(coalesce(er.amount, 0) - coalesce(ct.amount, 0))::numeric as amount
		from earner_rewards as er
		left join claimed_tokens as ct on (
			ct.token = er.token
			and ct.earner = er.earner
		)
	`

	args := []interface{}{
		sql.Named("earner", earner),
		sql.Named("blockNumber", blockHeight),
		sql.Named("snapshot", snapshot.GetSnapshotDate()),
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		tokens = lowercaseTokenList(tokens)
		args = append(args, sql.Named("tokens", tokens))
	}

	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"cutoffDate":   cutoffDate,
		"snapshotDate": snapshotDateTime.Format(time.DateOnly),
	})
	if err != nil {
		rds.logger.Sugar().Errorw("failed to render query template",
			zap.Uint64("blockHeight", blockHeight),
			zap.Bool("claimable", true),
			zap.Error(err),
		)
		return nil, nil, err
	}

	claimableRewards := make([]*RewardAmount, 0)
	res := rds.db.Raw(renderedQuery, args...).Scan(&claimableRewards)
	if res.Error != nil {
		return nil, nil, res.Error
	}
	return claimableRewards, snapshot, nil
}

//nolint:unused
func (rds *RewardsDataService) findRewardsGenerationForSnapshotDate(snapshotDate string) (*storage.GeneratedRewardsSnapshots, error) {
	query := `
		select
			*
		from generated_rewards_snapshots
		where
			snapshot_date >= @snapshotDate
			and status = 'complete'
		order by snapshot_date asc
		limit 1
	`
	var generatedSnapshot *storage.GeneratedRewardsSnapshots
	res := rds.db.Raw(query, sql.Named("snapshotDate", snapshotDate)).Scan(&generatedSnapshot)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return generatedSnapshot, nil
}

// findDistributionRootClosestToBlockHeight returns the distribution root that is closest to the provided block height
// that is also not disabled.
func (rds *RewardsDataService) findDistributionRootClosestToBlockHeight(blockHeight uint64, claimable bool) (*eigenStateTypes.SubmittedDistributionRoot, error) {
	query := `
		select
			*
		from submitted_distribution_roots as sdr
		left join disabled_distribution_roots as ddr on (sdr.root_index = ddr.root_index)
		where
			ddr.root_index is null
			and sdr.block_number <= @blockHeight
		{{ if eq .claimable "true" }}
			and sdr.activated_at <= now()
		{{ end }}
		order by sdr.block_number desc
		limit 1
	`

	claimableStr := "false"
	if claimable {
		claimableStr = "true"
	}

	// only render claimable since it's safe; blockHeight should be sanitized
	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"claimable": claimableStr,
	})
	if err != nil {
		rds.logger.Sugar().Errorw("failed to render query template",
			zap.Uint64("blockHeight", blockHeight),
			zap.Bool("claimable", claimable),
			zap.Error(err),
		)
		return nil, err
	}

	var root *eigenStateTypes.SubmittedDistributionRoot
	res := rds.db.Raw(renderedQuery, sql.Named("blockHeight", blockHeight)).Scan(&root)
	if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return nil, errors.Join(fmt.Errorf("Failed to find distribution for block number '%d'", blockHeight), res.Error)
	}
	if errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("no distribution root found for blockHeight '%d'", blockHeight)
	}
	return root, nil
}

type SummarizedReward struct {
	Token     string
	Earned    string
	Active    string
	Claimed   string
	Claimable string
}

func setTokenValueInMap(tokenMap map[string]*SummarizedReward, values []*RewardAmount, fieldName string) {
	for _, value := range values {
		v, ok := tokenMap[value.Token]
		if !ok {
			v = &SummarizedReward{
				Token: value.Token,
			}
			tokenMap[value.Token] = v
		}
		f := reflect.ValueOf(v).Elem().FieldByName(fieldName)
		if f.IsValid() && f.CanSet() {
			f.SetString(value.Amount)
		}
	}
}

// GetSummarizedRewards returns the summarized rewards for a given earner at a given block height.
// The blockHeight will be used to find the root that is <= the provided blockHeight
func (rds *RewardsDataService) GetSummarizedRewards(ctx context.Context, earner string, tokens []string, blockHeight uint64) ([]*SummarizedReward, error) {
	if earner == "" {
		return nil, fmt.Errorf("earner is required")
	}
	earner = strings.ToLower(earner)
	tokens = lowercaseTokenList(tokens)

	blockHeight, err := rds.BaseDataService.GetCurrentBlockHeightIfNotPresent(context.Background(), blockHeight)
	if err != nil {
		return nil, err
	}

	tokenMap := make(map[string]*SummarizedReward)

	type ChanResult[T any] struct {
		Data  T
		Error error
	}

	// channels to aggregate results together in a thread safe way
	earnedRewardsChan := make(chan *ChanResult[[]*RewardAmount], 1)
	activeRewardsChan := make(chan *ChanResult[[]*RewardAmount], 1)
	claimableRewardsChan := make(chan *ChanResult[[]*RewardAmount], 1)
	claimedRewardsChan := make(chan *ChanResult[[]*RewardAmount], 1)
	wg := sync.WaitGroup{}
	wg.Add(4)

	go func() {
		defer wg.Done()
		res := &ChanResult[[]*RewardAmount]{}
		earnedRewards, err := rds.GetTotalRewardsForEarner(ctx, earner, tokens, blockHeight, false)
		if err != nil {
			res.Error = err
		} else {
			res.Data = earnedRewards
		}
		earnedRewardsChan <- res
	}()

	go func() {
		defer wg.Done()
		res := &ChanResult[[]*RewardAmount]{}
		activeRewards, err := rds.GetTotalRewardsForEarner(ctx, earner, tokens, blockHeight, true)

		if err != nil {
			res.Error = err
		} else {
			res.Data = activeRewards
		}
		activeRewardsChan <- res
	}()

	go func() {
		defer wg.Done()
		res := &ChanResult[[]*RewardAmount]{}
		claimableRewards, _, err := rds.GetClaimableRewardsForEarner(ctx, earner, tokens, blockHeight)
		if err != nil {
			res.Error = err
		} else {
			res.Data = claimableRewards
		}
		claimableRewardsChan <- res
	}()

	go func() {
		defer wg.Done()
		res := &ChanResult[[]*RewardAmount]{}
		claimedRewards, err := rds.GetTotalClaimedRewards(ctx, earner, tokens, blockHeight)
		if err != nil {
			res.Error = err
		} else {
			res.Data = utils.Map(claimedRewards, func(cr *TotalClaimedReward, i uint64) *RewardAmount {
				return &RewardAmount{
					Token:  cr.Token,
					Amount: cr.Amount,
				}
			})
		}
		claimedRewardsChan <- res
	}()
	wg.Wait()
	close(earnedRewardsChan)
	close(activeRewardsChan)
	close(claimableRewardsChan)
	close(claimedRewardsChan)

	earnedRewards := <-earnedRewardsChan
	if earnedRewards.Error != nil {
		return nil, earnedRewards.Error
	}
	setTokenValueInMap(tokenMap, earnedRewards.Data, "Earned")

	activeRewards := <-activeRewardsChan
	if activeRewards.Error != nil {
		return nil, activeRewards.Error
	}
	setTokenValueInMap(tokenMap, activeRewards.Data, "Active")

	claimableRewards := <-claimableRewardsChan
	if claimableRewards.Error != nil {
		return nil, claimableRewards.Error
	}
	setTokenValueInMap(tokenMap, claimableRewards.Data, "Claimable")

	claimedRewards := <-claimedRewardsChan
	if claimedRewards.Error != nil {
		return nil, claimedRewards.Error
	}
	setTokenValueInMap(tokenMap, claimedRewards.Data, "Claimed")

	tokenList := make([]*SummarizedReward, 0)
	for _, v := range tokenMap {
		tokenList = append(tokenList, v)
	}
	return tokenList, nil
}

func (rds *RewardsDataService) ListAvailableRewardsTokens(ctx context.Context, earner string, blockHeight uint64) ([]string, error) {
	if earner == "" {
		return nil, fmt.Errorf("earner is required")
	}
	earner = strings.ToLower(earner)

	blockHeight, err := rds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	snapshot, err := rds.findDistributionRootClosestToBlockHeight(blockHeight, false)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("no distribution root found for blockHeight '%d'", blockHeight)
	}

	query := `
		select
			distinct(token) as token
		from gold_table as gt
		where
			earner = @earner
			and snapshot <= @snapshot
	`

	var tokens []string
	res := rds.db.Raw(query,
		sql.Named("earner", earner),
		sql.Named("snapshot", snapshot.GetSnapshotDate()),
	).Scan(&tokens)

	if res.Error != nil {
		return nil, res.Error
	}
	return tokens, nil
}

func (rds *RewardsDataService) GetDistributionRootForBlockHeight(ctx context.Context, blockHeight uint64) (*rewards.DistributionRoot, error) {
	if blockHeight == 0 {
		return nil, fmt.Errorf("blockHeight is required")
	}
	query := `
		select
			sdr.*,
			case when ddr.root_index is null then false else true end as disabled
		from submitted_distribution_roots as sdr
		left join disabled_distribution_roots as ddr on (sdr.root_index = ddr.root_index)
		where
			ddr.root_index is null
			and sdr.block_number = @blockHeight
		limit 1
	`
	var root *rewards.DistributionRoot

	res := rds.db.Raw(query, sql.Named("blockHeight", blockHeight)).Scan(&root)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return root, nil
}

func (rds *RewardsDataService) getDistributionRootByRootIndex(rootIndex uint64) (*eigenStateTypes.SubmittedDistributionRoot, error) {
	var root *eigenStateTypes.SubmittedDistributionRoot
	query := `
		select
			*
		from submitted_distribution_roots
		where root_index = @rootIndex
	`
	res := rds.db.Raw(query, sql.Named("rootIndex", rootIndex)).Scan(&root)

	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, errors2.Wrapf(res.Error, "distribution root not found for root index '%d'", rootIndex)
		}
		return nil, res.Error
	}
	return root, nil
}

type AvsReward struct {
	Avs        string
	Earner     string
	Token      string
	Snapshot   string
	RewardHash string
	Amount     string
	RewardType string
}

func (rds *RewardsDataService) GetRewardsByAvsForDistributionRoot(ctx context.Context, rootIndex uint64, pagination *serviceTypes.Pagination) ([]*AvsReward, error) {
	root, err := rds.getDistributionRootByRootIndex(rootIndex)
	if err != nil {
		return nil, err
	}

	if root == nil {
		return nil, fmt.Errorf("no distribution root found for root index '%d'", rootIndex)
	}

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
		SELECT
			cr.avs as avs,
			cr.reward_type as reward_type,
			gt.*
		FROM gold_table as gt
		JOIN (
			SELECT DISTINCT avs, reward_hash,
				reward_type
			FROM combined_rewards as cr
		) cr ON (cr.reward_hash = gt.reward_hash)
		where
			gt.snapshot <= date '{{.snapshotDate}}'
			and gt.reward_hash in (select reward_hash from all_combined_rewards)
		{{ if .pagination }}
		order by gt.snapshot, cr.avs, gt.earner, gt.token desc
		limit @limit
		offset @offset
		{{ end }}
	`

	snapshotDateTime, err := time.Parse(time.DateOnly, root.GetSnapshotDate())
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot date '%s' for rootIndex '%d'", root.GetSnapshotDate(), root.RootIndex)
	}
	cutoffDate := snapshotDateTime.Add(time.Hour * 24).Format(time.DateOnly)

	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"cutoffDate":   cutoffDate,
		"snapshotDate": snapshotDateTime.Format(time.DateOnly),
		"pagination":   pagination != nil,
	})

	queryArgs := []interface{}{}
	if pagination != nil {
		queryArgs = append(queryArgs, sql.Named("limit", pagination.PageSize))
		queryArgs = append(queryArgs, sql.Named("offset", pagination.Page))
	}

	if err != nil {
		return nil, errors2.Wrap(err, "failed to render query template")
	}

	var rewardsResults []*AvsReward
	res := rds.db.Raw(renderedQuery, queryArgs...).Scan(&rewardsResults)
	if res.Error != nil {
		return nil, res.Error
	}
	return rewardsResults, nil
}

type HistoricalReward struct {
	Token    string
	Amount   string
	Snapshot string
}

func (rds *RewardsDataService) ListHistoricalRewardsForEarner(
	ctx context.Context,
	earnerAddress string,
	startBlockHeight uint64,
	endBlockHeight uint64,
	tokens []string,
) ([]*HistoricalReward, error) {
	if earnerAddress == "" {
		return nil, fmt.Errorf("earner is required")
	}
	if endBlockHeight != 0 && startBlockHeight > endBlockHeight {
		return nil, fmt.Errorf("startBlockHeight must be less than or equal to endBlockHeight")
	}

	var startBlock *storage.Block
	var endBlock *storage.Block
	var err error

	if startBlockHeight > 0 {
		startBlock, err = rds.BaseDataService.GetBlock(ctx, startBlockHeight)
		if err != nil {
			return nil, err
		}
		if startBlock == nil {
			return nil, fmt.Errorf("no block found for startBlock '%d'", startBlockHeight)
		}
	}

	// if endBlockHeight is 0, we will use the latest confirmed block
	if endBlockHeight == 0 {
		endBlock, err = rds.BaseDataService.GetLatestConfirmedBlock(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		endBlock, err = rds.BaseDataService.GetBlock(ctx, endBlockHeight)
		if err != nil {
			return nil, err
		}
		if endBlock == nil {
			return nil, fmt.Errorf("no block found for endBlock '%d'", endBlockHeight)
		}
	}

	args := []interface{}{
		sql.Named("earner", earnerAddress),
	}
	query := `
		select
		    snapshot,
			token,
			sum(amount) as amount
		from gold_table as gt
		where
			earner = @earner
	`
	if startBlock != nil {
		query += " and gt.snapshot >= @startBlockDate"
		startBlockDate := startBlock.BlockTime.Format(time.DateOnly)
		args = append(args, sql.Named("startBlockDate", startBlockDate))
	}
	if endBlock != nil {
		query += " and gt.snapshot <= @endBlockDate"
		endBlockDate := endBlock.BlockTime.Format(time.DateOnly)
		args = append(args, sql.Named("endBlockDate", endBlockDate))
	}

	if len(tokens) > 0 {
		query += " and gt.token in (?)"
		tokens = lowercaseTokenList(tokens)
		args = append(args, sql.Named("tokens", tokens))
	}

	query += "group by 1, 2 order by 1 desc, 2 asc"

	var historicalRewards []*HistoricalReward
	res := rds.db.Raw(query, args...).Scan(&historicalRewards)
	if res.Error != nil {
		return nil, res.Error
	}
	return historicalRewards, nil
}
