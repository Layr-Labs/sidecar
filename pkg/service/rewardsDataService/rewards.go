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
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"reflect"
	"strings"
	"sync"
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

func (rds *RewardsDataService) GetRewardsForSnapshot(ctx context.Context, snapshot string) ([]*rewardsTypes.Reward, error) {
	return rds.rewardsCalculator.FetchRewardsForSnapshot(snapshot)
}

type TotalClaimedReward struct {
	Earner string
	Token  string
	Amount string
}

func (rds *RewardsDataService) GetTotalClaimedRewards(ctx context.Context, earner string, tokens []string, blockHeight uint64) ([]*TotalClaimedReward, error) {
	blockHeight, err := rds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		select
			earner,
			token,
			sum(claimed_amount) as amount
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
		args = append(args, sql.Named("earner", earner))
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		formattedTokens := utils.Map(tokens, func(token string, i uint64) string {
			return strings.ToLower(token)
		})
		args = append(args, sql.Named("tokens", formattedTokens))
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
			order by snapshot desc
		)
		select
			token,
			sum(amount) as amount
		from token_snapshots
		group by 1
	`
	args := []interface{}{
		sql.Named("earner", earner),
		sql.Named("snapshot", snapshot.GetSnapshotDate()),
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		formattedTokens := utils.Map(tokens, func(token string, i uint64) string {
			return strings.ToLower(token)
		})
		args = append(args, sql.Named("tokens", formattedTokens))
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
	snapshot, err := rds.findDistributionRootClosestToBlockHeight(blockHeight, true)
	if err != nil {
		return nil, nil, err
	}
	if snapshot == nil {
		return nil, nil, fmt.Errorf("no distribution root found for blockHeight '%d'", blockHeight)
	}
	query := `
		with claimed_tokens as (
			select
				earner,
				token,
				sum(claimed_amount) as amount
			from rewards_claimed as rc
			where
				earner = @earner
				and block_number <= @blockNumber
			group by 1, 2
		),
		earner_tokens as (
			select
				earner,
				token,
				sum(amount) as amount
			from gold_table as gt
			where
				earner = @earner
				and snapshot <= @snapshot
			group by earner, token
		)
		select
			et.earner,
			et.token,
			et.amount::numeric as earned_amount,
			coalesce(ct.amount, 0)::numeric as claimed_amount,
			(coalesce(et.amount, 0) - coalesce(ct.amount, 0))::numeric as claimable
		from earner_tokens as et
		left join claimed_tokens as ct on (
			ct.token = et.token
			and ct.earner = et.earner
		)
	`
	args := []interface{}{
		sql.Named("earner", earner),
		sql.Named("blockNumber", blockHeight),
		sql.Named("snapshot", snapshot.GetSnapshotDate()),
	}
	if len(tokens) > 0 {
		query += " and token in (?)"
		formattedTokens := utils.Map(tokens, func(token string, i uint64) string {
			return strings.ToLower(token)
		})
		args = append(args, sql.Named("tokens", formattedTokens))
	}

	claimableRewards := make([]*RewardAmount, 0)
	res := rds.db.Raw(query, args...).Scan(&claimableRewards)
	if res.Error != nil {
		return nil, nil, res.Error
	}
	return claimableRewards, snapshot, nil
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
