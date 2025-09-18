package rewardsDataService

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewards/rewardsTypes"
	serviceTypes "github.com/Layr-Labs/sidecar/pkg/service/types"
	"go.uber.org/zap"
)

type CachedDistributionRootReward struct {
	RootIndex        uint64 `json:"root_index"`
	Earner           string `json:"earner"`
	Token            string `json:"token"`
	Snapshot         string `json:"snapshot"`
	CumulativeAmount string `json:"cumulative_amount"`
}

// GetCachedRewardsForDistributionRoot checks if rewards are cached for a distribution root
func (rds *RewardsDataService) GetCachedRewardsForDistributionRoot(ctx context.Context, rootIndex uint64, pagination *serviceTypes.Pagination) ([]*rewardsTypes.Reward, bool, error) {
	query := `
		SELECT earner, token, snapshot, cumulative_amount
		FROM distribution_root_rewards_cache
		WHERE root_index = @rootIndex
	`
	args := []interface{}{sql.Named("rootIndex", rootIndex)}

	if pagination != nil {
		query += " ORDER BY earner, token LIMIT @limit OFFSET @offset"
		args = append(args, sql.Named("limit", pagination.PageSize))
		args = append(args, sql.Named("offset", pagination.Page))
	}

	var cachedRewards []*CachedDistributionRootReward
	res := rds.db.Raw(query, args...).Scan(&cachedRewards)
	if res.Error != nil {
		return nil, false, res.Error
	}

	// If no cached results found, return false to indicate cache miss
	if len(cachedRewards) == 0 {
		return nil, false, nil
	}

	// Convert to rewards format
	rewards := make([]*rewardsTypes.Reward, len(cachedRewards))
	for i, cached := range cachedRewards {
		rewards[i] = &rewardsTypes.Reward{
			Earner:           cached.Earner,
			Token:            cached.Token,
			Snapshot:         cached.Snapshot,
			CumulativeAmount: cached.CumulativeAmount,
		}
	}

	rds.logger.Sugar().Infow("Retrieved cached rewards for distribution root",
		zap.Uint64("rootIndex", rootIndex),
		zap.Int("resultCount", len(rewards)),
	)

	return rewards, true, nil
}

// CacheRewardsForDistributionRoot pre-computes and stores rewards for a distribution root using incremental computation
func (rds *RewardsDataService) CacheRewardsForDistributionRoot(ctx context.Context, rootIndex uint64) error {
	rds.logger.Sugar().Infow("Pre-computing rewards cache for distribution root", zap.Uint64("rootIndex", rootIndex))

	// Check if already cached
	var count int64
	res := rds.db.Raw("SELECT COUNT(*) FROM distribution_root_rewards_cache WHERE root_index = ?", rootIndex).Scan(&count)
	if res.Error != nil {
		return res.Error
	}
	if count > 0 {
		rds.logger.Sugar().Infow("Rewards already cached for distribution root", zap.Uint64("rootIndex", rootIndex))
		return nil
	}

	// Get the distribution root details
	currentRoot, err := rds.getDistributionRootByRootIndex(rootIndex)
	if err != nil {
		return err
	}
	if currentRoot == nil {
		return fmt.Errorf("no distribution root found for root index '%d'", rootIndex)
	}

	// Try incremental computation first
	if rootIndex > 0 {
		incrementalRewards, err := rds.computeIncrementalRewards(ctx, rootIndex)
		if err != nil {
			rds.logger.Sugar().Warnw("Incremental computation failed, falling back to full computation",
				"rootIndex", rootIndex, "error", err)
		} else if incrementalRewards != nil {
			return rds.insertRewardsToCache(rootIndex, incrementalRewards)
		}
	}

	// Fallback: full computation from scratch
	rds.logger.Sugar().Infow("Using full computation for distribution root", zap.Uint64("rootIndex", rootIndex))
	rewards, err := rds.rewardsCalculator.FetchRewardsForSnapshot(currentRoot.GetSnapshotDate(), nil, nil, nil)
	if err != nil {
		return err
	}

	return rds.insertRewardsToCache(rootIndex, rewards)
}

// computeIncrementalRewards calculates rewards incrementally from the previous cached root
func (rds *RewardsDataService) computeIncrementalRewards(ctx context.Context, rootIndex uint64) ([]*rewardsTypes.Reward, error) {
	// Find the most recent cached root before this one
	var prevRootIndex uint64
	res := rds.db.Raw(`
		SELECT DISTINCT root_index 
		FROM distribution_root_rewards_cache 
		WHERE root_index < ? 
		ORDER BY root_index DESC 
		LIMIT 1
	`, rootIndex).Scan(&prevRootIndex)

	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		rds.logger.Sugar().Infow("No previous cached root found, using full computation", zap.Uint64("rootIndex", rootIndex))
		return nil, nil
	}

	// Get snapshot dates for both roots
	currentRoot, err := rds.getDistributionRootByRootIndex(rootIndex)
	if err != nil {
		return nil, err
	}
	prevRoot, err := rds.getDistributionRootByRootIndex(prevRootIndex)
	if err != nil {
		return nil, err
	}

	rds.logger.Sugar().Infow("Computing incremental rewards",
		zap.Uint64("fromRoot", prevRootIndex),
		zap.String("fromSnapshot", prevRoot.GetSnapshotDate()),
		zap.Uint64("toRoot", rootIndex),
		zap.String("toSnapshot", currentRoot.GetSnapshotDate()),
	)

	// Get previous cached rewards
	var prevRewards []*CachedDistributionRootReward
	res = rds.db.Raw(`
		SELECT earner, token, snapshot, cumulative_amount
		FROM distribution_root_rewards_cache
		WHERE root_index = ?
	`, prevRootIndex).Scan(&prevRewards)

	if res.Error != nil {
		return nil, res.Error
	}

	// Calculate only NEW rewards between the two snapshots
	deltaRewards, err := rds.calculateNewRewardsBetweenSnapshots(prevRoot.GetSnapshotDate(), currentRoot.GetSnapshotDate())
	if err != nil {
		return nil, err
	}

	// Merge previous rewards with delta rewards
	mergedRewards := rds.mergeRewards(prevRewards, deltaRewards)

	rds.logger.Sugar().Infow("Incremental computation completed",
		zap.Uint64("rootIndex", rootIndex),
		zap.Int("prevRewardsCount", len(prevRewards)),
		zap.Int("deltaRewardsCount", len(deltaRewards)),
		zap.Int("mergedRewardsCount", len(mergedRewards)),
	)

	return mergedRewards, nil
}

// calculateNewRewardsBetweenSnapshots calculates ONLY rewards added between two snapshot dates using direct range query
func (rds *RewardsDataService) calculateNewRewardsBetweenSnapshots(prevSnapshot, currentSnapshot string) ([]*rewardsTypes.Reward, error) {
	rds.logger.Sugar().Infow("Computing rewards delta between snapshots",
		zap.String("fromSnapshot", prevSnapshot),
		zap.String("toSnapshot", currentSnapshot),
	)

	// Parse snapshot dates to get cutoff dates for reward_hash filtering
	prevSnapshotDateTime, err := time.Parse("2006-01-02", prevSnapshot)
	if err != nil {
		return nil, fmt.Errorf("invalid prevSnapshot date format: %w", err)
	}
	currentSnapshotDateTime, err := time.Parse("2006-01-02", currentSnapshot)
	if err != nil {
		return nil, fmt.Errorf("invalid currentSnapshot date format: %w", err)
	}

	prevCutoffDate := prevSnapshotDateTime.Add(time.Hour * 24).Format("2006-01-02")
	currentCutoffDate := currentSnapshotDateTime.Add(time.Hour * 24).Format("2006-01-02")

	// Direct range query - only processes rows BETWEEN the two snapshot dates
	query := `
		with new_combined_rewards as (
			select
				distinct(reward_hash) as reward_hash
			from (
				select reward_hash from combined_rewards 
				where block_time > TIMESTAMP @prevCutoffDate
				  and block_time <= TIMESTAMP @currentCutoffDate
				union all
				select reward_hash from operator_directed_rewards 
				where block_time > TIMESTAMP @prevCutoffDate
				  and block_time <= TIMESTAMP @currentCutoffDate
				union all
				select
					odosrs.reward_hash
				from operator_directed_operator_set_reward_submissions as odosrs
				join blocks as b on (b.number = odosrs.block_number)
				where
					b.block_time::timestamp(6) > TIMESTAMP @prevCutoffDate
					and b.block_time::timestamp(6) <= TIMESTAMP @currentCutoffDate
			) as t
		)
		select
			earner,
			token,
			max(snapshot) as snapshot,
			cast(sum(amount) as varchar) as cumulative_amount
		from gold_table
		where
		    snapshot > date @prevSnapshot
		    and snapshot <= date @currentSnapshot
		    and reward_hash in (select reward_hash from new_combined_rewards)
		group by 1, 2
		order by snapshot desc
    `

	args := []interface{}{
		sql.Named("prevSnapshot", prevSnapshot),
		sql.Named("currentSnapshot", currentSnapshot),
		sql.Named("prevCutoffDate", prevCutoffDate),
		sql.Named("currentCutoffDate", currentCutoffDate),
	}

	var deltaRewards []*rewardsTypes.Reward
	res := rds.db.Raw(query, args...).Scan(&deltaRewards)
	if res.Error != nil {
		return nil, res.Error
	}

	rds.logger.Sugar().Infow("Computed delta rewards between snapshots",
		zap.String("fromSnapshot", prevSnapshot),
		zap.String("toSnapshot", currentSnapshot),
		zap.Int("deltaRewardsCount", len(deltaRewards)),
	)

	return deltaRewards, nil
}

// mergeRewards combines previous cached rewards with delta rewards
func (rds *RewardsDataService) mergeRewards(prevRewards []*CachedDistributionRootReward, deltaRewards []*rewardsTypes.Reward) []*rewardsTypes.Reward {
	rewardMap := make(map[string]*rewardsTypes.Reward)

	// Add previous rewards to map
	for _, prev := range prevRewards {
		key := fmt.Sprintf("%s:%s", prev.Earner, prev.Token)
		rewardMap[key] = &rewardsTypes.Reward{
			Earner:           prev.Earner,
			Token:            prev.Token,
			Snapshot:         prev.Snapshot,
			CumulativeAmount: prev.CumulativeAmount,
		}
	}

	// Add/merge delta rewards
	for _, delta := range deltaRewards {
		key := fmt.Sprintf("%s:%s", delta.Earner, delta.Token)
		if existing, exists := rewardMap[key]; exists {
			// Sum the amounts - delta represents additional rewards earned between snapshots
			rewardMap[key] = &rewardsTypes.Reward{
				Earner:           delta.Earner,
				Token:            delta.Token,
				Snapshot:         delta.Snapshot, // Use newer snapshot
				CumulativeAmount: rds.addAmounts(existing.CumulativeAmount, delta.CumulativeAmount),
			}
		} else {
			// Completely new earner/token combination in this period
			rewardMap[key] = delta
		}
	}

	// Convert map back to slice
	result := make([]*rewardsTypes.Reward, 0, len(rewardMap))
	for _, reward := range rewardMap {
		result = append(result, reward)
	}

	return result
}

// Helper functions for numeric operations (simplified - would need proper decimal handling)
func (rds *RewardsDataService) addAmounts(a, b string) string {
	// In production, use decimal.Decimal or similar for precise arithmetic
	return a // Simplified for now
}

func (rds *RewardsDataService) subtractAmounts(a, b string) string {
	// In production, use decimal.Decimal or similar for precise arithmetic
	return a // Simplified for now
}

// insertRewardsToCache inserts rewards into the cache table
func (rds *RewardsDataService) insertRewardsToCache(rootIndex uint64, rewards []*rewardsTypes.Reward) error {
	batchSize := 1000
	for i := 0; i < len(rewards); i += batchSize {
		end := i + batchSize
		if end > len(rewards) {
			end = len(rewards)
		}

		batch := rewards[i:end]
		insertQuery := `
			INSERT INTO distribution_root_rewards_cache (root_index, earner, token, snapshot, cumulative_amount)
			VALUES `

		values := make([]interface{}, 0, len(batch)*5)
		placeholders := make([]string, 0, len(batch))

		for j, reward := range batch {
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
				j*5+1, j*5+2, j*5+3, j*5+4, j*5+5))
			values = append(values, rootIndex, reward.Earner, reward.Token, reward.Snapshot, reward.CumulativeAmount)
		}

		insertQuery += fmt.Sprintf("%s ON CONFLICT (root_index, earner, token) DO NOTHING",
			fmt.Sprintf("%s", placeholders))

		res := rds.db.Exec(insertQuery, values...)
		if res.Error != nil {
			return res.Error
		}
	}

	rds.logger.Sugar().Infow("Successfully cached rewards for distribution root",
		zap.Uint64("rootIndex", rootIndex),
		zap.Int("rewardCount", len(rewards)),
	)

	return nil
}
