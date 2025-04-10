package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const rewardsCombinedQuery = `
insert into combined_rewards (avs, reward_hash, token, amount, start_timestamp, duration, end_timestamp, strategy, multiplier, strategy_index, block_number, block_time, block_date, reward_type)
with _combined_rewards as (
	select
		rs.avs,
		rs.reward_hash,
		rs.token,
		rs.amount,
		rs.strategy,
		rs.strategy_index,
		rs.multiplier,
		rs.start_timestamp,
		rs.end_timestamp,
		rs.duration,
		rs.block_number,
		b.block_time::timestamp(6),
		to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
		rs.reward_type
	from reward_submissions as rs
	left join blocks as b on (b.number = rs.block_number) 
	-- pipeline bronze table uses this to filter the correct records
	where b.block_time < TIMESTAMP '{{.cutoffDate}}'
)
select
	avs,
	reward_hash,
	token,
	amount,
	start_timestamp,
	duration,
	end_timestamp,
	strategy,
	multiplier,
	strategy_index,
	block_number,
	block_time,
	block_date,
	reward_type
from _combined_rewards
on conflict on constraint uniq_combined_rewards do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertCombinedRewards(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(rewardsCombinedQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render rewards combined query",
			zap.Error(err),
		)
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate combined rewards",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return err
	}
	return nil
}

func (rc *RewardsCalculator) ListCombinedRewards() ([]*CombinedRewards, error) {
	var combinedRewards []*CombinedRewards
	res := rc.grm.Model(&CombinedRewards{}).Find(&combinedRewards)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list combined rewards", "error", res.Error)
		return nil, res.Error
	}
	return combinedRewards, nil
}
