package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const stakeOperatorSetRewardsQuery = `
-- Insert unique stake submissions
INSERT INTO stake_operator_set_rewards (avs, operator_set_id, reward_hash, token, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, reward_type, block_number, block_time, block_date)
SELECT
	usrs.avs,
	usrs.operator_set_id,
	usrs.reward_hash,
	usrs.token,
	usrs.amount,
	usrs.strategy,
	usrs.strategy_index,
	usrs.multiplier,
	usrs.start_timestamp::TIMESTAMP(6),
	usrs.end_timestamp::TIMESTAMP(6),
	usrs.duration,
	'unique_stake' as reward_type,
	usrs.block_number,
	b.block_time::TIMESTAMP(6),
	TO_CHAR(b.block_time, 'YYYY-MM-DD') AS block_date
FROM unique_stake_reward_submissions AS usrs
JOIN blocks AS b ON (b.number = usrs.block_number)
WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
ON CONFLICT ON CONSTRAINT uniq_stake_operator_set_rewards DO NOTHING;

-- Insert total stake submissions
INSERT INTO stake_operator_set_rewards (avs, operator_set_id, reward_hash, token, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, reward_type, block_number, block_time, block_date)
SELECT
	tsrs.avs,
	tsrs.operator_set_id,
	tsrs.reward_hash,
	tsrs.token,
	tsrs.amount,
	tsrs.strategy,
	tsrs.strategy_index,
	tsrs.multiplier,
	tsrs.start_timestamp::TIMESTAMP(6),
	tsrs.end_timestamp::TIMESTAMP(6),
	tsrs.duration,
	'total_stake' as reward_type,
	tsrs.block_number,
	b.block_time::TIMESTAMP(6),
	TO_CHAR(b.block_time, 'YYYY-MM-DD') AS block_date
FROM total_stake_reward_submissions AS tsrs
JOIN blocks AS b ON (b.number = tsrs.block_number)
WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
ON CONFLICT ON CONSTRAINT uniq_stake_operator_set_rewards DO NOTHING;
`

func (r *RewardsCalculator) GenerateAndInsertStakeOperatorSetRewards(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(stakeOperatorSetRewardsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render stake_operator_set_rewards query", "error", err)
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate stake_operator_set_rewards",
			zap.Error(res.Error),
			zap.String("snapshotDate", snapshotDate),
		)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) ListStakeOperatorSetRewards() ([]*StakeOperatorSetRewards, error) {
	var stakeOperatorSetRewards []*StakeOperatorSetRewards
	res := rc.grm.Model(&StakeOperatorSetRewards{}).Find(&stakeOperatorSetRewards)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list stake operator set rewards", "error", res.Error)
		return nil, res.Error
	}
	return stakeOperatorSetRewards, nil
}
