package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorDirectedOperatorSetRewardsQuery = `
insert into operator_directed_operator_set_rewards (avs, operator_set_id, reward_hash, token, operator, operator_index, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, block_number, block_time, block_date)
SELECT
	odosrs.avs,
	odosrs.operator_set_id,
	odosrs.reward_hash,
	odosrs.token,
	odosrs.operator,
	odosrs.operator_index,
	odosrs.amount,
	odosrs.strategy,
	odosrs.strategy_index,
	odosrs.multiplier,
	odosrs.start_timestamp::TIMESTAMP(6),
	odosrs.end_timestamp::TIMESTAMP(6),
	odosrs.duration,
	odosrs.block_number,
	b.block_time::TIMESTAMP(6),
	TO_CHAR(b.block_time, 'YYYY-MM-DD') AS block_date
FROM operator_directed_operator_set_reward_submissions AS odosrs
JOIN blocks AS b ON (b.number = odosrs.block_number)
WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
on conflict on constraint uniq_operator_directed_operator_set_rewards do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorDirectedOperatorSetRewards(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(operatorDirectedOperatorSetRewardsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render operator_directed_operator_set_rewards query", "error", err)
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_directed_operator_set_rewards",
			zap.Error(res.Error),
			zap.String("snapshotDate", snapshotDate),
		)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) ListOperatorDirectedOperatorSetRewards() ([]*OperatorDirectedOperatorSetRewards, error) {
	var operatorDirectedOperatorSetRewards []*OperatorDirectedOperatorSetRewards
	res := rc.grm.Model(&OperatorDirectedOperatorSetRewards{}).Find(&operatorDirectedOperatorSetRewards)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list combined rewards", "error", res.Error)
		return nil, res.Error
	}
	return operatorDirectedOperatorSetRewards, nil
}
