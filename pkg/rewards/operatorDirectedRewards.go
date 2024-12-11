package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

const operatorDirectedRewardsQuery = `
	with _operator_directed_rewards as (
		SELECT
			odrs.avs,
			odrs.reward_hash,
			odrs.token,
			odrs.operator,
			odrs.operator_index,
			odrs.amount,
			odrs.strategy,
			odrs.strategy_index,
			odrs.multiplier,
			odrs.start_timestamp::TIMESTAMP(6),
			odrs.end_timestamp::TIMESTAMP(6),
			odrs.duration,
			odrs.block_number,
			b.block_time::TIMESTAMP(6),
			TO_CHAR(b.block_time, 'YYYY-MM-DD') AS block_date
		FROM operator_directed_reward_submissions AS odrs
		JOIN blocks AS b ON(b.number = odrs.block_number)
		WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
	)
	select
		avs,
		reward_hash,
		token,
		operator,
		operator_index,
		amount,
		strategy,
		strategy_index,
		multiplier,
		start_timestamp::TIMESTAMP(6),
		end_timestamp::TIMESTAMP(6),
		duration,
		block_number,
		block_time,
		block_date
	from _operator_directed_rewards
`

func (r *RewardsCalculator) GenerateAndInsertOperatorDirectedRewards(snapshotDate string) error {
	tableName := "operator_directed_rewards"

	query, err := rewardsUtils.RenderQueryTemplate(operatorDirectedRewardsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render rewards combined query", "error", err)
		return err
	}

	err = r.generateAndInsertFromQuery(tableName, query, nil)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate combined rewards", "error", err)
		return err
	}
	return nil
}

func (rc *RewardsCalculator) ListOperatorDirectedRewards() ([]*OperatorDirectedRewards, error) {
	var operatorDirectedRewards []*OperatorDirectedRewards
	res := rc.grm.Model(&OperatorDirectedRewards{}).Find(&operatorDirectedRewards)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list combined rewards", "error", res.Error)
		return nil, res.Error
	}
	return operatorDirectedRewards, nil
}
