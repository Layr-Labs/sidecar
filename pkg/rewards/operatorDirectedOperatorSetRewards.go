package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

const operatorDirectedOperatorSetRewardsQuery = `
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
`

func (r *RewardsCalculator) GenerateAndInsertOperatorDirectedOperatorSetRewards(snapshotDate string) error {
	tableName := "operator_directed_operator_set_rewards"

	query, err := rewardsUtils.RenderQueryTemplate(operatorDirectedOperatorSetRewardsQuery, map[string]interface{}{
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

func (rc *RewardsCalculator) ListOperatorDirectedOperatorSetRewards() ([]*OperatorDirectedOperatorSetRewards, error) {
	var operatorDirectedOperatorSetRewards []*OperatorDirectedOperatorSetRewards
	res := rc.grm.Model(&OperatorDirectedOperatorSetRewards{}).Find(&operatorDirectedOperatorSetRewards)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list combined rewards", "error", res.Error)
		return nil, res.Error
	}
	return operatorDirectedOperatorSetRewards, nil
}
