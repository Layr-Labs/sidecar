package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

const operatorSharesQuery = `
	select
		operator,
		strategy,
		transaction_hash,
		log_index,
		block_number,
		block_date,
		block_time,
		SUM(shares) OVER (PARTITION BY operator, strategy ORDER BY block_time, log_index) AS shares
	from operator_share_deltas
	where block_date < '{{.cutoffDate}}'
`

func (r *RewardsCalculator) GenerateAndInsertOperatorShares(snapshotDate string) error {
	tableName := "operator_shares"

	query, err := rewardsUtils.RenderQueryTemplate(operatorSharesQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	err = r.generateAndInsertFromQuery(tableName, query, nil)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate staker_shares", "error", err)
		return err
	}
	return nil
}

func (r *RewardsCalculator) ListOperatorShares() ([]*OperatorShares, error) {
	var operatorShares []*OperatorShares
	res := r.grm.Model(&OperatorShares{}).Find(&operatorShares)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list staker share operatorShares", "error", res.Error)
		return nil, res.Error
	}
	return operatorShares, nil
}
