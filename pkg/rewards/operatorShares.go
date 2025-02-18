package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorSharesQuery = `
	insert into operator_shares (operator, strategy, transaction_hash, log_index, block_number, block_date, block_time, shares)
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
	on conflict on constraint uniq_operator_shares do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorShares(snapshotDate string) error {

	query, err := rewardsUtils.RenderQueryTemplate(operatorSharesQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", zap.Error(err))
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_shares",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
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
