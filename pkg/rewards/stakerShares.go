package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

const stakerSharesQuery = `
	select
		staker,
		strategy,
		-- Sum each share amount over the window to get total shares for each (staker, strategy) at every timestamp update */
		SUM(shares) OVER (PARTITION BY staker, strategy ORDER BY block_time, log_index) AS shares, 
		transaction_hash,
		log_index,
		strategy_index,
		block_time,
		block_date,
		block_number
	from staker_share_deltas
	where block_date < '{{.cutoffDate}}'
`

func (r *RewardsCalculator) GenerateAndInsertStakerShares(snapshotDate string) error {
	tableName := "staker_shares"

	query, err := rewardsUtils.RenderQueryTemplate(stakerSharesQuery, map[string]interface{}{
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

func (r *RewardsCalculator) ListStakerShares() ([]*StakerShares, error) {
	var stakerShares []*StakerShares
	res := r.grm.Model(&StakerShares{}).Find(&stakerShares)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list staker share stakerShares", "error", res.Error)
		return nil, res.Error
	}
	return stakerShares, nil
}
