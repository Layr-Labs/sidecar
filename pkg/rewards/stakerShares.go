package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const stakerSharesQuery = `
	insert into staker_shares(staker, strategy, shares, transaction_hash, log_index, strategy_index, block_time, block_date, block_number)
	with recent_max_block as (
	    select coalesce(max(block_number), 0) as block_number from staker_shares
	)
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
	where
		block_date < '{{.cutoffDate}}'
		-- only need to fetch new records since the last snapshot .
	  	-- we use a >= in case not all records are inserted for the MAX(block_number)
	  	and block_number >= (select block_number from recent_max_block)
	on conflict on constraint uniq_staker_shares do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertStakerShares(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(stakerSharesQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := r.grm.Debug().Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert staker_shares",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
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
