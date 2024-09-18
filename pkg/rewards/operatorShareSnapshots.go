package rewards

import "database/sql"

const operatorShareSnapshotsQuery = `
with ranked_operator_records as (
	select
		os.operator,
		os.strategy,
		os.block_number,
		b.block_time,
		DATE(b.block_time) as block_date,
		ROW_NUMBER() OVER (PARTITION BY os.operator, os.strategy, DATE(b.block_date) ORDER BY b.block_time DESC) as rn
	from operator_shares as os
	left join blocks as b on (b.number = os.block_number)
)


`

type OperatorShareSnapshots struct {
}

func (r *RewardsCalculator) GenerateOperatorShareSnapshots(snapshotDate string) ([]*OperatorShareSnapshots, error) {
	results := make([]*OperatorShareSnapshots, 0)

	res := r.grm.Raw(operatorShareSnapshotsQuery, sql.Named("cutoffDate", snapshotDate)).Scan(&results)

	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator share snapshots", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}
