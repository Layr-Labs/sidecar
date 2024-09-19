package rewards

import (
	"database/sql"
	"go.uber.org/zap"
)

const operatorShareSnapshotsQuery = `
with operator_shares_with_block_info as (
	select
		os.operator,
		os.strategy,
		os.shares,
		os.block_number,
		b.block_time,
		DATE(b.block_time) as block_date
	from operator_shares as os
	left join blocks as b on (b.number = os.block_number)
),
ranked_operator_records as (
	select
		*,
		ROW_NUMBER() OVER (PARTITION BY operator, strategy, block_date ORDER BY block_time DESC) as rn
	from operator_shares_with_block_info as os
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
	SELECT
		operator,
		strategy,
		shares,
		block_time,
		DATE(block_date, '+1 day') as snapshot_time
	from ranked_operator_records
	where rn = 1
),
-- Get the range for each operator, strategy pairing
operator_share_windows as (
	SELECT
		operator, strategy, shares, snapshot_time as start_time,
	CASE
		-- If the range does not have the end, use the current timestamp truncated to 0 UTC
		WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time) is null THEN DATE(@cutoffDate)
		ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time)
	END AS end_time
	FROM snapshotted_records
),
cleaned_records as (
	SELECT * FROM operator_share_windows
	WHERE start_time < end_time
),
date_bounds as (
	select
		min(start_time) as min_start,
		max(end_time) as max_end
	from cleaned_records
),
day_series AS (
	with RECURSIVE day_series_inner AS (
		SELECT DATE(min_start) AS day
		FROM date_bounds
		UNION ALL
		SELECT DATE(day, '+1 day')
		FROM day_series_inner
		WHERE day < (SELECT max_end FROM date_bounds)
	)
	select * from day_series_inner
),
final_results as (
	SELECT
		operator,
		strategy,
		shares,
		day as snapshot
	FROM cleaned_records
	cross join day_series
		where DATE(day) between DATE(start_time) and DATE(end_time, '-1 day')
)
select * from final_results
`

type OperatorShareSnapshots struct {
	Operator string
	Strategy string
	Shares   string
	Snapshot string
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

func (r *RewardsCalculator) GenerateAndInsertOperatorShareSnapshots(snapshotDate string) error {
	snapshots, err := r.GenerateOperatorShareSnapshots(snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator share snapshots", "error", err)
		return err
	}

	r.logger.Sugar().Infow("Inserting operator share snapshots", "count", len(snapshots))
	res := r.calculationDB.Model(&OperatorShareSnapshots{}).CreateInBatches(snapshots, 100)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert operator share snapshots", "error", res.Error)
		return res.Error
	}

	return nil
}

func (r *RewardsCalculator) CreateOperatorSharesSnapshotsTable() error {
	res := r.calculationDB.Exec(`
		CREATE TABLE IF NOT EXISTS operator_share_snapshots (
			operator TEXT,
			strategy TEXT,
			shares TEXT,
			snapshot TEXT
		)
	`)
	if res.Error != nil {
		r.logger.Error("Failed to create operator share snapshots table", zap.Error(res.Error))
		return res.Error
	}
	return nil
}
