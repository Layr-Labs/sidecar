package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorAllocationSnapshotsQuery = `
create table {{.destTableName}} as
WITH allocation_events AS (
	SELECT
		oa.operator,
		oa.strategy,
		oa.magnitude,
		oa.avs,
		oa.operator_set_id,
		oa.block_number,
		oa.effective_block,
		oa.transaction_hash,
		oa.log_index
	FROM operator_allocations oa
	WHERE oa.effective_block <= @cutoffDate_blockHeight
),
-- Use effective_block to find the latest allocation for each (operator, strategy, avs, operator_set_id) combination
-- at the cutoff block height
latest_allocations AS (
	SELECT
		operator,
		strategy,
		magnitude,
		avs,
		operator_set_id,
		effective_block,
		ROW_NUMBER() OVER (
			PARTITION BY operator, strategy, avs, operator_set_id
			ORDER BY effective_block DESC, block_number DESC, log_index DESC
		) AS rn
	FROM allocation_events
),
-- Get the most recent max_magnitude for each (operator, strategy) at the cutoff block
latest_max_magnitudes AS (
	SELECT
		omm.operator,
		omm.strategy,
		omm.max_magnitude,
		ROW_NUMBER() OVER (
			PARTITION BY omm.operator, omm.strategy
			ORDER BY omm.block_number DESC, omm.log_index DESC
		) AS rn
	FROM operator_max_magnitudes omm
	WHERE omm.block_number <= @cutoffDate_blockHeight
),
-- Join allocations with max_magnitudes
current_allocations AS (
	SELECT
		la.operator,
		la.strategy,
		la.magnitude,
		lmm.max_magnitude,
		la.avs,
		la.operator_set_id,
		cast(@cutoffDate as date) as snapshot
	FROM latest_allocations la
	LEFT JOIN latest_max_magnitudes lmm
		ON la.operator = lmm.operator
		AND la.strategy = lmm.strategy
		AND lmm.rn = 1
	WHERE la.rn = 1
		AND la.magnitude > 0  -- Only include active allocations
		AND lmm.max_magnitude IS NOT NULL  -- Must have max_magnitude
		AND lmm.max_magnitude != '0'  -- Max magnitude must be non-zero
)
SELECT * FROM current_allocations
ORDER BY operator, strategy, avs, operator_set_id
`

type OperatorAllocationSnapshot struct {
	Operator      string
	Strategy      string
	Magnitude     string
	MaxMagnitude  string
	Avs           string
	OperatorSetId uint64
	Snapshot      string
}

func (rc *RewardsCalculator) GenerateAndInsertOperatorAllocationSnapshots(snapshotDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_OperatorAllocationSnapshots]

	rc.logger.Sugar().Infow("Generating operator allocation snapshots",
		zap.String("snapshotDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	// CRITICAL: Get the block height AT the snapshot date for retroactive rewards support
	// This ensures we only include allocations that were effective at that historical point in time
	type BlockAtDate struct {
		BlockNumber uint64
	}
	var blockAtDate BlockAtDate

	blockQuery := `
		SELECT number as block_number
		FROM blocks
		WHERE block_time::date <= @snapshotDate::date
		ORDER BY block_time DESC
		LIMIT 1
	`

	res := rc.grm.Raw(blockQuery, sql.Named("snapshotDate", snapshotDate)).Scan(&blockAtDate)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get block at snapshot date", "error", res.Error)
		return res.Error
	}

	rc.logger.Sugar().Infow("Using block height for snapshot",
		zap.String("snapshotDate", snapshotDate),
		zap.Uint64("blockNumber", blockAtDate.BlockNumber),
	)

	query, err := rewardsUtils.RenderQueryTemplate(operatorAllocationSnapshotsQuery, map[string]interface{}{
		"destTableName": destTableName,
		"cutoffDate":    snapshotDate,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res = rc.grm.Exec(query,
		sql.Named("cutoffDate", snapshotDate),
		sql.Named("cutoffDate_blockHeight", blockAtDate.BlockNumber),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator allocation snapshots", "error", res.Error)
		return res.Error
	}

	return nil
}
