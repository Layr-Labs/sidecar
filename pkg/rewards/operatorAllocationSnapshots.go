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
		operator,
		strategy,
		magnitude,
		avs,
		operator_set_id,
		block_number,
		effective_block,
		transaction_hash,
		log_index
	FROM operator_allocations
	WHERE effective_block <= @cutoffDate_blockHeight
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
-- Keep only the most recent allocation for each combination
current_allocations AS (
	SELECT
		operator,
		strategy,
		magnitude,
		avs,
		operator_set_id,
		cast(@cutoffDate as date) as snapshot
	FROM latest_allocations
	WHERE rn = 1
		AND magnitude > 0  -- Only include active allocations
)
SELECT * FROM current_allocations
ORDER BY operator, strategy, avs, operator_set_id
`

type OperatorAllocationSnapshot struct {
	Operator      string
	Strategy      string
	Magnitude     string
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

	// Get the latest block (we'll use snapshot date in the query instead)
	latestBlock, err := rc.blockStore.GetLatestBlock()
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to get latest block", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(operatorAllocationSnapshotsQuery, map[string]interface{}{
		"destTableName": destTableName,
		"cutoffDate":    snapshotDate,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query,
		sql.Named("cutoffDate", snapshotDate),
		sql.Named("cutoffDate_blockHeight", latestBlock.Number),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate operator allocation snapshots", "error", res.Error)
		return res.Error
	}

	return nil
}
