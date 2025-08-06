package protocolDataService

import (
	"context"
	"database/sql"
	"strings"
)

type OperatorSet struct {
	Operator      string
	OperatorSetId uint64
}

// ListOperatorsForStaker returns operators that a staker has delegated to
func (pds *ProtocolDataService) ListOperatorsForStaker(ctx context.Context, staker string, blockHeight uint64) ([]string, error) {
	staker = strings.ToLower(staker)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		select distinct on (staker)
			operator
		from staker_delegation_changes
		where
			staker = @staker
			and delegated = true
			and block_number <= @blockHeight
		order by staker, block_number desc, log_index asc
	`

	var operators []string
	res := pds.db.Raw(query,
		sql.Named("staker", staker),
		sql.Named("blockHeight", blockHeight),
	).Scan(&operators)
	if res.Error != nil {
		return nil, res.Error
	}
	return operators, nil
}

// ListOperatorsForStrategy returns operators that have delegated stakers in the specified strategy
func (pds *ProtocolDataService) ListOperatorsForStrategy(ctx context.Context, strategy string, blockHeight uint64) ([]string, error) {
	strategy = strings.ToLower(strategy)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	// First get the block time for the given block height
	var blockTime string
	blockTimeRes := pds.db.Raw("select block_time::date from blocks where number <= @blockHeight limit 1",
		sql.Named("blockHeight", blockHeight)).Scan(&blockTime)
	if blockTimeRes.Error != nil {
		return nil, blockTimeRes.Error
	}

	// Then use the block time to filter staker_operator
	query := `
		select distinct operator
		from staker_operator
		where
			strategy = @strategy
			and snapshot <= @blockTime
		order by operator
	`

	var operators []string
	res := pds.db.Raw(query,
		sql.Named("strategy", strategy),
		sql.Named("blockTime", blockTime),
	).Scan(&operators)
	if res.Error != nil {
		return nil, res.Error
	}
	return operators, nil
}

// ListOperatorsForAvs returns operators registered to an AVS (both registered and unregistered)
func (pds *ProtocolDataService) ListOperatorsForAvs(ctx context.Context, avs string, blockHeight uint64) ([]OperatorSet, error) {
	avs = strings.ToLower(avs)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		with latest_registrations as (
			select distinct on (operator)
				operator,
				registered
			from avs_operator_state_changes
			where
				avs = @avs
				and block_number <= @blockHeight
		)
		select 
			lr.operator,
			case 
				when lr.registered = true then os.operator_set_id
				else null
			end as operator_set_id
		from latest_registrations lr
		left join operator_sets os on (
			os.avs = @avs
			and lr.registered = true
			and os.block_number <= @blockHeight
		)
		order by operator
	`

	var operatorSets []OperatorSet
	res := pds.db.Raw(query,
		sql.Named("avs", avs),
		sql.Named("blockHeight", blockHeight),
	).Scan(&operatorSets)
	if res.Error != nil {
		return nil, res.Error
	}

	return operatorSets, nil
}

// ListOperatorsForBlockRange returns operators across a block range with optional filters
// Uses staker_operator as the main table and adds WHERE clauses based on filters
func (pds *ProtocolDataService) ListOperatorsForBlockRange(ctx context.Context, startBlock, endBlock uint64, avsAddress string, strategyAddress string, stakerAddress string) ([]string, error) {
	// Base query using staker_operator table
	query := `
		select distinct operator
		from staker_operator
		where
			snapshot::date >= (
				select block_time::date
				from blocks
				where number >= @startBlock
				limit 1
			)
			snapshot::date <= (
				select block_time::date
				from blocks
				where number <= @endBlock
				limit 1
			)
	`

	params := []interface{}{
		sql.Named("startBlock", startBlock),
		sql.Named("endBlock", endBlock),
	}

	// Add WHERE clauses based on filters
	if stakerAddress != "" {
		query += ` and earner = @staker`
		params = append(params, sql.Named("staker", strings.ToLower(stakerAddress)))
	}

	if strategyAddress != "" {
		query += ` and strategy = @strategy`
		params = append(params, sql.Named("strategy", strings.ToLower(strategyAddress)))
	}

	if avsAddress != "" {
		query += ` and avs = @avs`
		params = append(params, sql.Named("avs", strings.ToLower(avsAddress)))
	}

	query += ` order by operator`

	var operators []string
	res := pds.db.Raw(query, params...).Scan(&operators)
	if res.Error != nil {
		return nil, res.Error
	}
	return operators, nil
}
