package protocolDataService

import (
	"context"
	"database/sql"
	"strings"
)

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

// ListOperatorsForStrategy returns operators that support a specific strategy
func (pds *ProtocolDataService) ListOperatorsForStrategy(ctx context.Context, strategy string, blockHeight uint64) ([]string, error) {
	strategy = strings.ToLower(strategy)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		select distinct osors.operator
		from operator_set_strategy_registration_snapshots ossrs
		join operator_set_operator_registration_snapshots osors
			on ossrs.avs = osors.avs 
			and ossrs.operator_set_id = osors.operator_set_id
			and ossrs.snapshot_block_number = osors.snapshot_block_number
		where 
			ossrs.strategy = @strategy
			and ossrs.is_active = true
			and osors.is_active = true
			and ossrs.snapshot_block_number = (
				select max(snapshot_block_number) 
				from operator_set_strategy_registration_snapshots 
				where snapshot_block_number <= @blockHeight
			)
		order by osors.operator
	`

	var operators []string
	res := pds.db.Raw(query,
		sql.Named("strategy", strategy),
		sql.Named("blockHeight", blockHeight),
	).Scan(&operators)
	if res.Error != nil {
		return nil, res.Error
	}
	return operators, nil
}

// ListOperatorsForAvs returns operators registered to an AVS
func (pds *ProtocolDataService) ListOperatorsForAvs(ctx context.Context, avs string, blockHeight uint64) ([]string, error) {
	avs = strings.ToLower(avs)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		select distinct osors.operator
		from operator_set_operator_registration_snapshots osors
		where 
			osors.avs = @avs
			and osors.is_active = true
			and osors.snapshot_block_number = (
				select max(snapshot_block_number) 
				from operator_set_operator_registration_snapshots 
				where snapshot_block_number <= @blockHeight
			)
		order by osors.operator
	`

	var operators []string
	res := pds.db.Raw(query,
		sql.Named("avs", avs),
		sql.Named("blockHeight", blockHeight),
	).Scan(&operators)
	if res.Error != nil {
		return nil, res.Error
	}
	return operators, nil
}

// ListOperatorsForBlockRange returns operators across a block range with optional filters
func (pds *ProtocolDataService) ListOperatorsForBlockRange(ctx context.Context, startBlock, endBlock uint64, avsAddress string, strategyAddress string, stakerAddress string) ([]string, error) {
	// Build query based on filters
	var query string
	var params []interface{}

	if stakerAddress != "" {
		// Query for staker operators across block range
		query = `
			SELECT DISTINCT operator
			FROM staker_delegation_changes
			WHERE delegated = true
				AND block_number BETWEEN @startBlock AND @endBlock
				AND staker = @staker
			ORDER BY operator
		`
		params = []interface{}{
			sql.Named("startBlock", startBlock),
			sql.Named("endBlock", endBlock),
			sql.Named("staker", strings.ToLower(stakerAddress)),
		}
	} else if strategyAddress != "" {
		// Query for strategy operators across block range
		query = `
			SELECT DISTINCT osors.operator
			FROM operator_set_strategy_registration_snapshots ossrs
			JOIN operator_set_operator_registration_snapshots osors
				ON ossrs.avs = osors.avs 
				AND ossrs.operator_set_id = osors.operator_set_id
				AND ossrs.snapshot_block_number = osors.snapshot_block_number
			WHERE ossrs.is_active = true
				AND osors.is_active = true
				AND ossrs.snapshot_block_number BETWEEN @startBlock AND @endBlock
				AND ossrs.strategy = @strategy
			ORDER BY osors.operator
		`
		params = []interface{}{
			sql.Named("startBlock", startBlock),
			sql.Named("endBlock", endBlock),
			sql.Named("strategy", strings.ToLower(strategyAddress)),
		}
	} else if avsAddress != "" {
		// Query for AVS operators across block range
		query = `
			SELECT DISTINCT osors.operator
			FROM operator_set_operator_registration_snapshots osors
			WHERE osors.is_active = true
				AND osors.snapshot_block_number BETWEEN @startBlock AND @endBlock
				AND osors.avs = @avs
			ORDER BY osors.operator
		`
		params = []interface{}{
			sql.Named("startBlock", startBlock),
			sql.Named("endBlock", endBlock),
			sql.Named("avs", strings.ToLower(avsAddress)),
		}
	} else {
		// Query for all operators across block range (no filter)
		query = `
			SELECT DISTINCT osors.operator
			FROM operator_set_operator_registration_snapshots osors
			WHERE osors.is_active = true
				AND osors.snapshot_block_number BETWEEN @startBlock AND @endBlock
			ORDER BY osors.operator
		`
		params = []interface{}{
			sql.Named("startBlock", startBlock),
			sql.Named("endBlock", endBlock),
		}
	}

	var operators []string
	res := pds.db.Raw(query, params...).Scan(&operators)
	if res.Error != nil {
		return nil, res.Error
	}
	return operators, nil
}
