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

	// First get the block time for the given block height
	var blockTime string
	blockTimeRes := pds.db.Raw("select block_time::date from blocks where number <= @blockHeight limit 1",
		sql.Named("blockHeight", blockHeight)).Scan(&blockTime)
	if blockTimeRes.Error != nil {
		return nil, blockTimeRes.Error
	}

	query := `
		select distinct operator
		from staker_delegation_snapshots
		where
			staker = @staker
			and snapshot <= @blockTime
	`

	var operators []string
	res := pds.db.Raw(query,
		sql.Named("staker", staker),
		sql.Named("blockTime", blockTime),
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
		from operator_avs_strategy_snapshots
		where
			strategy = @strategy
			and snapshot <= @blockTime
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

// ListOperatorsForAvs returns operators registered to an AVS
func (pds *ProtocolDataService) ListOperatorsForAvs(ctx context.Context, avs string, blockHeight uint64) ([]OperatorSet, error) {
	avs = strings.ToLower(avs)
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

	query := `
		select distinct operator
		from operator_avs_registration_snapshots
		where
			avs = @avs
			and snapshot <= @blockTime
	`

	var operatorSets []OperatorSet
	res := pds.db.Raw(query,
		sql.Named("avs", avs),
		sql.Named("blockTime", blockTime),
	).Scan(&operatorSets)
	if res.Error != nil {
		return nil, res.Error
	}

	return operatorSets, nil
}
