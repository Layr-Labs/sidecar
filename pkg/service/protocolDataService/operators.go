package protocolDataService

import (
	"context"
	"database/sql"
	"strings"
)

type OperatorSet struct {
	Avs           string
	OperatorSetId uint32
}

// ListOperatorsForStaker returns operators that a staker has delegated to
func (pds *ProtocolDataService) ListOperatorsForStaker(ctx context.Context, staker string, blockHeight uint64) ([]string, error) {
	staker = strings.ToLower(staker)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		select distinct operator
		from staker_delegation_snapshots
		where
			staker = @staker
			and snapshot <= (select block_time from blocks where number = @blockHeight)
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

	// Then use the block time to filter staker_operator
	query := `
		select distinct operator
		from operator_shares
		where
			strategy = @strategy
			and block_number <= @blockHeight
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
		select distinct operator
		from operator_avs_registration_snapshots
		where
			avs = @avs
			and snapshot <= (select block_time from blocks where number = @blockHeight)
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

// ListOperatorSets returns all existing operator sets
func (pds *ProtocolDataService) ListOperatorSets(ctx context.Context) ([]OperatorSet, error) {
	query := `
		select distinct operator_set_id, avs
		from operator_sets
		order by operator_set_id, avs
	`

	var operatorSets []OperatorSet
	res := pds.db.Raw(query).Scan(&operatorSets)
	if res.Error != nil {
		return nil, res.Error
	}

	return operatorSets, nil
}
