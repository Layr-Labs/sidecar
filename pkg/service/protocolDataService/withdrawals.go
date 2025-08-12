package protocolDataService

import (
	"context"
	"database/sql"
	"slices"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/queuedSlashingWithdrawals"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
)

type StakerQueuedWithdrawal struct {
	queuedSlashingWithdrawals.QueuedSlashingWithdrawal
	Completed bool
}

type WithdrawalFilters struct {
	Staker     string
	Strategies []string
}

func (pds *ProtocolDataService) ListQueuedWithdrawals(
	ctx context.Context,
	filters WithdrawalFilters,
	blockHeight uint64,
) (
	[]*StakerQueuedWithdrawal,
	error,
) {
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		select
			qsw.*,
			case when csw.withdrawal_root is not null then true else false end as completed
		from queued_slashing_withdrawals as qsw
		left join completed_slashing_withdrawals as csw on (
		    qsw.withdrawal_root = csw.withdrawal_root
		)
		where
		    qsw.block_number <= @blockHeight
		{{ if .filterStaker }}
			and qsw.staker = @stakerAddress
		{{ end }}
		{{ if .filterStrategies }}
		 	and qsw.strategy in @strategies
		{{ end }}
		order by qsw.block_number desc
	`

	templateArgs := map[string]interface{}{
		"filterStaker":     false,
		"filterStrategies": false,
	}
	queryArgs := []interface{}{sql.Named("blockHeight", blockHeight)}

	if filters.Staker != "" {
		templateArgs["filterStaker"] = true
		queryArgs = append(queryArgs, sql.Named("stakerAddress", filters.Staker))
	}

	if len(filters.Strategies) > 0 {
		templateArgs["filterStrategies"] = true
		queryArgs = append(queryArgs, sql.Named("strategies", filters.Strategies))
	}

	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, templateArgs)
	if err != nil {
		return nil, err
	}

	var withdrawals []*StakerQueuedWithdrawal
	res := pds.db.Raw(renderedQuery, queryArgs...).Scan(&withdrawals)

	if res.Error != nil {
		return nil, res.Error
	}
	return withdrawals, nil
}

func (pds *ProtocolDataService) ListOperatorQueuedWithdrawals(
	ctx context.Context,
	operator string,
	blockHeight uint64,
) ([]*StakerQueuedWithdrawal, error) {
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	strategies, err := pds.ListDelegatedStrategiesForOperator(ctx, operator, blockHeight)
	if err != nil {
		return nil, err
	}

	if len(strategies) == 0 {
		return []*StakerQueuedWithdrawal{}, nil
	}

	return pds.ListQueuedWithdrawals(ctx, WithdrawalFilters{
		Strategies: strategies,
	}, blockHeight)
}

func (pds *ProtocolDataService) ListOperatorQueuedWithdrawalsForStrategy(
	ctx context.Context,
	operator string,
	strategy string,
	blockHeight uint64,
) ([]*StakerQueuedWithdrawal, error) {
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	strategies, err := pds.ListDelegatedStrategiesForOperator(ctx, operator, blockHeight)
	if err != nil {
		return nil, err
	}

	// strategy is not currently delegated to operator
	if !slices.Contains(strategies, strategy) {
		return []*StakerQueuedWithdrawal{}, nil
	}

	return pds.ListQueuedWithdrawals(ctx, WithdrawalFilters{
		Strategies: []string{strategy},
	}, blockHeight)
}
