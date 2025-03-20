package protocolDataService

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"github.com/Layr-Labs/sidecar/pkg/utils"
)

type TokenList []string

func (tl *TokenList) Value() (driver.Value, error) {
	return json.Marshal(tl)
}

func (tl *TokenList) Scan(value interface{}) error {
	if value == nil {
		*tl = TokenList{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSONB value: %v", value)
	}

	return json.Unmarshal(bytes, tl)
}

type Strategy struct {
	Strategy     string
	TotalStaked  string
	RewardTokens TokenList
}

func (pds *ProtocolDataService) ListStrategies(ctx context.Context, strategyAddresses []string, blockHeight uint64) ([]*Strategy, error) {
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		with distinct_strategies as (
			select
				ssd.strategy,
				sum(ssd.shares) as shares
			from staker_share_deltas as ssd
			where block_number <= @blockNumber
			group by 1
			order by 1
		),
		reward_submission_tokens as (
			select
				ds.strategy,
				ds.shares,
				rs.token
			from distinct_strategies as ds
			left join reward_submissions as rs on (
				rs.strategy = ds.strategy
				and rs.block_number <= @blockNumber
			)
		),
		od_reward_tokens as (
			select
				ds.strategy,
				ds.shares,
				rs.token
			from distinct_strategies as ds
			left join operator_directed_reward_submissions as rs on (
				rs.strategy = ds.strategy
				and rs.block_number <= @blockNumber
			)
		),
		odos_reward_tokens as (
			select
				ds.strategy,
				ds.shares,
				rs.token
			from distinct_strategies as ds
			left join operator_directed_operator_set_reward_submissions as rs on (
				rs.strategy = ds.strategy
				and rs.block_number <= @blockNumber
			)
		)
		select
			ds.strategy,
			ds.shares as total_staked,
			coalesce((
				select jsonb_agg(distinct token)
				from (
					select jsonb_array_elements(rst.avs_tokens) as token
					union
					select jsonb_array_elements(odrt.avs_tokens) as token
					union
					select jsonb_array_elements(odosrt.avs_tokens) as token
				) t
				WHERE token IS NOT NULL AND token <> 'null'::jsonb
			), '[]'::jsonb) as reward_tokens
		from distinct_strategies as ds
		left join lateral (
			select
				jsonb_agg(distinct token) as avs_tokens
			from reward_submission_tokens as rst
			where rst.strategy = ds.strategy
		) as rst on true
		left join lateral (
			select
				jsonb_agg(distinct token) as avs_tokens
			from od_reward_tokens as odrt
			where odrt.strategy = ds.strategy
		) as odrt on true
		left join lateral (
			select
				jsonb_agg(distinct token) as avs_tokens
			from odos_reward_tokens as odosrt
			where odosrt.strategy = ds.strategy
		) as odosrt on true
	`

	args := []interface{}{sql.Named("blockNumber", blockHeight)}
	if len(strategyAddresses) > 0 {
		query += " where ds.strategy in @strategies"
		args = append(args, sql.Named("strategies", strategyAddresses))
	}

	var strategies []*Strategy
	res := pds.DB.Raw(query, args...).Scan(&strategies)
	if res.Error != nil {
		return nil, res.Error
	}
	return strategies, nil
}

type DetailedStakerStrategy struct {
	Amount   string
	Strategy *Strategy
}

type StakerStrategy struct {
	Strategy string
	Amount   string
}

func (pds *ProtocolDataService) ListStakerStrategies(
	ctx context.Context,
	staker string,
	blockHeight uint64,
	strategyAddresses []string,
) ([]*DetailedStakerStrategy, error) {
	blockNumber, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}
	query := `
		select
			ssd.strategy,
			sum(ssd.shares) as shares
		from staker_share_deltas as ssd
		where
			ssd.staker= @staker
			and ssd.block_number <= @blockNumber
		{{ if .strategy }}
			and ssd.strategy in @strategies
		{{ end }}
		group by 1
		order by 1
	`

	args := []interface{}{
		sql.Named("staker", staker),
		sql.Named("blockNumber", blockNumber),
	}
	renderArgs := map[string]interface{}{}
	if len(strategyAddresses) > 0 {
		args = append(args, sql.Named("strategies", strategyAddresses))
		renderArgs["strategy"] = true
	}

	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, renderArgs)
	if err != nil {
		return nil, err
	}

	var strategies []*StakerStrategy
	res := pds.DB.Raw(renderedQuery, args...).Scan(&strategies)
	if res.Error != nil {
		return nil, res.Error
	}

	foundStrategyArgs := utils.Map(strategies, func(strategy *StakerStrategy, i uint64) string {
		return strategy.Strategy
	})

	strategyDetails, err := pds.ListStrategies(ctx, foundStrategyArgs, blockHeight)
	if err != nil {
		return nil, err
	}

	detailedStrategies := utils.Map(strategies, func(strategy *StakerStrategy, i uint64) *DetailedStakerStrategy {
		return &DetailedStakerStrategy{
			Amount: strategy.Amount,
			Strategy: utils.Find(strategyDetails, func(s *Strategy) bool {
				return s.Strategy == strategy.Strategy
			}),
		}
	})

	return detailedStrategies, nil

}

func (pds *ProtocolDataService) GetStrategyForStaker(
	ctx context.Context,
	staker string,
	strategy string,
	blockHeight uint64,
) (*DetailedStakerStrategy, error) {
	strategies, err := pds.ListStakerStrategies(ctx, staker, blockHeight, []string{strategy})
	if err != nil {
		return nil, err
	}
	if len(strategies) == 0 {
		return nil, fmt.Errorf("Strategy %s not found for staker %s", strategy, staker)
	}
	return strategies[0], nil
}
