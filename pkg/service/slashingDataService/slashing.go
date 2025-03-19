package slashingDataService

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"github.com/Layr-Labs/sidecar/pkg/service/baseDataService"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SlashingDataService struct {
	baseDataService.BaseDataService
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config
}

func NewSlashingDataService(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) *SlashingDataService {
	return &SlashingDataService{
		db:           db,
		logger:       logger,
		globalConfig: globalConfig,
	}
}

type SlashingEvent struct {
	Description     string
	Operator        string
	TransactionHash string
	BlockNumber     uint64
	LogIndex        uint64
	Strategies      []*Strategy
	OperatorSet     *OperatorSet
	SlashedShares   string
}

type StakerSlashingEvent struct {
	Staker        string
	SlashingEvent *SlashingEvent
}

type OperatorSet struct {
	Id  uint64
	Avs string
}

type Strategy struct {
	Strategy           string
	WadSlashed         string
	TotalSharesSlashed string
}

const (
	HistoryFilter_Operator = iota
	HistoryFilter_Avs
	HistoryFilter_AvsOperatorSet

	HistoryFilter_OperatorKey    string = "operator"
	HistoryFilter_AvsKey         string = "avs"
	HistoryFilter_OperatorSetKey string = "operatorSet"
)

type SlashingHistoryFilter struct {
	FilterType   int
	FilterValues map[string]interface{}
}

func buildSlashingEventGroupingKey(se *SlashingEventRow) string {
	return fmt.Sprintf("%016x-%s-%s-%016x-%016x",
		se.OperatorSetId,
		se.Avs,
		se.TransactionHash,
		se.LogIndex,
		se.BlockNumber,
	)
}

type SlashingEventRow struct {
	slashedOperators.SlashedOperator
	SlashedShares string
}

func (sds *SlashingDataService) listSlashingEvents(ctx context.Context, filter *SlashingHistoryFilter, blockHeight uint64) ([]*SlashingEvent, error) {
	blockHeight, err := sds.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, errors.Wrapf(err, "listSlashingEvents: failed to get current block height")
	}

	query := `
		select
			so.operator,
			so.strategy,
			so.wad_slashed,
			so.description,
			so.operator_set_id,
			so.avs,
			so.block_number,
			so.transaction_hash,
			so.log_index
		{{- .additionalSelect }}
		from slashed_operators as so
		{{- .joinQuery }}
		where
		    so.block_number <= @blockHeight
	`

	templateArgs := map[string]interface{}{
		"additionalSelect": "",
		"joinQuery":        "",
	}
	args := []interface{}{
		sql.Named("blockHeight", blockHeight),
	}
	if filter != nil {
		switch filter.FilterType {
		case HistoryFilter_Operator:
			joinQuery := `
				left join slashed_operator_shares as sos on (
					sos.transaction_hash = so.transaction_hash
					and sos.block_number = so.block_number
					and sos.operator = so.operator
					and sos.strategy = so.strategy
				)
			`
			query = fmt.Sprintf("%s and so.operator = @operatorAddress", query)
			args = append(args, sql.Named("operatorAddress", filter.FilterValues[HistoryFilter_OperatorKey]))
			templateArgs["additionalSelect"] = ", sos.total_slashed_shares as slashed_shares"
			templateArgs["joinQuery"] = joinQuery
		case HistoryFilter_Avs:
			joinQuery := `
				left join slashed_operator_shares as sos on (
					sos.transaction_hash = so.transaction_hash
					and sos.block_number = so.block_number
					and sos.operator = so.operator
					and sos.strategy = so.strategy
				)
			`
			query = fmt.Sprintf("%s and so.avs = @avsAddress", query)
			args = append(args, sql.Named("avsAddress", filter.FilterValues[HistoryFilter_AvsKey]))
			templateArgs["additionalSelect"] = ", sos.total_slashed_shares as slashed_shares"
			templateArgs["joinQuery"] = joinQuery
		case HistoryFilter_AvsOperatorSet:
			joinQuery := `
				left join slashed_operator_shares as sos on (
					sos.transaction_hash = so.transaction_hash
					and sos.block_number = so.block_number
					and sos.operator = so.operator
					and sos.strategy = so.strategy
				)
			`

			query = fmt.Sprintf("%s and so.avs = @avsAddress and so.operator_set_id = @operatorSetId", query)
			args = append(args, sql.Named("avsAddress", filter.FilterValues[HistoryFilter_AvsKey]))
			args = append(args, sql.Named("operatorSetId", filter.FilterValues[HistoryFilter_OperatorSetKey]))
			templateArgs["additionalSelect"] = ", sos.total_slashed_shares as slashed_shares"
			templateArgs["joinQuery"] = joinQuery
		default:
			sds.logger.Sugar().Infow("listSlashingEvents: invalid filter type", zap.Int("filterType", filter.FilterType))
		}
	}

	query = fmt.Sprintf("%s order by so.block_number desc", query)

	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, templateArgs)
	if err != nil {
		sds.logger.Sugar().Errorw("listSlashingEvents: failed to render query", zap.Error(err))
		return nil, err
	}

	var slashingEvents []*SlashingEventRow
	res := sds.db.Raw(renderedQuery, args...).Scan(&slashingEvents)
	if res.Error != nil {
		return nil, errors.Wrapf(res.Error, "listSlashingEvents: failed to list slashing events")
	}

	mappedSlashingEvents := make(map[string]*SlashingEvent)
	for _, se := range slashingEvents {
		key := buildSlashingEventGroupingKey(se)
		event, ok := mappedSlashingEvents[key]
		if !ok {
			event = &SlashingEvent{
				Description:     se.Description,
				Operator:        se.Operator,
				TransactionHash: se.TransactionHash,
				BlockNumber:     se.BlockNumber,
				LogIndex:        se.LogIndex,
				Strategies:      make([]*Strategy, 0),
				OperatorSet: &OperatorSet{
					Avs: se.Avs,
					Id:  se.OperatorSetId,
				},
			}
			mappedSlashingEvents[key] = event
		}
		event.Strategies = append(event.Strategies, &Strategy{
			Strategy:           se.Strategy,
			WadSlashed:         se.WadSlashed,
			TotalSharesSlashed: se.SlashedShares,
		})
	}
	// return just the values
	slashingEventsList := make([]*SlashingEvent, 0)
	for _, event := range mappedSlashingEvents {
		slashingEventsList = append(slashingEventsList, event)
	}
	return slashingEventsList, nil
}

type SlashedStakerRow struct {
	Operator        string
	Strategy        string
	WadSlashed      string
	Description     string
	TransactionHash string
	OperatorSetId   uint64
	Avs             string
	BlockNumber     uint64
	SlashedShares   string
	LogIndex        uint64
}

func buildStakerSlashingEventGroupingKey(se *SlashedStakerRow) string {
	return fmt.Sprintf("%016x-%s-%s-%016x-%016x",
		se.OperatorSetId,
		se.Avs,
		se.TransactionHash,
		se.LogIndex,
		se.BlockNumber,
	)
}

func (sds *SlashingDataService) ListStakerSlashingHistory(
	ctx context.Context,
	stakerAddress string,
	blockHeight uint64,
) ([]*SlashingEvent, error) {
	blockHeight, err := sds.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, errors.Wrapf(err, "listStakerSlashingHistory: failed to get current block height")
	}

	query := `
	with windowed_staker_operators as (
		with staker_delegations_with_block_info as (
			select
				sdc.staker,
				case when sdc.delegated = false then '0x0000000000000000000000000000000000000000' else sdc.operator end as operator,
				sdc.log_index,
				sdc.block_number
			from staker_delegation_changes as sdc
			where
				staker = @stakerAddress
				and block_number <= @blockHeight
		),
		ranked_delegations as (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY staker, block_number ORDER BY block_number DESC, log_index DESC) AS rn
			FROM staker_delegations_with_block_info
		),
		snapshotted_records as (
			SELECT
			 staker,
			 operator,
			 block_number
			from ranked_delegations
			where rn = 1
		),
		staker_delegation_windows as (
		 SELECT
			 staker, operator, block_number as start_block,
			 CASE
				 WHEN LEAD(block_number) OVER (PARTITION BY staker ORDER BY block_number) is null THEN @blockHeight
				 ELSE LEAD(block_number - 1) OVER (PARTITION BY staker ORDER BY block_number)
				 END AS end_block
		 FROM snapshotted_records
		),
		cleaned_records as (
			SELECT * FROM staker_delegation_windows
			WHERE start_block < end_block
		)
		select * from cleaned_records
		where operator != '0x0000000000000000000000000000000000000000'
	),
	windowed_staker_strategies as (
		WITH ranked_staker_records as (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY staker, strategy, block_number ORDER BY block_number DESC, log_index DESC) AS rn
			FROM staker_shares
			where
				staker = @stakerAddress
				and block_number <= @blockHeight
		),
		snapshotted_records as (
		 SELECT *
		 from ranked_staker_records
		 where rn = 1
		),
		staker_share_windows as (
			SELECT
				staker,
				strategy,
				shares,
				block_number as start_block,
				CASE
					WHEN LEAD(block_number) OVER (PARTITION BY staker, strategy ORDER BY block_number) is null THEN @blockHeight
					ELSE LEAD(block_number - 1) OVER (PARTITION BY staker, strategy ORDER BY block_number)
				END AS end_block
			FROM snapshotted_records
		),
		cleaned_records as (
			SELECT * FROM staker_share_windows
			WHERE start_block < end_block
		)
		select * from cleaned_records
	)
	select
		soo.operator,
		soo.strategy,
		soo.wad_slashed,
		soo.description,
		soo.transaction_hash,
		soo.log_index,
		soo.operator_set_id,
		soo.avs,
		soo.block_number,
		ssd.shares as slashed_shares
	from windowed_staker_operators as wso
	left join (
		select
			so.*
		from slashed_operators as so
		left join windowed_staker_strategies as wss on (
			so.strategy = wss.strategy
			and so.block_number >= wss.start_block
			and so.block_number <= wss.end_block
		)
		where wss.strategy is not null
	) as soo on (
		soo.operator = wso.operator
		and soo.block_number >= wso.start_block
		and soo.block_number <= wso.end_block
	)
	left join staker_share_deltas as ssd on (
		ssd.staker = wso.staker
		and ssd.transaction_hash = soo.transaction_hash
		and ssd.log_index = soo.log_index
		and ssd.strategy = soo.strategy
		and ssd.strategy_index = 0
	)
	where soo.operator is not null
	order by soo.block_number desc
	`

	var slashingEvents []*SlashedStakerRow
	res := sds.db.Raw(query,
		sql.Named("stakerAddress", stakerAddress),
		sql.Named("blockHeight", blockHeight),
	).Scan(&slashingEvents)
	if res.Error != nil {
		return nil, errors.Wrapf(res.Error, "listSlashingEvents: failed to list slashing events")
	}

	mappedSlashingEvents := make(map[string]*SlashingEvent)
	for _, se := range slashingEvents {
		key := buildStakerSlashingEventGroupingKey(se)
		event, ok := mappedSlashingEvents[key]
		if !ok {
			event = &SlashingEvent{
				Description:     se.Description,
				Operator:        se.Operator,
				TransactionHash: se.TransactionHash,
				BlockNumber:     se.BlockNumber,
				LogIndex:        se.LogIndex,
				Strategies:      make([]*Strategy, 0),
				OperatorSet: &OperatorSet{
					Avs: se.Avs,
					Id:  se.OperatorSetId,
				},
			}
			mappedSlashingEvents[key] = event
		}
		event.Strategies = append(event.Strategies, &Strategy{
			Strategy:           se.Strategy,
			WadSlashed:         se.WadSlashed,
			TotalSharesSlashed: se.SlashedShares,
		})
	}
	// return just the values
	slashingEventsList := make([]*SlashingEvent, 0)
	for _, event := range mappedSlashingEvents {
		slashingEventsList = append(slashingEventsList, event)
	}
	return slashingEventsList, nil
}

func (sds *SlashingDataService) ListOperatorSlashingHistory(ctx context.Context, operator string, blockHeight uint64) ([]*SlashingEvent, error) {
	return sds.listSlashingEvents(ctx, &SlashingHistoryFilter{
		FilterType:   HistoryFilter_Operator,
		FilterValues: map[string]interface{}{HistoryFilter_OperatorKey: operator},
	}, blockHeight)
}

func (sds *SlashingDataService) ListAvsSlashingHistory(ctx context.Context, avs string, blockHeight uint64) ([]*SlashingEvent, error) {
	return sds.listSlashingEvents(ctx, &SlashingHistoryFilter{
		FilterType:   HistoryFilter_Avs,
		FilterValues: map[string]interface{}{HistoryFilter_AvsKey: avs},
	}, blockHeight)
}

func (sds *SlashingDataService) ListAvsOperatorSetSlashingHistory(ctx context.Context, avs string, operatorSetId uint64, blockHeight uint64) ([]*SlashingEvent, error) {
	return sds.listSlashingEvents(ctx, &SlashingHistoryFilter{
		FilterType: HistoryFilter_AvsOperatorSet,
		FilterValues: map[string]interface{}{
			HistoryFilter_AvsKey:         avs,
			HistoryFilter_OperatorSetKey: operatorSetId,
		},
	}, blockHeight)
}
