package slashingDataService

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperators"
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

func buildSlashingEventGroupingKey(se *slashedOperators.SlashedOperator) string {
	return fmt.Sprintf("%016x-%s-%s-%016x-%016x",
		se.OperatorSetId,
		se.Avs,
		se.TransactionHash,
		se.LogIndex,
		se.BlockNumber,
	)
}

func (sds *SlashingDataService) listSlashingEvents(ctx context.Context) ([]*SlashingEvent, error) {
	query := `
		select
			so.operator,
			so.strategy,
			so.wad_slashed,
			so.description
		from slashed_operators as so
		order by so.block_number desc
	`

	var slashingEvents []*slashedOperators.SlashedOperator
	res := sds.db.Raw(query).Scan(&slashingEvents)
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
			Strategy:   se.Strategy,
			WadSlashed: se.WadSlashed,
		})
	}
	// return just the values
	slashingEventsList := make([]*SlashingEvent, 0)
	for _, event := range mappedSlashingEvents {
		slashingEventsList = append(slashingEventsList, event)
	}
	return slashingEventsList, nil

}

func (sds *SlashingDataService) ListStakerSlashingHistory(
	ctx context.Context,
	stakerAddress string,
	blockHeight uint64,
) (interface{}, error) {
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
		)
		select
			so.operator,
			so.strategy,
			so.wad_slashed,
			so.description
		from windowed_staker_operators as wso
		left join slashed_operators as so on (
			so.operator = wso.operator
			and so.block_number >= wso.start_block
			and so.block_number <= wso.end_block
		)
		where so.operator is not null
		order by so.block_number desc
	`

	var slashingEvents []*slashedOperators.SlashedOperator
	res := sds.db.Raw(query).Scan(&slashingEvents)
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
			Strategy:   se.Strategy,
			WadSlashed: se.WadSlashed,
		})
	}
	// return just the values
	slashingEventsList := make([]*SlashingEvent, 0)
	for _, event := range mappedSlashingEvents {
		slashingEventsList = append(slashingEventsList, event)
	}
	return slashingEventsList, nil
}

func (sds *SlashingDataService) ListOperatorSlashingHistory(ctx context.Context, operator string) (interface{}, error) {

}

func (sds *SlashingDataService) ListAvsSlashingHistory(ctx context.Context, avs string) (interface{}, error) {

}

func (sds *SlashingDataService) ListAvsOperatorSetSlashingHistory(ctx context.Context, avs string, operatorSet string) (interface{}, error) {

}
