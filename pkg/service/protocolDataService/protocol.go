package protocolDataService

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/service/baseDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ProtocolDataService struct {
	baseDataService.BaseDataService
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config
	stateManager *stateManager.EigenStateManager
}

func NewProtocolDataService(
	sm *stateManager.EigenStateManager,
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) *ProtocolDataService {
	return &ProtocolDataService{
		BaseDataService: baseDataService.BaseDataService{
			DB: db,
		},
		stateManager: sm,
		db:           db,
		logger:       logger,
		globalConfig: globalConfig,
	}
}

func (pds *ProtocolDataService) ListRegisteredAVSsForOperator(ctx context.Context, operator string, blockHeight uint64) ([]string, error) {
	operator = strings.ToLower(operator)

	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		with ranked_operators as (
			select
				aosc.operator,
				aosc.avs,
				aosc.registered,
				row_number() over (partition by aosc.operator order by aosc.block_number desc, aosc.log_index asc) as rn
			from avs_operator_state_changes as aosc
			where
				operator = @operator
				and block_number <= @blockHeight
		)
		select
			distinct ro.avs as avs
		from ranked_operators as ro
		where
			ro.rn = 1
			and ro.registered = true
	`
	var avsAddresses []string
	res := pds.db.Raw(query,
		sql.Named("operator", operator),
		sql.Named("blockHeight", blockHeight),
	).Scan(&avsAddresses)

	if res.Error != nil {
		return nil, res.Error
	}
	return avsAddresses, nil
}

func (pds *ProtocolDataService) ListDelegatedStrategiesForOperator(ctx context.Context, operator string, blockHeight uint64) ([]string, error) {
	operator = strings.ToLower(operator)
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		with operator_stakers as (
			select distinct on (staker)
					staker,
					block_number,
					delegated
			from staker_delegation_changes
			where
					operator = @operator
					and block_number <= @blockHeight
			order by staker, block_number desc, log_index asc
		),
		delegated_stakers as (
			select
					os.staker,
					os.block_number,
					ssd.shares,
					ssd.strategy
			from operator_stakers as os
			left join staker_share_deltas as ssd
					on ssd.staker = os.staker
			where
				os.delegated = true
		),
		strategy_shares as (
			select
					ss.strategy,
					sum(ss.shares) as shares
			from delegated_stakers as ss
			group by 1
		)
		select
				strategy
		from strategy_shares
		where shares > 0;
	`

	var strategies []string
	res := pds.db.Raw(query,
		sql.Named("operator", operator),
		sql.Named("blockHeight", blockHeight),
	).Scan(&strategies)

	if res.Error != nil {
		return nil, res.Error
	}
	return strategies, nil
}

// GetTotalDelegatedOperatorSharesForStrategy returns the shares delegated to operators for a given strategy at a given block height.
// If operator is empty, returns all operators. If operator is specified, returns just that operator.
func (pds *ProtocolDataService) GetTotalDelegatedOperatorSharesForStrategy(ctx context.Context, operator string, strategy string, blockHeight uint64, pagination *types.Pagination) ([]*OperatorDelegatedStake, error) {
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	if pagination == nil {
		pagination = &types.Pagination{
			Page:     0,
			PageSize: 100,
		}
	}

	queryTemplate := `
		with operator_stakers as (
			select
				staker,
				operator,
				delegated,
				block_number,
				log_index,
				row_number() over (partition by staker order by block_number desc, log_index asc) as rn
			from staker_delegation_changes
			where
				block_number <= @blockHeight
				{{if .HasOperator}}and operator = @operator{{end}}
			order by block_number desc, log_index desc
		),
		distinct_delegated_stakers as (
			select
				distinct staker,
				operator,
				log_index
			from operator_stakers as os
			where
				os.rn = 1
				and os.delegated = true
		),
		summed_staker_shares as (
			select
				ssd.staker,
				ssd.strategy,
				sum(shares) as shares
			from staker_share_deltas as ssd
			where
				ssd.staker in (select staker from distinct_delegated_stakers)
				and ssd.block_number <= @blockHeight
		   group by 1, 2
		),
		delegated_staker_shares as (
			select
				dds.*,
				ss.shares,
				ss.strategy
			from distinct_delegated_stakers as dds
			join summed_staker_shares as ss on (
				ss.staker = dds.staker
			)
		),
		final_results as (
			select
				operator,
				sum(shares) as shares
			from delegated_staker_shares as dss
			where dss.strategy = @strategy
			group by 1
		)
		select
			operator,
			shares
		from final_results
		where shares > 0
		order by shares::numeric desc
		{{if .HasPagination}}LIMIT @limit{{if .HasOffset}} OFFSET @offset{{end}}{{end}};
	`

	// Template data for conditional rendering
	templateData := struct {
		HasOperator   bool
		HasPagination bool
		HasOffset     bool
	}{
		HasOperator:   operator != "",
		HasPagination: pagination != nil,
		HasOffset:     pagination != nil && pagination.Page > 0,
	}

	tmpl, err := template.New("delegatedStakesQuery").Parse(queryTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query template: %w", err)
	}

	var queryBuffer bytes.Buffer
	if err := tmpl.Execute(&queryBuffer, templateData); err != nil {
		return nil, fmt.Errorf("failed to execute query template: %w", err)
	}

	query := queryBuffer.String()

	queryParams := []interface{}{
		sql.Named("strategy", strings.ToLower(strategy)),
		sql.Named("blockHeight", blockHeight),
	}

	if operator != "" {
		queryParams = append(queryParams, sql.Named("operator", strings.ToLower(operator)))
	}

	if pagination != nil {
		queryParams = append(queryParams, sql.Named("limit", pagination.PageSize))
		if pagination.Page > 0 {
			queryParams = append(queryParams, sql.Named("offset", pagination.Page*pagination.PageSize))
		}
	}

	type result struct {
		Operator string `gorm:"column:operator"`
		Shares   string `gorm:"column:shares"`
	}

	var results []result
	res := pds.db.Raw(query, queryParams...).Scan(&results)

	if res.Error != nil {
		return nil, res.Error
	}

	if len(results) == 0 {
		return []*OperatorDelegatedStake{}, nil
	}

	operatorStakes := make([]*OperatorDelegatedStake, len(results))
	for i, result := range results {
		operatorStakes[i] = &OperatorDelegatedStake{
			Operator: result.Operator,
			Shares:   result.Shares,
		}
	}

	return operatorStakes, nil
}

type OperatorDelegatedStake struct {
	Operator     string
	Shares       string
	AvsAddresses []string
}

type ResultCollector[T any] struct {
	Result T
	Error  error
}

func (pds *ProtocolDataService) GetOperatorDelegatedStake(ctx context.Context, operator string, strategy string, blockHeight uint64) (*OperatorDelegatedStake, error) {
	blockHeight, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	sharesChan := make(chan *ResultCollector[string], 1)
	avsChan := make(chan *ResultCollector[[]string], 1)

	wg.Add(2)

	go func() {
		defer wg.Done()
		result := &ResultCollector[string]{}

		operatorShares, err := pds.GetTotalDelegatedOperatorSharesForStrategy(ctx, operator, strategy, blockHeight, nil)
		if err != nil {
			result.Error = err
		} else {
			// For single operator queries, return the shares of the first (and only) result
			if len(operatorShares) > 0 {
				result.Result = operatorShares[0].Shares
			} else {
				result.Result = "0"
			}
		}
		sharesChan <- result
	}()

	go func() {
		defer wg.Done()
		result := &ResultCollector[[]string]{}

		avsAddresses, err := pds.ListRegisteredAVSsForOperator(ctx, operator, blockHeight)
		if err != nil {
			result.Error = err
		} else {
			result.Result = avsAddresses
		}
		avsChan <- result
	}()
	wg.Wait()
	close(sharesChan)
	close(avsChan)

	shares := <-sharesChan
	if shares.Error != nil {
		pds.logger.Sugar().Errorw("Failed to get operator delegated stake",
			zap.String("operator", operator),
			zap.String("strategy", strategy),
			zap.Uint64("blockHeight", blockHeight),
			zap.Error(shares.Error),
		)
		return nil, shares.Error
	}

	registeredAvss := <-avsChan
	if registeredAvss.Error != nil {
		pds.logger.Sugar().Errorw("Failed to get registered AVSs for operator",
			zap.String("operator", operator),
			zap.String("strategy", strategy),
			zap.Uint64("blockHeight", blockHeight),
			zap.Error(registeredAvss.Error),
		)
		return nil, registeredAvss.Error
	}

	return &OperatorDelegatedStake{
		Shares:       shares.Result,
		AvsAddresses: registeredAvss.Result,
	}, nil
}

func (pds *ProtocolDataService) ListDelegatedStakersForOperator(ctx context.Context, operator string, blockHeight uint64, pagination *types.Pagination) ([]string, error) {
	bh, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		with staker_operator_delegations as (
			SELECT DISTINCT ON (staker)
				staker,
				operator,
				delegated
			FROM staker_delegation_changes
			WHERE operator = @operator
				AND block_number <= @blockHeight
			ORDER BY staker, block_number desc, log_index asc
		)
		SELECT
			sod.staker
		from staker_operator_delegations as sod
		where sod.delegated = true
	`

	queryParams := []interface{}{
		sql.Named("operator", operator),
		sql.Named("blockHeight", bh),
	}

	if pagination != nil {
		query += ` LIMIT @limit`
		queryParams = append(queryParams, sql.Named("limit", pagination.PageSize))

		if pagination.Page > 0 {
			query += ` OFFSET @offset`
			queryParams = append(queryParams, sql.Named("offset", pagination.Page*pagination.PageSize))
		}
	}

	var stakers []string
	res := pds.db.Raw(query, queryParams...).Scan(&stakers)
	if res.Error != nil {
		return nil, res.Error
	}
	return stakers, nil
}

type AvsAddresses []string

func (aa *AvsAddresses) Value() (driver.Value, error) {
	return json.Marshal(aa)
}

func (aa *AvsAddresses) Scan(value interface{}) error {
	if value == nil {
		*aa = AvsAddresses{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSONB value: %v", value)
	}

	return json.Unmarshal(bytes, aa)
}

type StakerShares struct {
	Staker       string
	Strategy     string
	Shares       string
	BlockHeight  uint64
	Operator     *string
	Delegated    bool
	AvsAddresses AvsAddresses `gorm:"type:jsonb"`
}

// ListStakerShares returns the shares of a staker at a given block height, including the operator they were delegated to
// and the addresses of the AVSs the operator was registered to.
//
// If not blockHeight is provided, the most recently indexed block will be used.
func (pds *ProtocolDataService) ListStakerShares(ctx context.Context, staker string, blockHeight uint64) ([]*StakerShares, error) {

	bh, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		with distinct_staker_strategies as (
			select
				ssd.staker,
				ssd.strategy,
				sum(ssd.shares) as shares
			from staker_share_deltas as ssd
			where
				ssd.staker = @staker
				and block_number <= @blockHeight
			group by ssd.staker, ssd.strategy
		)
		select
			dss.*,
			dsc.operator,
			dsc.delegated,
			coalesce(aosc.avs_list, '[]'::jsonb) as avs_addresses
		from distinct_staker_strategies as dss
		left join lateral (
			select
				sdc.staker,
				sdc.operator,
				sdc.delegated,
				row_number() over (partition by sdc.staker order by sdc.block_number desc, sdc.log_index) as rn
			from staker_delegation_changes as sdc
			where
				sdc.staker = dss.staker
				and sdc.block_number <= @blockHeight
			order by block_number desc
		) as dsc on (dsc.rn = 1)
		left join lateral (
			select
				jsonb_agg(distinct aosc.avs) as avs_list
			from avs_operator_state_changes aosc
			where
				aosc.operator = dsc.operator
				and aosc.block_number <= @blockHeight
				and aosc.registered = true
		) as aosc on true
	`
	shares := make([]*StakerShares, 0)
	res := pds.db.Raw(query,
		sql.Named("staker", staker),
		sql.Named("blockHeight", bh),
	).Scan(&shares)
	if res.Error != nil {
		return nil, res.Error
	}
	return shares, nil
}

type Withdrawal struct {
	Staker      string
	Strategy    string
	Shares      string
	Operator    string
	BlockNumber uint64
}

func (pds *ProtocolDataService) ListWithdrawalsForStrategies(ctx context.Context, strategies []string, blockHeight uint64, pagination *types.Pagination) ([]*Withdrawal, error) {
	bh, err := pds.BaseDataService.GetCurrentBlockHeightIfNotPresent(ctx, blockHeight)
	if err != nil {
		return nil, err
	}

	query := `
		WITH events AS (
			SELECT event_name, output_data, block_number, log_index
			FROM transaction_logs
			WHERE event_name IN ('WithdrawalQueued', 'WithdrawalCompleted', 'SlashingWithdrawalCompleted', 'MaxMagnitudeUpdated')
				AND block_number <= @blockHeight
		),

		completed_roots AS (
			SELECT output_data->>'withdrawalRoot' as withdrawal_root
			FROM events 
			WHERE event_name IN ('WithdrawalCompleted', 'SlashingWithdrawalCompleted')
			UNION
			SELECT withdrawal_root FROM completed_slashing_withdrawals
			WHERE block_number <= @blockHeight
		),

		queued_regular AS (
			SELECT 
				output_data->>'withdrawalRoot' as withdrawal_root,
				output_data->'withdrawal'->>'staker' as staker,
				strategy,
				shares::numeric,
				block_number,
				output_data->'withdrawal'->>'delegatedTo' as operator
			FROM events,
			LATERAL (
				SELECT 
					jsonb_array_elements_text(output_data->'withdrawal'->'strategies') AS strategy,
					jsonb_array_elements_text(output_data->'withdrawal'->'shares') AS shares
			) AS expanded
			WHERE event_name = 'WithdrawalQueued'
				AND strategy IN @strategies
				AND output_data->>'withdrawalRoot' NOT IN (SELECT withdrawal_root FROM completed_roots)
		),

		queued_slashing AS (
			SELECT 
				withdrawal_root,
				staker,
				strategy,
				scaled_shares::numeric as shares,
				block_number,
				operator
			FROM queued_slashing_withdrawals
			WHERE strategy IN @strategies
				AND withdrawal_root NOT IN (SELECT withdrawal_root FROM completed_roots)
		),

		-- Get all magnitude events for slashing withdrawals
		magnitude_events AS (
			SELECT 
				output_data->>'operator' as operator,
				output_data->>'strategy' as strategy,
				(output_data->>'maxMagnitude')::numeric as max_magnitude,
				block_number,
				log_index
			FROM events
			WHERE event_name = 'MaxMagnitudeUpdated'
		),

		-- Cross join slashing withdrawals with magnitude events and find the right one
		slashing_with_magnitudes AS (
			SELECT 
				qs.withdrawal_root,
				qs.staker,
				qs.strategy,
				qs.shares,
				qs.block_number,
				qs.operator,
				me.max_magnitude,
				me.block_number as mag_block,
				me.log_index as mag_log,
				-- Determine which block to check magnitude at
				CASE 
					WHEN qs.block_number + @withdrawalDelayBlocks < @blockHeight 
					THEN qs.block_number + @withdrawalDelayBlocks
					ELSE @blockHeight
				END as magnitude_block_threshold,
				ROW_NUMBER() OVER (
					PARTITION BY qs.withdrawal_root 
					ORDER BY me.block_number DESC, me.log_index DESC
				) as rn
			FROM queued_slashing qs
			LEFT JOIN magnitude_events me ON (
				me.operator = qs.operator 
				AND me.strategy = qs.strategy 
				AND me.block_number <= CASE 
					WHEN qs.block_number + @withdrawalDelayBlocks < @blockHeight 
					THEN qs.block_number + @withdrawalDelayBlocks
					ELSE @blockHeight
				END
			)
		),

		-- Take the most recent magnitude for each withdrawal
		slashing_magnitudes AS (
			SELECT 
				withdrawal_root,
				COALESCE(max_magnitude, 1e18) as max_magnitude
			FROM slashing_with_magnitudes
			WHERE rn = 1
		)

		SELECT 
			q.staker,
			q.strategy,
			CASE 
				WHEN qs.withdrawal_root IS NOT NULL 
				THEN q.shares * sm.max_magnitude / 1e18
				ELSE q.shares
			END AS shares,
			q.operator,
			q.block_number
		FROM (
			SELECT * FROM queued_regular
			UNION ALL
			SELECT * FROM queued_slashing
		) q
		LEFT JOIN queued_slashing qs USING (withdrawal_root)
		LEFT JOIN slashing_magnitudes sm ON qs.withdrawal_root = sm.withdrawal_root
		ORDER BY q.block_number DESC
	`

	// Default withdrawal delay is 100800 blocks (~14 days on mainnet)
	// This is used to calculate when withdrawals become completable
	const defaultWithdrawalDelayBlocks = 100800

	queryParams := []interface{}{
		sql.Named("strategies", strategies),
		sql.Named("blockHeight", bh),
		sql.Named("withdrawalDelayBlocks", defaultWithdrawalDelayBlocks),
	}

	if pagination != nil {
		query += ` LIMIT @limit`
		queryParams = append(queryParams, sql.Named("limit", pagination.PageSize))

		if pagination.Page > 0 {
			query += ` OFFSET @offset`
			queryParams = append(queryParams, sql.Named("offset", pagination.Page*pagination.PageSize))
		}
	}

	var withdrawals []*Withdrawal
	res := pds.db.Raw(query, queryParams...).Scan(&withdrawals)
	if res.Error != nil {
		return nil, res.Error
	}

	return withdrawals, nil
}

type KeyRotationScheduled struct {
	Avs             string
	OperatorSetId   uint32
	Operator        string
	CurveType       string
	OldPubkey       string
	NewPubkey       string
	ActivateAt      uint64
	TransactionHash string
	BlockNumber     uint64
	LogIndex        uint64
}

func (pds *ProtocolDataService) GetPendingKeyRotations(ctx context.Context, minActivateAt uint64, maxActivateAt uint64) ([]*KeyRotationScheduled, error) {
	query := pds.db.Table("key_rotation_scheduled")

	// Get current block to determine current timestamp
	currentBlock, err := pds.GetCurrentBlockHeight(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get current block height: %w", err)
	}
	if currentBlock == nil {
		return nil, fmt.Errorf("no current block found")
	}

	// Only return rotations where activate_at is in the future (greater than current block time)
	currentTimestamp := uint64(currentBlock.BlockTime.Unix())
	query = query.Where("activate_at > ?", currentTimestamp)

	if minActivateAt > 0 {
		query = query.Where("activate_at >= ?", minActivateAt)
	}
	if maxActivateAt > 0 {
		query = query.Where("activate_at <= ?", maxActivateAt)
	}

	var rotations []*KeyRotationScheduled
	res := query.Order("activate_at ASC, block_number ASC, log_index ASC").Find(&rotations)
	if res.Error != nil {
		return nil, res.Error
	}

	return rotations, nil
}

func (pds *ProtocolDataService) GetStateRoot(ctx context.Context, blockHeight uint64) (*stateManager.StateRoot, error) {
	var stateRoot *stateManager.StateRoot

	query := pds.db.Model(&stateRoot)
	if blockHeight > 0 {
		query = query.Where("eth_block_number = ?", blockHeight)
	} else {
		query = query.Order("eth_block_number desc")
	}

	res := query.First(&stateRoot)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return stateRoot, nil
}

func (pds *ProtocolDataService) GetCurrentConfirmedBlockHeight(ctx context.Context) (*storage.Block, error) {
	stateRoot, err := pds.GetStateRoot(ctx, 0)
	if err != nil {
		return nil, err
	}

	if stateRoot == nil {
		return nil, errors.New("no state root found")
	}

	var block *storage.Block
	res := pds.db.Model(&block).Where("number = ?", stateRoot.EthBlockNumber).First(&block)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return block, nil
}

func (pds *ProtocolDataService) GetCurrentBlockHeight(ctx context.Context, confirmed bool) (*storage.Block, error) {
	if confirmed {
		return pds.GetCurrentConfirmedBlockHeight(ctx)
	}

	var block *storage.Block
	res := pds.db.Model(&block).Order("number desc").First(&block)

	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return block, nil
}

func (pds *ProtocolDataService) GetEigenStateChangesForBlock(ctx context.Context, blockHeight uint64) (map[string][]interface{}, error) {
	results, err := pds.stateManager.ListForBlockRange(blockHeight, blockHeight)
	if err != nil {
		return nil, err
	}
	return results, nil
}
