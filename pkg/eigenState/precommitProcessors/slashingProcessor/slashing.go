package slashingProcessor

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SlashingProcessor struct {
	logger       *zap.Logger
	grm          *gorm.DB
	globalConfig *config.Config
}

func NewSlashingProcessor(sm *stateManager.EigenStateManager, logger *zap.Logger, grm *gorm.DB, cfg *config.Config) *SlashingProcessor {
	processor := &SlashingProcessor{
		logger:       logger,
		grm:          grm,
		globalConfig: cfg,
	}
	sm.RegisterPrecommitProcessor(processor, 0)
	return processor
}

func (sp *SlashingProcessor) GetName() string {
	return "slashingProcessor"
}

// Process handles compiling staker shares and delegation state for the current block.
//
// When applying slashing conditions, we need to ensure that we're capturing not only past delegation events,
// but also those that are occurring in the current block. Since the StakerShares model knows nothing about
// the StakerDelegation model and since changes arent committed until the very end, we need a mechanism
// to inject the current block's delegation events into the StakerShares model for consideration for slashing.
func (sp *SlashingProcessor) Process(blockNumber uint64, models map[string]types.IEigenStateModel) error {
	sp.logger.Sugar().Debug("Running slashing processor for block number", zap.Uint64("blockNumber", blockNumber))
	stakerSharesModel, ok := models[stakerShares.StakerSharesModelName].(*stakerShares.StakerSharesModel)
	if !ok || stakerSharesModel == nil {
		sp.logger.Sugar().Error("Staker shares model not found in models map")
		return fmt.Errorf("staker shares model not found in models map")
	}

	stakerDelegationModel, ok := models[stakerDelegations.StakerDelegationsModelName].(*stakerDelegations.StakerDelegationsModel)
	if !ok || stakerDelegationModel == nil {
		sp.logger.Sugar().Error("Staker delegation model not found in models map")
		return fmt.Errorf("staker delegation model not found in models map")
	}

	// check to see if we encountered any slashing events. If there arent any, we can skip
	slashingDeltas, ok := stakerSharesModel.SlashingAccumulator[blockNumber]
	if !ok {
		sp.logger.Sugar().Debug("No slashing deltas found for block number", zap.Uint64("blockNumber", blockNumber))
		return nil
	}
	if len(slashingDeltas) == 0 {
		sp.logger.Sugar().Debug("No slashing deltas found for block number", zap.Uint64("blockNumber", blockNumber))
		return nil
	}

	// get the in-memory staker delegations for this block. If there arent any, theres nothing to do
	delegations := stakerDelegationModel.GetAccumulatedState(blockNumber)
	if len(delegations) == 0 {
		sp.logger.Sugar().Debug("No staker delegations found for block number", zap.Uint64("blockNumber", blockNumber))
		return nil
	}

	// inject the current block delegations into the staker shares model
	precommitDelegations := make([]*stakerShares.PrecommitDelegatedStaker, 0)
	for _, d := range delegations {
		delegation := &stakerShares.PrecommitDelegatedStaker{
			Staker:           d.Staker,
			Operator:         d.Operator,
			Delegated:        d.Delegated,
			TransactionHash:  d.TransactionHash,
			TransactionIndex: d.TransactionIndex,
			LogIndex:         d.LogIndex,
		}
		precommitDelegations = append(precommitDelegations, delegation)
	}
	stakerSharesModel.PrecommitDelegatedStakers[blockNumber] = precommitDelegations

	// Also handle slashing adjustments for queued withdrawals
	err := sp.processQueuedWithdrawalSlashing(blockNumber, models)
	if err != nil {
		sp.logger.Sugar().Errorw("Failed to process queued withdrawal slashing",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
		)
		return err
	}

	return nil
}

// SlashingEvent represents a slashing event from the SlashingAccumulator
type SlashingEvent struct {
	SlashedEntity   string
	BeaconChain     bool
	Strategy        string
	WadSlashed      string
	TransactionHash string
	LogIndex        uint64
}

// processQueuedWithdrawalSlashing creates adjustment records for queued withdrawals
// when an operator is slashed, so that the effective withdrawal amount is reduced.
// It uses the SlashingAccumulator from stakerSharesModel which contains real-time
// slash events (both operator and beacon chain) with WadSlashed as a percentage.
func (sp *SlashingProcessor) processQueuedWithdrawalSlashing(blockNumber uint64, models map[string]types.IEigenStateModel) error {
	stakerSharesModel, ok := models[stakerShares.StakerSharesModelName].(*stakerShares.StakerSharesModel)
	if !ok || stakerSharesModel == nil {
		sp.logger.Sugar().Error("Staker shares model not found in models map")
		return fmt.Errorf("staker shares model not found in models map")
	}

	// Get slashing events from the accumulator (includes both operator and beacon chain slashes)
	slashingDeltas, ok := stakerSharesModel.SlashingAccumulator[blockNumber]
	if !ok || len(slashingDeltas) == 0 {
		sp.logger.Sugar().Debug("No slashing events found for block number", zap.Uint64("blockNumber", blockNumber))
		return nil
	}

	// For each slashing event, find active queued withdrawals and create adjustment records
	for _, slashDiff := range slashingDeltas {
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}
		err := sp.createSlashingAdjustments(slashEvent, blockNumber)
		if err != nil {
			sp.logger.Sugar().Errorw("Failed to create slashing adjustments",
				zap.Error(err),
				zap.Uint64("blockNumber", blockNumber),
				zap.String("slashedEntity", slashEvent.SlashedEntity),
				zap.String("strategy", slashEvent.Strategy),
				zap.Bool("beaconChain", slashEvent.BeaconChain),
			)
			return err
		}
	}

	return nil
}

func (sp *SlashingProcessor) createSlashingAdjustments(slashEvent *SlashingEvent, blockNumber uint64) error {
	// Build query based on slash type:
	// - Operator slash: affects all stakers delegated to the operator for the given strategy
	// - Beacon chain slash: affects only the specific staker (pod owner) for native ETH strategy
	var query string
	var params map[string]any

	baseSelect := `
		SELECT
			qsw.staker,
			qsw.strategy,
			qsw.operator,
			qsw.block_number as withdrawal_block_number,
			@slashBlockNumber as slash_block_number,
			-- Calculate cumulative slash multiplier: previous multipliers * (1 - current_slash)
			COALESCE(
				(SELECT slash_multiplier
				 FROM queued_withdrawal_slashing_adjustments adj
				 WHERE adj.staker = qsw.staker
				 AND adj.strategy = qsw.strategy
				 AND adj.operator = qsw.operator
				 AND adj.withdrawal_block_number = qsw.block_number
				 ORDER BY adj.slash_block_number DESC
				 LIMIT 1),
				1
			) * (1 - LEAST(@wadSlashed / 1e18, 1)) as slash_multiplier,
			@blockNumber as block_number,
			@transactionHash as transaction_hash,
			@logIndex as log_index
		FROM queued_slashing_withdrawals qsw
		INNER JOIN blocks b_queued ON qsw.block_number = b_queued.number
	`

	baseWhere := `
		-- Withdrawal was queued before this slash (check block number AND log index for same-block events)
		AND (
			qsw.block_number < @slashBlockNumber
			OR (qsw.block_number = @slashBlockNumber AND qsw.log_index < @logIndex)
		)
		-- Still within withdrawal queue window (not yet completable)
		AND b_queued.block_time + (@withdrawalQueueWindow * INTERVAL '1 day') > (
			SELECT block_time FROM blocks WHERE number = @blockNumber
		)
		-- Backwards compatibility: only process records with valid data
		AND qsw.staker IS NOT NULL
		AND qsw.strategy IS NOT NULL
		AND qsw.operator IS NOT NULL
		AND qsw.shares_to_withdraw IS NOT NULL
		AND b_queued.block_time IS NOT NULL
	`

	if slashEvent.BeaconChain {
		// Beacon chain slash: affects only the specific staker (pod owner)
		query = baseSelect + `
		WHERE qsw.staker = @staker
		AND qsw.strategy = @strategy
		` + baseWhere

		params = map[string]any{
			"slashBlockNumber":      blockNumber,
			"wadSlashed":            slashEvent.WadSlashed,
			"blockNumber":           blockNumber,
			"transactionHash":       slashEvent.TransactionHash,
			"logIndex":              slashEvent.LogIndex,
			"staker":                slashEvent.SlashedEntity, // For beacon chain, SlashedEntity is the staker
			"strategy":              slashEvent.Strategy,
			"withdrawalQueueWindow": sp.globalConfig.Rewards.WithdrawalQueueWindow,
		}
	} else {
		// Operator slash: affects all stakers delegated to the operator
		query = baseSelect + `
		WHERE qsw.operator = @operator
		AND qsw.strategy = @strategy
		` + baseWhere

		params = map[string]any{
			"slashBlockNumber":      blockNumber,
			"wadSlashed":            slashEvent.WadSlashed,
			"blockNumber":           blockNumber,
			"transactionHash":       slashEvent.TransactionHash,
			"logIndex":              slashEvent.LogIndex,
			"operator":              slashEvent.SlashedEntity, // For operator slash, SlashedEntity is the operator
			"strategy":              slashEvent.Strategy,
			"withdrawalQueueWindow": sp.globalConfig.Rewards.WithdrawalQueueWindow,
		}
	}

	type AdjustmentRecord struct {
		Staker                string
		Strategy              string
		Operator              string
		WithdrawalBlockNumber uint64
		SlashBlockNumber      uint64
		SlashMultiplier       string
		BlockNumber           uint64
		TransactionHash       string
		LogIndex              uint64
	}

	var adjustments []AdjustmentRecord
	err := sp.grm.Raw(query, params).Scan(&adjustments).Error

	if err != nil {
		return fmt.Errorf("failed to find active withdrawals for slashing: %w", err)
	}

	if len(adjustments) == 0 {
		sp.logger.Sugar().Debugw("No active queued withdrawals found for slashing event",
			zap.String("slashedEntity", slashEvent.SlashedEntity),
			zap.String("strategy", slashEvent.Strategy),
			zap.Bool("beaconChain", slashEvent.BeaconChain),
			zap.Uint64("blockNumber", blockNumber),
		)
		return nil
	}

	// Insert adjustment records
	for _, adj := range adjustments {
		err := sp.grm.Table("queued_withdrawal_slashing_adjustments").Create(&adj).Error
		if err != nil {
			sp.logger.Sugar().Errorw("Failed to create slashing adjustment record",
				zap.Error(err),
				zap.String("staker", adj.Staker),
				zap.String("strategy", adj.Strategy),
				zap.Uint64("withdrawalBlockNumber", adj.WithdrawalBlockNumber),
			)
			return err
		}

		sp.logger.Sugar().Infow("Created queued withdrawal slashing adjustment",
			zap.String("staker", adj.Staker),
			zap.String("strategy", adj.Strategy),
			zap.String("operator", adj.Operator),
			zap.Uint64("withdrawalBlock", adj.WithdrawalBlockNumber),
			zap.Uint64("slashBlock", adj.SlashBlockNumber),
			zap.String("multiplier", adj.SlashMultiplier),
			zap.Bool("beaconChain", slashEvent.BeaconChain),
		)
	}

	return nil
}
