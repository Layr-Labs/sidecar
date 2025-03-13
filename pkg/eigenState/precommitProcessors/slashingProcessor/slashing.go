package slashingProcessor

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SlashingProcessor struct {
	logger *zap.Logger
	grm    *gorm.DB
}

func NewSlashingProcessor(sm *stateManager.EigenStateManager, logger *zap.Logger, grm *gorm.DB) *SlashingProcessor {
	processor := &SlashingProcessor{
		logger: logger,
		grm:    grm,
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
	return nil
}
