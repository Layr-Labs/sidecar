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

func (sp *SlashingProcessor) Process(blockNumber uint64, models map[string]types.IEigenStateModel) error {
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
	if delegations == nil || len(delegations) == 0 {
		sp.logger.Sugar().Debug("No staker delegations found for block number", zap.Uint64("blockNumber", blockNumber))
		return nil
	}

	// inject the current block delegations into the staker shares model
	precommitDelegations := make([]*stakerShares.PrecommitDelegatedStaker, 0)
	for _, d := range delegations {
		precommitDelegations = append(precommitDelegations, &stakerShares.PrecommitDelegatedStaker{
			Staker:    d.Staker,
			Operator:  d.Operator,
			Delegated: d.Delegated,
		})
	}
	stakerSharesModel.PrecommitDelegatedStakers[blockNumber] = precommitDelegations
	return nil
}
