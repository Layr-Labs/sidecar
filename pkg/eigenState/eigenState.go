package eigenState

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/defaultOperatorSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/disabledDistributionRoots"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/encumberedMagnitudes"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAVSSplits"
	operatorAllocationDelayDelays "github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAllocationDelays"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAllocations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorDirectedOperatorSetRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorDirectedRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorMaxMagnitudes"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorPISplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetOperatorRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetStrategyRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSets"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/queuedSlashingWithdrawals"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/rewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/submittedDistributionRoots"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func LoadEigenStateModels(
	sm *stateManager.EigenStateManager,
	grm *gorm.DB,
	l *zap.Logger,
	cfg *config.Config,
) error {
	if _, err := avsOperators.NewAvsOperatorsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create AvsOperatorsModel", zap.Error(err))
		return err
	}
	if _, err := operatorShares.NewOperatorSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorSharesModel", zap.Error(err))
		return err
	}
	if _, err := stakerDelegations.NewStakerDelegationsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create StakerDelegationsModel", zap.Error(err))
		return err
	}
	if _, err := stakerShares.NewStakerSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create StakerSharesModel", zap.Error(err))
		return err
	}
	if _, err := submittedDistributionRoots.NewSubmittedDistributionRootsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create SubmittedDistributionRootsModel", zap.Error(err))
		return err
	}
	if _, err := rewardSubmissions.NewRewardSubmissionsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create RewardSubmissionsModel", zap.Error(err))
		return err
	}
	if _, err := disabledDistributionRoots.NewDisabledDistributionRootsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create DisabledDistributionRootsModel", zap.Error(err))
		return err
	}
	if _, err := operatorDirectedRewardSubmissions.NewOperatorDirectedRewardSubmissionsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorDirectedRewardSubmissionsModel", zap.Error(err))
		return err
	}
	if _, err := operatorAVSSplits.NewOperatorAVSSplitModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorAVSSplitModel", zap.Error(err))
		return err
	}
	if _, err := operatorPISplits.NewOperatorPISplitModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorPISplitModel", zap.Error(err))
		return err
	}
	if _, err := defaultOperatorSplits.NewDefaultOperatorSplitModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create DefaultOperatorSplitModel", zap.Error(err))
		return err
	}
	if _, err := operatorDirectedOperatorSetRewardSubmissions.NewOperatorDirectedOperatorSetRewardSubmissionsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorDirectedOperatorSetRewardSubmissionsModel", zap.Error(err))
		return err
	}
	if _, err := operatorSetSplits.NewOperatorSetSplitModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorSetSplitModel", zap.Error(err))
		return err
	}
	if _, err := operatorSetOperatorRegistrations.NewOperatorSetOperatorRegistrationModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorSetOperatorRegistrationModel", zap.Error(err))
		return err
	}
	if _, err := operatorSetStrategyRegistrations.NewOperatorSetStrategyRegistrationModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorSetStrategyRegistrationModel", zap.Error(err))
		return err
	}
	if _, err := operatorSets.NewOperatorSetModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorSetModel", zap.Error(err))
		return err
	}
	if _, err := operatorAllocations.NewOperatorAllocationModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorAllocationModel", zap.Error(err))
		return err
	}
	if _, err := slashedOperators.NewSlashedOperatorModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create SlashedOperatorModel", zap.Error(err))
		return err
	}
	if _, err := encumberedMagnitudes.NewEncumberedMagnitudeModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create EncumberedMagnitudeModel", zap.Error(err))
		return err
	}
	if _, err := operatorMaxMagnitudes.NewOperatorMaxMagnitudeModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorMaxMagnitudeModel", zap.Error(err))
		return err
	}
	if _, err := slashedOperatorShares.NewSlashedOperatorSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create SlashedOperatorSharesModel", zap.Error(err))
		return err
	}
	if _, err := operatorAllocationDelayDelays.NewOperatorAllocationDelayModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create OperatorAllocationDelayModel", zap.Error(err))
		return err
	}
	if _, err := queuedSlashingWithdrawals.NewQueuedSlashingWithdrawalModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Errorw("Failed to create QueuedSlashingWithdrawalModel", zap.Error(err))
		return err
	}

	return nil
}
