package eigenState

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/defaultOperatorSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/disabledDistributionRoots"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAVSSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorDirectedRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorPISplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/rewardSubmissions"
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
	return nil
}
