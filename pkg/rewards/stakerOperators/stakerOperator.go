package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type StakerOperatorsGenerator struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config
}

func NewStakerOperatorGenerator(
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) *StakerOperatorsGenerator {
	return &StakerOperatorsGenerator{
		db:           grm,
		logger:       logger,
		globalConfig: globalConfig,
	}
}

func (sog *StakerOperatorsGenerator) GenerateStakerOperatorsTable(cutoffDate string) error {
	forks, err := sog.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		return err
	}

	if !sog.globalConfig.Rewards.GenerateStakerOperatorsTable {
		sog.logger.Sugar().Infow("Skipping generation of staker operators table, disabled via config",
			zap.String("cutoffDate", cutoffDate),
		)
		return nil
	}

	sog.logger.Sugar().Infow("Generating staker operators table", zap.String("cutoffDate", cutoffDate))
	if err := sog.GenerateAndInsert1StakerStrategyPayouts(cutoffDate, forks); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 1 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert2OperatorStrategyRewards(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 2 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert3RewardsForAllStrategyPayout(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 3 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert4RfaeStakerStrategyPayout(cutoffDate, forks); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 4 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert5RfaeOperatorStrategyPayout(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 5 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert6OperatorODStrategyPayouts(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 6 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert7StakerODStrategyPayouts(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 7 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert8AvsODStrategyPayouts(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 8 staker strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert9OperatorODOperatorSetStrategyPayouts(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 9 operator OD operator set strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert10StakerODOperatorSetStrategyPayouts(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 10 staker OD operator set strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert11AvsODOperatorSetStrategyPayouts(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 11 AVS OD operator set strategy rewards",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert12StakerOperatorStaging(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 12 staker operator staging",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}

	if err := sog.GenerateAndInsert13StakerOperator(cutoffDate); err != nil {
		sog.logger.Sugar().Errorw("Failed to generate and insert 13 staker operator",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(err),
		)
		return err
	}
	return nil
}
