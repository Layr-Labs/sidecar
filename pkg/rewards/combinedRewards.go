package rewards

import (
	"errors"
	"github.com/Layr-Labs/go-sidecar/internal/types/numbers"
	"go.uber.org/zap"
	"math"
	"math/big"
	"time"
)

const rewardsCombinedQuery = `
	with combined_rewards as (
		select
			avs,
			reward_hash,
			token,
			amount,
			strategy,
			strategy_index,
			multiplier,
			start_timestamp,
			end_timestamp,
			duration,
			block_number,
			reward_type,
			ROW_NUMBER() OVER (PARTITION BY reward_hash, strategy_index ORDER BY block_number asc) as rn
		from reward_submissions
	)
	select * from combined_rewards
	where rn = 1
`

type CombinedRewards struct {
	Avs                 string
	RewardHash          string
	Token               string
	Amount              string
	Strategy            string
	StrategyIndex       uint64
	Multiplier          string
	StartTimestamp      *time.Time `gorm:"type:DATETIME"`
	EndTimestamp        *time.Time `gorm:"type:DATETIME"`
	Duration            uint64
	BlockNumber         uint64
	RewardType          string // avs, all_stakers, all_earners
	TokensPerDay        string
	TokensPerDayDecimal string
}

func (r *RewardsCalculator) GenerateCombinedRewards() ([]*CombinedRewards, error) {
	combinedRewards := make([]*CombinedRewards, 0)

	res := r.grm.Raw(rewardsCombinedQuery).Scan(&combinedRewards)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate combined rewards", "error", res.Error)
		return nil, res.Error
	}
	return combinedRewards, nil
}

func (r *RewardsCalculator) GenerateAndInsertCombinedRewards() error {
	combinedRewards, err := r.GenerateCombinedRewards()
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate combined rewards", "error", err)
		return err
	}

	for _, combinedReward := range combinedRewards {
		amount := big.NewFloat(0)
		amount, _, err := amount.Parse(combinedReward.Amount, 10)
		if err != nil {
			r.logger.Sugar().Errorw("Failed to parse amount",
				zap.String("amount", combinedReward.Amount),
				zap.String("reward_hash", combinedReward.RewardHash),
			)
			return errors.New("failed to parse amount")
		}
		// float64(combinedReward.Duration) / 86400
		tokensPerDay := amount.Quo(amount, big.NewFloat(float64(combinedReward.Duration)/86400))

		precision := (math.Pow(10, 15) - float64(1)) / math.Pow(10, 15)

		// Round down to 15 sigfigs for double precision, ensuring know errouneous round up or down
		tokensPerDayDecimal := tokensPerDay.Mul(tokensPerDay, big.NewFloat(precision))

		// We use floor to ensure we are always underesimating total tokens per day
		tokensPerDayFloored := numbers.NewBig257()
		tokensPerDayFloored, _ = tokensPerDay.Int(tokensPerDayFloored)

		combinedReward.TokensPerDay = tokensPerDayFloored.String()
		combinedReward.TokensPerDayDecimal = tokensPerDayDecimal.String()
	}

	res := r.calculationDB.Model(&CombinedRewards{}).CreateInBatches(combinedRewards, 100)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert combined rewards", "error", res.Error)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) CreateCombinedRewardsTable() error {
	res := r.calculationDB.Exec(`
		CREATE TABLE IF NOT EXISTS combined_rewards (
			avs TEXT,
			reward_hash TEXT,
			token TEXT,
			amount TEXT,
			strategy TEXT,
			strategy_index INTEGER,
			multiplier TEXT,
			start_timestamp DATETIME,
			end_timestamp DATETIME,
			duration INTEGER,
			block_number INTEGER,
			reward_type string
		)`,
	)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to create combined_rewards table", "error", res.Error)
		return res.Error
	}
	return nil
}
