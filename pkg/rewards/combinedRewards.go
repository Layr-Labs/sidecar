package rewards

import "time"

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

type RewardsCombined struct {
	Avs            string
	RewardHash     string
	Token          string
	Amount         string
	Strategy       string
	StrategyIndex  uint64
	Multiplier     string
	StartTimestamp *time.Time `gorm:"type:DATETIME"`
	EndTimestamp   *time.Time `gorm:"type:DATETIME"`
	Duration       uint64
	BlockNumber    uint64
	RewardType     string // avs, all_stakers, all_earners
}

func (r *RewardsCalculator) GenerateCombinedRewards() ([]*RewardsCombined, error) {
	combinedRewards := make([]*RewardsCombined, 0)

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

	res := r.calculationDB.Model(&RewardsCombined{}).CreateInBatches(combinedRewards, 100)
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
