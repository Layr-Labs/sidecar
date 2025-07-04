package stakerOperators

import (
	"database/sql"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
)

const _1_stakerStrategyPayoutsQuery = `
create table {{.destTableName}} as
WITH base_data AS (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.tokens_per_day_decimal,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    oar.operator,
    ap.reward_submission_date,
    sds.staker,
    sss.shares,
    spa.staker_tokens
  FROM {{.activeRewardsTable}} ap
  JOIN operator_avs_registration_snapshots oar
    ON ap.avs = oar.avs AND ap.snapshot = oar.snapshot
  JOIN operator_avs_strategy_snapshots oas
    ON oar.operator = oas.operator 
    AND ap.avs = oas.avs 
    AND ap.strategy = oas.strategy 
    AND ap.snapshot = oas.snapshot
  JOIN staker_delegation_snapshots sds
    ON oas.operator = sds.operator AND ap.snapshot = sds.snapshot
  JOIN staker_share_snapshots sss
    ON sds.staker = sss.staker 
    AND ap.snapshot = sss.snapshot 
    AND ap.strategy = sss.strategy
  JOIN {{.stakerRewardAmountsTable}} spa
    ON ap.snapshot = spa.snapshot 
    AND ap.reward_hash = spa.reward_hash 
    AND sds.staker = spa.staker
  WHERE ap.reward_type = 'avs'
    AND sss.shares > 0 
    AND ap.multiplier != 0
),
staker_totals AS (
  SELECT 
    staker,
    reward_hash,
    snapshot,
    SUM(multiplier * shares) as staker_total_strategy_weight
  FROM base_data
  GROUP BY staker, reward_hash, snapshot
),
final_calculation AS (
  SELECT 
    bd.*,
    bd.multiplier * bd.shares AS staker_strategy_weight,
    st.staker_total_strategy_weight,
    FLOOR((bd.multiplier * bd.shares / st.staker_total_strategy_weight) * 1000000000000000) / 1000000000000000 as staker_strategy_proportion
  FROM base_data bd
  JOIN staker_totals st 
    ON bd.staker = st.staker 
    AND bd.reward_hash = st.reward_hash 
    AND bd.snapshot = st.snapshot
)
SELECT *,
  CASE
    WHEN snapshot < @amazonHardforkDate AND reward_submission_date < @amazonHardforkDate THEN
      cast(staker_strategy_proportion * staker_tokens AS DECIMAL(38,0))
    WHEN snapshot < @nileHardforkDate AND reward_submission_date < @nileHardforkDate THEN
      (staker_strategy_proportion * staker_tokens)::text::decimal(38,0)
    ELSE
      FLOOR(staker_strategy_proportion * staker_tokens)
  END as staker_strategy_tokens
FROM final_calculation
`

type StakerStrategyPayout struct {
	RewardHash           string
	Snapshot             time.Time
	Token                string
	TokensPerDay         float64
	Avs                  string
	Strategy             string
	Multiplier           string
	RewardType           string
	Staker               string
	Shares               string
	StakerStrategyTokens string
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert1StakerStrategyPayouts(cutoffDate string, forks config.ForkMap) error {
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_1_StakerStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 1_stakerStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_1_stakerStrategyPayoutsQuery, map[string]interface{}{
		"destTableName":            destTableName,
		"activeRewardsTable":       rewardsUtils.RewardsTable_1_ActiveRewards,
		"stakerRewardAmountsTable": rewardsUtils.RewardsTable_2_StakerRewardAmounts,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 1_stakerStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query,
		sql.Named("amazonHardforkDate", forks[config.RewardsFork_Amazon].Date),
		sql.Named("nileHardforkDate", forks[config.RewardsFork_Nile].Date),
	)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 1_stakerStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
