package rewards

const _6_goldRfaeOperatorsQuery = `
insert into gold_6_rfae_operators
with operator_token_sums AS (
  SELECT
    g.reward_hash,
    g.snapshot,
    g.token,
    g.avs,
    g.strategy,
    g.multiplier,
    g.reward_type,
    g.operator,
    sum_big(g.operator_tokens) OVER (PARTITION BY operator, reward_hash, snapshot) AS operator_tokens
  FROM gold_5_rfae_stakers as g
),
-- Dedupe the operator tokens across strategies for each operator, reward hash, and snapshot
distinct_operators AS (
  SELECT *
  FROM (
      SELECT *,
        -- We can use an arbitrary order here since the staker_weight is the same for each (operator, strategy, hash, snapshot)
        -- We use strategy ASC for better debuggability
        ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, operator ORDER BY strategy ASC) as rn
      FROM operator_token_sums
  ) t
  WHERE rn = 1
)
SELECT * FROM distinct_operators
`

func (rc *RewardsCalculator) GenerateGold6RfaeOperatorsTable() error {
	res := rc.grm.Exec(_6_goldRfaeOperatorsQuery)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_rfae_operators", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) CreateGold6RfaeOperatorsTable() error {
	query := `
		create table if not exists gold_6_rfae_operators (
			reward_hash TEXT NOT NULL,
			snapshot DATE NOT NULL,
			token TEXT NOT NULL,
			avs TEXT NOT NULL,
			strategy TEXT NOT NULL,
			multiplier TEXT NOT NULL,
			reward_type TEXT NOT NULL,
			operator TEXT NOT NULL,
			operator_tokens TEXT NOT NULL,
			rn INTEGER NOT NULL
		)
	`
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_6_rfae_operators", "error", res.Error)
		return res.Error
	}
	return nil
}
