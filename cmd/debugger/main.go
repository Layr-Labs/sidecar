package main

import (
	"fmt"
	"log"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/contractStore/sqliteContractStore"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/avsOperators"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/operatorShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stakerDelegations"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stakerShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"go.uber.org/zap"
)

func main() {
	cfg := config.NewConfig()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	db := sqlite.NewSqlite(&sqlite.SqliteConfig{
		Path:           "/Users/seanmcgary/Code/sidecar/test-db/test.db",
		ExtensionsPath: []string{"/Users/seanmcgary/Code/sidecar/sqlite-extensions/build/lib/libcalculations"},
	}, l)

	grm, err := sqlite.NewGormSqliteFromSqlite(db)
	if err != nil {
		l.Error("Failed to create gorm instance", zap.Error(err))
		panic(err)
	}

	migrator := migrations.NewSqliteMigrator(grm, l)
	if err = migrator.MigrateAll(); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	contractStore := sqliteContractStore.NewSqliteContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	sm := stateManager.NewEigenStateManager(l, grm)

	if _, err := avsOperators.NewAvsOperatorsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create AvsOperatorsModel", zap.Error(err))
	}
	if _, err := operatorShares.NewOperatorSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create OperatorSharesModel", zap.Error(err))
	}
	if _, err := stakerDelegations.NewStakerDelegationsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create StakerDelegationsModel", zap.Error(err))
	}
	if _, err := stakerShares.NewStakerSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create StakerSharesModel", zap.Error(err))
	}

	fmt.Printf("Running query\n")

	query := `
	WITH reward_snapshot_operators as (
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
	ap.reward_submission_date,
	oar.operator
  FROM gold_1_active_rewards ap
  JOIN operator_avs_registration_snapshots oar
  	ON ap.avs = oar.avs and ap.snapshot = oar.snapshot
  WHERE ap.reward_type = 'avs'
),
operator_restaked_strategies AS (
  SELECT
	rso.*
  FROM reward_snapshot_operators rso
  JOIN operator_avs_strategy_snapshots oas
  ON
	rso.operator = oas.operator
	and rso.avs = oas.avs
	and rso.strategy = oas.strategy
	and rso.snapshot = oas.snapshot
),
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
  SELECT
	ors.*,
	sds.staker
  FROM operator_restaked_strategies ors
  JOIN staker_delegation_snapshots sds
  ON
	ors.operator = sds.operator AND
	ors.snapshot = sds.snapshot
),
-- Get the shares for staker delegated to the operator
staker_avs_strategy_shares AS (
  SELECT
	sdo.*,
	sss.shares
  FROM staker_delegated_operators sdo
  JOIN staker_share_snapshots sss
  ON
	sdo.staker = sss.staker
	and sdo.snapshot = sss.snapshot
	and sdo.strategy = sss.strategy
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE big_gt(sss.shares, '0') and sdo.multiplier != '0'
),
-- Calculate the weight of a staker
staker_weight_grouped as (
	select
		staker,
	    reward_hash,
	    snapshot,
	    sum_big_c(numeric_multiply_c(multiplier, shares)) as staker_weight
	from staker_avs_strategy_shares
	group by staker, reward_hash, snapshot
),
staker_weights AS (
  SELECT
      s.*,
      swg.staker_weight
  FROM staker_avs_strategy_shares s
  left join staker_weight_grouped swg on (
      s.staker = swg.staker
      and s.reward_hash = swg.reward_hash
      and s.snapshot = swg.snapshot
  )
),
-- Get distinct stakers since their weights are already calculated
distinct_stakers AS (
  SELECT *
  FROM (
	  SELECT *,
		-- We can use an arbitrary order here since the staker_weight is the same for each (staker, strategy, hash, snapshot)
		-- We use strategy ASC for better debuggability
		ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, staker ORDER BY strategy ASC) as rn
	  FROM staker_weights
  ) t
  WHERE rn = 1
  ORDER BY reward_hash, snapshot, staker
),
staker_weight_sum_groups as (
	select
		reward_hash,
	   	snapshot,
	    sum_big_c(staker_weight) as total_weight
	from distinct_stakers
	group by reward_hash, snapshot
),
-- Calculate sum of all staker weights for each reward and snapshot
staker_weight_sum AS (
	SELECT
		s.*,
		sws.total_weight
  FROM distinct_stakers as s
  join staker_weight_sum_groups as sws on (s.reward_hash = sws.reward_hash and s.snapshot = sws.snapshot)
),
-- Calculate staker proportion of tokens for each reward and snapshot
staker_proportion AS (
  SELECT *,
	staker_weight(staker_weight, total_weight) as staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to the (staker, operator) pair
staker_operator_total_tokens AS (
  SELECT *,
	CASE -- For snapshots that are before the hard fork AND submitted before the hard fork, we use the old calc method
	  WHEN snapshot < DATE('1970-01-01') AND reward_submission_date < DATE('1970-01-01') THEN
		amazon_staker_token_rewards(staker_proportion, tokens_per_day)
	  WHEN snapshot < DATE('2024-08-13') AND reward_submission_date < DATE('2024-08-13') THEN
		nile_staker_token_rewards(staker_proportion, tokens_per_day)
	  ELSE
		staker_token_rewards(staker_proportion, tokens_per_day)
	END as total_staker_operator_payout
  FROM staker_proportion
),
operator_tokens as (
	select *,
		CASE
		  WHEN snapshot < DATE('1970-01-01') AND reward_submission_date < DATE('1970-01-01') THEN
			amazon_operator_token_rewards(total_staker_operator_payout)
		  WHEN snapshot < DATE('2024-08-13') AND reward_submission_date < DATE('2024-08-13') THEN
			nile_operator_token_rewards(total_staker_operator_payout)
		  ELSE
			operator_token_rewards(total_staker_operator_payout)
		END as operator_tokens
	from staker_operator_total_tokens
),
-- Calculate the token breakdown for each (staker, operator) pair
token_breakdowns AS (
  SELECT *,
	subtract_big(total_staker_operator_payout, operator_tokens) as staker_tokens
  FROM operator_tokens
)
SELECT * from staker_avs_strategy_shares
`
	rowResult := make([]interface{}, 0)
	res := grm.Raw(query).Scan(&rowResult)
	fmt.Printf("Error? %+v\n", res.Error)
	if res.Error != nil {
		log.Fatalf("Failed to load extension: %v", res.Error)
	}
	fmt.Printf("Got %d rows\n", len(rowResult))
	// for i, row := range rowResult {
	// 	fmt.Printf("Row %d - %+v\n", i, row)
	// }

}
