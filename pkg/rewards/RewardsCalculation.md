---
title: EigenLayer Rewards Calculation - Foundation Incentives
---

# EigenLayer Rewards Calculation - Programmatic Incentives

The previous rewards calculation link is [here](https://hackmd.io/u-NHKEvtQ7m7CVDb4_42bA). A Diff is [here](https://www.diffchecker.com/zxczJFZr/)

[TOC]

# Overview

The EigenLayer rewards calculation is a set of SQL queries that calculates the distribution from rewards made by AVSs to stakers and operators via the `RewardsCoordinator`. The data is ingested on Postgres tables and transformed to calculate final rewards.

The queries run in a daily job that use snapshots of core contract state to calculate rewards made from any active rewards submissions.

## Calculation Process

The calculation proceeds in 3 stages and is run daily

1. Data Extraction: Extracts data from event logs. There are referred to as the bronze tables. As part of this process, we reconcile all event data, including RPC data, with an additional data provider to ensure consistency.
2. Data Transformation: Transforms data from the bronze tables into daily snapshots of on-chain state.
3. Reward Calculation: Cross-references snapshots with active rewards submissions to calculate rewards to stakers and operators.

The pipeline then aggregates rewards all rewards up to `lastRewardTimestamp + calculationIntervalSeconds` and submits a root that merkleizes the the cumulative sum of each earner to the `RewardsCoordinator`.

## Job Sequencing

The Reward Calculation is an airflow pipeline that runs daily at 16:00 UTC. Queries to on-chain events and event calculations are all rounded down to 0:00 UTC. That is, **if the pipeline is run on 4-27 at 16:00 UTC, the `cutoff_date` parameter is set to 4-26 at 0:00 UTC.**

We handle reorgs by running the daily pipeline several hours after the 0:00 UTC, giving our reorg handler enough time to heal state.

## Key Considerations

Each of the three sections below details key considerations to be mindful of when reading the queries and understanding the calculation. A summary of these considerations are:

- Snapshots are taken of core contract state every 24 hours: `SNAPSHOT_CADENCE` in `RewardsCoordinator`
- Snapshots from on-chain state are _rounded up_ to the nearest day at 0:00 UTC. The one exception is operator<>avs deregistrations, which are rounded _down_ to the nearest day at 0:00 UTC
- Since snapshots are rounded up, we only care about the _latest state update_ from a single day
- The reward distribution to all earners must be <= the amount paid for a rewards submission

## Glossary

- `earner`: The entity, a staker or operator, receiving a reward
- `calculationIntervalSeconds`: The multiple that the duration of rewards submissions must be
- `SNAPSHOT_CADENCE`: The cadence at which snapshots of EigenLayer core contract state are taken
- `typo rewardSnaphot -> rewardSnapshot`: The reward to earners on a snapshot
- `cutoff-date`: The date at which transformations are run. Always set to the _previous_ days at 0:00 UTC
- `run`: An iteration of the daily rewards pipeline job
- `stakeWeight`: How an AVS values its earners stake, given by multipliers for each strategy of the reward
- `gold_table`: The table that contains the `rewardSnapshots`. Its columns are `earner`, `amount`, `token`, `snapshot`, `reward_hash`

# Data Extraction

## Key Considerations

Shares are transformed into Decimal(78,0), a data type that can hold up to uint256. The tokens that are whitelisted for deposit (all LSTs & Eigen) & Native ETH should not have this an issue with truncation.

## Cutoff Date

We set the cutoff date at the beginning of each run with the following logic:

```python=
def get_cutoff_date():
    # get current time in utc
    ts = datetime.now(timezone.utc)
    # round down to 00:00 UTC of the current day
    ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    # subtract 1 day
    ts = ts - timedelta(days=1)

    return ts
```

## Airflow Variables

At the daily run of the pipeline, we get the variables passed in if the run is a backfill. On a backfill run, we enforce that the start & end date are valid, namely that the end date is not after the cutoff and that the start date is not after the end date.

Backfills are run in worst case scenarios if there are events that are missed in the pipeline run. We run reconciliation with multiple data vendors to ensure this should not have to be done. In addition, we run a sanity check query at the end of the pipeline generation which ensures that:

1. The cumulative rewards for earners never decreases
2. The tokens per day of an AVS is always >= sum(earener payouts) for a given snapshot and reward hash
3. The number of rows of (`earner`, `reward_hash`, and `snapshot`) never decreases

```python=
def get_gold_calculation_dates(**kwargs):
    # Cutoff Date
    cutoff_date = get_cutoff_date()
    cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

    # Backfill Date
    dag_run = kwargs.get('dag_run')
    if dag_run is not None:
        start_date_str = dag_run.conf.get('start_date', '1970-01-01 00:00:00')
        end_date_str = dag_run.conf.get('end_date', cutoff_date_str)
        is_backfill = str.lower(dag_run.conf.get('is_backfill', 'false'))
    else:
        raise ValueError('Dag run is None')

    # Sanitize the start and end dates
    start_datetime = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    cutoff_datetime = datetime.strptime(cutoff_date_str, '%Y-%m-%d %H:%M:%S')

    if start_datetime >= end_datetime:
        raise ValueError('Start date must be before end date')

    if end_datetime > cutoff_datetime:
        raise ValueError('End date must be less than or equal to cutoff date')

    # Push to XCom
    kwargs['ti'].xcom_push(key='cutoff_date', value=end_date_str)
    kwargs['ti'].xcom_push(key='rewards_start', value=start_date_str)
    kwargs['ti'].xcom_push(key='is_backfill', value=is_backfill)
```

## Queries

This set of queries extracts event data from EigenLayer Core contracts. The event logs are automatically decoded from the contract ABIs [here](https://github.com/Layr-Labs/eigenlayer-contracts). Running `forge build` will build the contracts and ABIs are stored in the `/out` folder.

In the below queries, `block_date` is the date of the block whereas `block_time` is the full date + time of the block.

### Staker State

#### Deposits

```sql=
SELECT
  lower(coalesce(t.output_data ->> 'depositor', t.output_data ->> 'staker')) as staker,
  lower(t.output_data ->> 'strategy') as strategy,
  (t.output_data ->> 'shares')::numeric(78,0) as shares,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs as t
LEFT JOIN blocks as b ON (t.block_sequence_id = b.id)
WHERE t.address = '{{ var('strategy_manager_address') }}'
AND t.event_name = 'Deposit'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### EigenPodShares

_Note: Shares can be negative_

```sql=
SELECT
  lower(t.arguments #>> '{0,Value}') AS staker,
  (t.output_data ->> 'sharesDelta')::numeric(78,0) as shares,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('eigen_pod_manager_address') }}'
AND t.event_name = 'PodSharesUpdated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### M1 Withdrawals

Withdrawals in M1 were routed through the StrategyManager. Note that we remove the single withdrawal completed as shares in M1 as there was no deposit event for this code path.

```sql=
SELECT
  lower(coalesce(t.output_data ->> 'depositor', t.output_data ->> 'staker')) as staker,
  lower(t.output_data ->> 'strategy') as strategy,
  (t.output_data ->> 'shares')::numeric(78,0) as shares,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('strategy_manager_address') }}'
AND t.event_name = 'ShareWithdrawalQueued'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
-- Remove this transaction hash as it is the only withdrawal on m1 that was completed as shares. There is no corresponding deposit event. The withdrawal was completed to the same staker address.
AND t.transaction_hash != '0x62eb0d0865b2636c74ed146e2d161e39e42b09bac7f86b8905fc7a830935dc1e'
```

#### M2 Withdrawals

Unlike M1 withdrawal events, M2 withdrawal events return a tuple with a list of strategies and shares. Thus, we unwind the tuple into indivudal rows to create (staker, strategy$_0$, share$_0$), (staker, strategy$_1$, share$_1$). We discard all M2 withdrawals that were migrated from M1 so we do not double count a withdrawal.

```sql=
WITH migrations AS (
  SELECT
    (
      SELECT lower(string_agg(lpad(to_hex(elem::int), 2, '0'), ''))
      FROM jsonb_array_elements_text(t.output_data->'oldWithdrawalRoot') AS elem
    ) AS m1_withdrawal_root,
    (
      SELECT lower(string_agg(lpad(to_hex(elem::int), 2, '0'), ''))
      FROM jsonb_array_elements_text(t.output_data->'newWithdrawalRoot') AS elem
    ) AS m2_withdrawal_root
  FROM transaction_logs t
  WHERE t.address = '{{ var('delegation_manager_address') }}'
  AND t.event_name = 'WithdrawalMigrated'
),
full_m2_withdrawals AS (
  SELECT
    lower(t.output_data #>> '{withdrawal}') as withdrawals,
    (
      SELECT lower(string_agg(lpad(to_hex(elem::int), 2, '0'), ''))
      FROM jsonb_array_elements_text(t.output_data ->'withdrawalRoot') AS elem
    ) AS withdrawal_root,
    lower(t.output_data #>> '{withdrawal, staker}') AS staker,
    lower(t_strategy.strategy) AS strategy,
    (t_share.share)::numeric(78,0) AS shares,
    t_strategy.strategy_index,
    t_share.share_index,
    t.transaction_hash,
    t.log_index,
    b.block_time::timestamp(6),
    to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
    t.block_number
  FROM transaction_logs t
  LEFT JOIN blocks b ON t.block_sequence_id = b.id,
  jsonb_array_elements_text(t.output_data #> '{withdrawal, strategies}') WITH ORDINALITY AS t_strategy(strategy, strategy_index),
  jsonb_array_elements_text(t.output_data #> '{withdrawal, shares}') WITH ORDINALITY AS t_share(share, share_index)
  WHERE t.address = '{{ var('delegation_manager_address') }}'
  AND t.event_name = 'WithdrawalQueued'
  AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
  AND t_strategy.strategy_index = t_share.share_index
)
-- Parse out the m2 withdrawals that were migrated from m1
SELECT
  full_m2_withdrawals.*
FROM
  full_m2_withdrawals
LEFT JOIN
  migrations
ON
  full_m2_withdrawals.withdrawal_root = migrations.m2_withdrawal_root
WHERE
  migrations.m2_withdrawal_root IS NULL
```

### Operator State

Operator state is made of stake delegated to them by stakers.

#### Operator Shares Increased

```sql=
SELECT
  lower(t.arguments #>> '{0,Value}') as operator,
  lower(t.output_data ->> 'strategy') as strategy,
  (t.output_data ->> 'shares')::numeric(78,0) as shares,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('delegation_manager_address') }}'
AND t.event_name = 'OperatorSharesIncreased'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### Operator Shares Decreased

```sql=
SELECT
  lower(t.arguments #>> '{0,Value}') as operator,
  lower(t.output_data ->> 'strategy') as strategy,
  (t.output_data ->> 'shares')::numeric(78,0) as shares,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('delegation_manager_address') }}'
AND t.event_name = 'OperatorSharesDecreased'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

### Staker Delegations

#### Staker Delegated

```sql=
SELECT
  lower(t.arguments #>> '{0,Value}') AS staker,
  lower(t.arguments #>> '{1,Value}') AS operator,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('delegation_manager_address') }}'
AND t.event_name = 'StakerDelegated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### Staker Undelegated

```sql=
SELECT
  lower(t.arguments #>> '{0,Value}') AS staker,
  lower(t.arguments #>> '{1,Value}') AS operator,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('delegation_manager_address') }}'
AND t.event_name = 'StakerUndelegated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

### Operator Splits

Three types of operator splits are tracked daily:

1. Default Operator Split: Determines reward split between operator and their delegated stakers for all operators (10% default, 1000 bips)
2. Operator-PI Split: Determines reward split between operator and their delegated stakers for Programmatic Incentives.
3. Operator-Set Split: Determines reward split between operator and their delegated stakers for Operator-Directed Rewards.

Split calculation follows these rules:

- Pre-Arno fork: Uses 10% default (1000 bips)
- Post-Arno fork: Uses operator-specific split or 10% default (1000 bips)
- Post-Trinity fork: Uses operator-specific split, global default, or 10% fallback

Example operator-AVS split snapshot:

```sql=
WITH operator_avs_splits_with_block_info as (
select
oas.operator,
oas.avs,
oas.activated_at::timestamp(6) as activated_at,
oas.new_operator_avs_split_bips as split,
oas.block_number,
oas.log_index,
b.block_time::timestamp(6) as block_time
from operator_avs_splits as oas
join blocks as b on (b.number = oas.block_number)
where activated_at < TIMESTAMP '{{.cutoffDate}}'
)
```

### Rewards Submissions

There are two types of rewards submissions in the protocol:

1. AVS Rewards Submission: Permissionless function called by any AVS
2. Reward for All: Permissioned reward to all stakers of the protocol
3. Operator-Directed Rewards Submission: Permissionless function called by any AVS to direct rewards to a specific operators

_Note: The amount in the RewardsCoordinator has a max value of $1e38-1$, which allows us to truncate it to a DECIMAL(38,0)._

#### AVS Rewards Submissions

For each rewards submission, we extract each (strategy,multiplier) in a separate row for easier accounting

```sql=
SELECT
    lower(tl.arguments #>> '{0,Value}') AS avs,
    lower(tl.arguments #>> '{2,Value}') AS reward_hash,
    coalesce(lower(tl.output_data #>> '{rewardsSubmission}'), lower(tl.output_data #>> '{rangePayment}')) as rewards_submission,
    coalesce(lower(tl.output_data #>> '{rewardsSubmission, token}'), lower(tl.output_data #>> '{rangePayment, token}')) as token,
    coalesce(tl.output_data #>> '{rewardsSubmission,amount}', tl.output_data #>> '{rangePayment,amount}')::numeric(78,0) as amount,
    to_timestamp(coalesce(tl.output_data #>> '{rewardsSubmission,startTimestamp}', tl.output_data #>> '{rangePayment,startTimestamp}')::bigint)::timestamp(6) as start_timestamp,
    coalesce(tl.output_data #>> '{rewardsSubmission,duration}', tl.output_data #>> '{rangePayment,duration}')::bigint as duration,
    to_timestamp(
            coalesce(tl.output_data #>> '{rewardsSubmission,startTimestamp}', tl.output_data #>> '{rangePayment,startTimestamp}')::bigint
                + coalesce(tl.output_data #>> '{rewardsSubmission,duration}', tl.output_data #>> '{rangePayment,duration}')::bigint
    )::timestamp(6) as end_timestamp,
    lower(t.entry ->> 'strategy') as strategy,
    (t.entry ->> 'multiplier')::numeric(78,0) as multiplier,
    t.strategy_index as strategy_index,
    tl.transaction_hash,
    tl.log_index,
    b.block_time::timestamp(6),
    to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
    tl.block_number
FROM transaction_logs tl
    LEFT JOIN blocks b ON (tl.block_sequence_id = b.id)
    CROSS JOIN LATERAL jsonb_array_elements(
        coalesce(tl.output_data #> '{rewardsSubmission,strategiesAndMultipliers}',tl.output_data #> '{rangePayment,strategiesAndMultipliers}')
    ) WITH ORDINALITY AS t(entry, strategy_index)
WHERE address = '{{ var('rewards_coordinator_address') }}'
AND event_name = 'AVSRewardsSubmissionCreated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### Rewards for All Submissions

```sql=

SELECT
  lower(tl.arguments #>> '{0,Value}') AS avs, -- Keeping as AVS for compatibility with unioning on range_payments.
  lower(tl.arguments #>> '{2,Value}') AS reward_hash,
  coalesce(lower(tl.output_data #>> '{rewardsSubmission}'), lower(tl.output_data #>> '{rangePayment}')) as rewards_submission,
  coalesce(lower(tl.output_data #>> '{rewardsSubmission, token}'), lower(tl.output_data #>> '{rangePayment, token}')) as token,
  coalesce(tl.output_data #>> '{rewardsSubmission,amount}', tl.output_data #>> '{rangePayment,amount}')::numeric(78,0) as amount,
  to_timestamp(coalesce(tl.output_data #>> '{rewardsSubmission,startTimestamp}', tl.output_data #>> '{rangePayment,startTimestamp}')::bigint)::timestamp(6) as start_timestamp,
  coalesce(tl.output_data #>> '{rewardsSubmission,duration}', tl.output_data #>> '{rangePayment,duration}')::bigint as duration,
  to_timestamp(
            coalesce(tl.output_data #>> '{rewardsSubmission,startTimestamp}', tl.output_data #>> '{rangePayment,startTimestamp}')::bigint
                + coalesce(tl.output_data #>> '{rewardsSubmission,duration}', tl.output_data #>> '{rangePayment,duration}')::bigint
    )::timestamp(6) as end_timestamp,
  lower(t.entry ->> 'strategy') as strategy,
  (t.entry ->> 'multiplier')::numeric(78,0) as multiplier,
  t.strategy_index as strategy_index,
  tl.transaction_hash,
  tl.log_index,
  b.block_time::timestamp(6),
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  tl.block_number
FROM transaction_logs tl
LEFT JOIN blocks b ON tl.block_sequence_id = b.id
CROSS JOIN LATERAL jsonb_array_elements(
    coalesce(tl.output_data #> '{rewardsSubmission,strategiesAndMultipliers}',tl.output_data #> '{rangePayment,strategiesAndMultipliers}')
) WITH ORDINALITY AS t(entry, strategy_index)
WHERE address = '{{ var('rewards_coordinator_address') }}'
AND event_name = 'RewardsSubmissionForAllCreated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### Rewards for All Earners Submissions

```sql=
SELECT
  lower(tl.arguments #>> '{0,Value}') AS avs, -- Keeping as AVS for compatibility with unioning on range_payments.
  lower(tl.arguments #>> '{2,Value}') AS reward_hash,
  coalesce(lower(tl.output_data #>> '{rewardsSubmission}'), lower(tl.output_data #>> '{rangePayment}')) as rewards_submission,
  coalesce(lower(tl.output_data #>> '{rewardsSubmission, token}'), lower(tl.output_data #>> '{rangePayment, token}')) as token,
  coalesce(tl.output_data #>> '{rewardsSubmission,amount}', tl.output_data #>> '{rangePayment,amount}')::numeric(78,0) as amount,
  to_timestamp(coalesce(tl.output_data #>> '{rewardsSubmission,startTimestamp}', tl.output_data #>> '{rangePayment,startTimestamp}')::bigint)::timestamp(6) as start_timestamp,
  coalesce(tl.output_data #>> '{rewardsSubmission,duration}', tl.output_data #>> '{rangePayment,duration}')::bigint as duration,
  to_timestamp(
            coalesce(tl.output_data #>> '{rewardsSubmission,startTimestamp}', tl.output_data #>> '{rangePayment,startTimestamp}')::bigint
                + coalesce(tl.output_data #>> '{rewardsSubmission,duration}', tl.output_data #>> '{rangePayment,duration}')::bigint
    )::timestamp(6) as end_timestamp,
  lower(t.entry ->> 'strategy') as strategy,
  (t.entry ->> 'multiplier')::numeric(78,0) as multiplier,
  t.strategy_index as strategy_index,
  tl.transaction_hash,
  tl.log_index,
  b.block_time::timestamp(6),
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  tl.block_number
FROM transaction_logs tl
LEFT JOIN blocks b ON tl.block_sequence_id = b.id
CROSS JOIN LATERAL jsonb_array_elements(
    coalesce(tl.output_data #> '{rewardsSubmission,strategiesAndMultipliers}',tl.output_data #> '{rangePayment,strategiesAndMultipliers}')
) WITH ORDINALITY AS t(entry, strategy_index)
WHERE address = '{{ var('rewards_coordinator_address') }}'
AND event_name = 'RewardsSubmissionForAllEarnersCreated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### Operator-Directed Rewards Submissions

```sql=
WITH _operator_directed_rewards as (
		SELECT
			odrs.avs,
			odrs.reward_hash,
			odrs.token,
			odrs.operator,
			odrs.operator_index,
			odrs.amount,
			odrs.strategy,
			odrs.strategy_index,
			odrs.multiplier,
			odrs.start_timestamp::TIMESTAMP(6),
			odrs.end_timestamp::TIMESTAMP(6),
			odrs.duration,
			odrs.block_number,
			b.block_time::TIMESTAMP(6),
			TO_CHAR(b.block_time, 'YYYY-MM-DD') AS block_date
		FROM operator_directed_reward_submissions AS odrs
		JOIN blocks AS b ON(b.number = odrs.block_number)
		WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
	)
	SELECT
		avs,
		reward_hash,
		token,
		operator,
		operator_index,
		amount,
		strategy,
		strategy_index,
		multiplier,
		start_timestamp::TIMESTAMP(6),
		end_timestamp::TIMESTAMP(6),
		duration,
		block_number,
		block_time,
		block_date
	FROM _operator_directed_rewards
```

### Operator<>AVS State

Every deregistration and registration from an Operator to an AVS is recorded in the AVSDirectory

#### Operator Registrations

```sql=
SELECT
  lower(t.arguments #>> '{0,Value}') as operator,
  lower(t.arguments #>> '{1,Value}') as avs,
  (t.output_data -> 'status')::int as status,
  t.transaction_hash,
  t.log_index,
  b.block_time,
  to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
  t.block_number
FROM transaction_logs t
LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '{{ var('avs_directory_address') }}'
AND t.event_name = 'OperatorAVSRegistrationStatusUpdated'
AND date_trunc('day', b.block_time) < TIMESTAMP '{{ var("cutoff_date") }}'
```

#### Operator Restaked Strategies

The AVS Directory **does not** emit an event for the strategies that an operator restakes or un-restakes on an AVS. To retrieve this information we run a cron job every 3600 blocks (ie. `blockNum % 3600 = 0`), starting when the AVSDirectory was deployed, that:

1. Retrieves all operators restaked on the AVS
2. Calls `getOperatorRestakedStrategies(address operator) returns (address[])` on each AVS's serviceManager contract

It is a requirement that AVSs are compliant with this interface, as referenced in our [docs](https://docs.eigenlayer.xyz/eigenlayer/avs-guides/avs-dashboard-onboarding)

Assuming that an operator is registered to an AVS at timestamp $t$, an example output of this cron job is:

| Operator  | AVS   | Strategy | Block Time |
| --------- | ----- | -------- | ---------- |
| Operator1 | AVS-A | stETH    | t          |
| Operator1 | AVS-A | rETH     | t          |
| Operator2 | AVS-A | rETH     | t          |
| Operator3 | AVS-B | cbETH    | t          |

# Data Transformation

Once we extract all logs and relevant storage of EigenLayer core contracts and AVSs, we transform this to create daily snapshots of state in two parts

1. Aggregation of extraction data into on-chain contract state
2. Combine state into ranges and unwind into daily snapshots

## Key Considerations

In part 2, once state has been aggregated, we unwind the ranges of state into daily snapshots.

**_Snapshots of state are rounded up to the nearest day, except for operator<>avs deregistrations, which are rounded down_**. Let's assume we have the following range with events A & B being updates for a staker's shares.

```
GENESIS_TIMESTAMP---------------------Day1---------------------Day2
                     ^       ^
                  A=100    B=200
```

The output of the snapshot transformation should denote that on Day1 the Staker has 200 shares. More generally, we take the _latest_ update in [Day$_{i-1}$, Day$_i$] range and set that to the state on Day$_i$. We refer to the reward on a given day as a _reward snapshot_.

### Operator<>AVS Registration/Deregistration

In the case of an operator registration and deregistration:

```
GENESIS_TIMESTAMP---------------------Day1---------------------Day2
                     ^                       ^
                  Register                Deregister
```

The end state is that the operator has registered & deregistered on day 1, resulting in no reward being made to the operator. We add this mechanism as a protection for operators gaining extra days of rewards if we were to round up deregistrations. The side effect is the following:

```
--------------Day1--------------Day2--------------Day3--------------Day4
          ^                                                       ^
        Register                                             Deregister
```

The operator in this case will be deregistered on Day3, resulting in the operator not receving any reward on the [Day3,Day4] range for which it was securing the AVS. Rounding down deregistrations is why the `cutoff_date` must be the _previous day_ at 0:00 UTC.

## Part 1: Aggregation

### Staker Shares

The LST shares for a staker, $s$, and strategy, $y$, is given by:

Shares$_{s,y}$ = Deposits$_{s,y}$ $-$ M1Withdrawals$_{s,y}$ $-$ M2Withdrawals$_{s,y}$

The Native ETH shares for a staker is the sum of all `PodSharesUpdated` events for a given staker. Note that Shares _can be negative in this event_.

NativeETHShares$_s$ = $\sum_{i=0}^{n}$ PodSharesUpdated$_i$- M1Withdrawals$_i$ - M2Withdrawals$_i$

Combining these two gives us the shares for a staker for every strategy for every update.

The key part of this query is:

```sql=
SUM(shares) OVER (PARTITION BY staker, strategy ORDER BY block_time, log_index) AS shares,
```

Which gets the **_running_** sum for every (staker, strategy) pair at every update.

```sql=
SELECT
    staker,
    strategy,
    -- Sum each share amount over the window to get total shares for each (staker, strategy) at every timestamp update */
    SUM(shares) OVER (PARTITION BY staker, strategy ORDER BY block_time, log_index) AS shares,
    transaction_hash,
    log_index,
    strategy_index,
    block_time,
    block_date,
    block_number
FROM (
    SELECT staker, strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM {{ ref('staker_deposits') }}

    UNION ALL

    -- Subtract m1 & m2 withdrawals
    SELECT staker, strategy, shares * -1, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM {{ ref('m1_staker_withdrawals') }}

    UNION ALL

    SELECT staker, strategy, shares * -1, strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM {{ ref('m2_staker_withdrawals') }}

    UNION all

    -- Shares in eigenpod are positive or negative, so no need to multiply by -1
    SELECT staker, '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM {{ ref('eigenpod_shares') }}
) combined_staker_shares

```

**_Note: Rewards For All will not pay out stakers who have not proven their beacon chain balances to the execution layer._**

### Operator Shares

The shares for an operator, $o$, for a strategy, $y$, is given by:
$Shares_{o,y} = ShareIncrease_{o,y} - ShareDecrease_{o,y}$

```sql=
SELECT
  operator,
  strategy,
  -- Sum each share amount over the window to get total shares for each (operator, strategy) at every timestamp update */
  SUM(shares) OVER (PARTITION BY operator, strategy ORDER BY block_time, log_index) AS shares,
  transaction_hash,
  log_index,
  block_time,
  block_date,
  block_number
FROM (
    SELECT operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number
    FROM {{ ref('operator_share_increases') }}

    UNION ALL

    SELECT operator, strategy, shares * -1 AS shares, transaction_hash, log_index, block_time, block_date, block_number
    FROM {{ ref('operator_share_decreases') }}
) combined_shares
```

### Staker Delegation Status

Here, we aggregate each delegation and undelegation into a single view. When a staker is undelegated, we mark its operator as `0x0000000000000000000000000000000000000000`.

```sql=
SELECT
  staker,
  CASE when src = 'undelegations' THEN '0x0000000000000000000000000000000000000000' ELSE operator END AS operator,
  transaction_hash,
  log_index,
  block_time,
  block_date,
  block_number
FROM (
    SELECT *, 'undelegations' AS src FROM {{ ref('staker_undelegations') }}
    UNION ALL
    SELECT *, 'delegations' AS src FROM {{ ref('staker_delegations') }}
) as delegations_combined
```

### Combined Rewards Submissions

Combines AVS Rewards Submissions and Rewards for All Submissions into one view, with an added `reward_type` parameter.

```sql=
SELECT *, 'avs' as reward_type from {{ ref('avs_reward_submissions') }}

UNION ALL

SELECT *, 'all_stakers' as reward_type from {{ ref('reward_submission_for_all') }}

UNION ALL

SELECT *, 'all_earners' as reward_type from {{ ref('reward_submission_for_all_earners') }}
```

### Operator AVS Statuses

Formats the [status tuple](https://github.com/Layr-Labs/eigenlayer-contracts/blob/90a0f6aee79b4a38e1b63b32f9627f21b1162fbb/src/contracts/interfaces/IAVSDirectory.sol#L8-L11) in the AVSDirectory as a `true` or `false`.

```sql=
SELECT
    operator,
    avs,
    CASE WHEN status = 1 then true ELSE false END AS registered,
    transaction_hash,
    log_index,
    block_date,
    block_time,
    block_number
FROM {{ ref('operator_avs_registrations') }}
```

## Part 2: Windows & Snapshots

Once we have transformed the on-chain state, we then aggregate the state into window of time for which the state was active. Lastly, the windows of state are unwound into daily snapshots.

The key design decision, which we explained in the [considerations above](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw?view#Key-Considerations2), is that **_state is always rounded up to the nearest day 0:00 UTC, except for operator<>avs deregistrations_**.

### Windows

#### Staker Share Windows

1. Ranked_staker_records: Ranks each record for a given staker, strategy and day. The lower the rank, the later the record is in the window

```sql=
WITH ranked_staker_records as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY staker, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM {{ ref('staker_shares') }}
),
```

2. Snapshotted_records: Select the latest record for each day. Round up the record to create a `snapshot_time` the latest record for each day & round up to the snapshot day

```sql=
 snapshotted_records as (
     SELECT
         staker,
         strategy,
         shares,
         block_time,
         date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
     from ranked_staker_records
     where rn = 1
 ),
```

3. Staker_share_windows: Get the range for each staker, strategy, shares

```sql=
staker_share_windows as (
     SELECT
     staker, strategy, shares, snapshot_time as start_time,
     CASE
         -- If the range does not have the end, use the current timestamp truncated to 0 UTC
         WHEN LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{ var("cutoff_date") }}')
         ELSE LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time)
         END AS end_time
     FROM snapshotted_records
 )
SELECT * from staker_share_windows
```

We make use of the `LEAD` operator, in the `staker_share_windows` CTE. This operator finds the next record for a given (`staker`, `strategy`) combination. The logic is:

- if there is no next record (ie. null), then set the `end_time` of the window to be the `cutoff_date`
- if there is a record, set the `end_time` of the current record's window to be the next record's `start_time`

_Note: The above logic can have (`staker`, `strategy`) combinations where the `end_record` of one record equal the `start_time` of the next record. This is handled when we unwind the windows into snapshots._

#### Operator Share Windows

```sql=
WITH ranked_operator_records as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY operator, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM {{ ref('operator_shares') }}
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
     operator,
     strategy,
     shares,
     block_time,
     date_trunc('day', block_time) + INTERVAL '1' day as snapshot_time
 from ranked_operator_records
 where rn = 1
),
-- Get the range for each operator, strategy pairing
operator_share_windows as (
 SELECT
     operator, strategy, shares, snapshot_time as start_time,
     CASE
         -- If the range does not have the end, use the current timestamp truncated to 0 UTC
         WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{ var("cutoff_date") }}')
         ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time)
         END AS end_time
 FROM snapshotted_records
)
SELECT * from operator_share_windows
```

This logic is the exact same as the Staker Share Windows.

#### Staker Delegation Windows

```sql=
with ranked_delegations as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY staker, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM {{ ref('staker_delegation_status') }}
),
-- Get the latest record for each day & round up to the snapshot day
 snapshotted_records as (
     SELECT
         staker,
         operator,
         block_time,
         date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
     from ranked_delegations
     where rn = 1
 ),
-- Get the range for each staker
 staker_delegation_windows as (
     SELECT
         staker, operator, snapshot_time as start_time,
         CASE
             -- If the range does not have the end, use the cutoff date truncated to 0 UTC
             WHEN LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{ var("cutoff_date") }}')
             ELSE LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time)
             END AS end_time
     FROM snapshotted_records
 )
SELECT * from staker_delegation_windows

```

This logic is the exact same as the Staker Share Windows.

#### Operator AVS Registration Windows

This calculation is different from the above 3 queries because registration windows cannot continue off each other. For example, if an operator had the following state:

| Operator  | AVS  | Registered | Date      |
| --------- | ---- | ---------- | --------- |
| Operator1 | AVS1 | True       | 4-24-2024 |
| Operator1 | AVS1 | False      | 4-26-2024 |
| Operator1 | AVS1 | True       | 4-29-2024 |
| Operator1 | AVS1 | False      | 5-1-2024  |
| Operator1 | AVS1 | True       | 5-1-2024  |
| Operator1 | AVS1 | False      | 5-1-2024  |

We do not care about a (deregistration, registration) window from rows 2 and 3. We also need to discard the (registration,deregistration window) from rows 5 and 6.

1. Mark a link between each registration

```sql=
WITH marked_statuses AS (
    SELECT
        operator,
        avs,
        registered,
        block_time,
        block_date,
        -- Mark the next action as next_block_time
        LEAD(block_time) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_time,
        -- The below lead/lag combinations are only used in the next CTE
        -- Get the next row's registered status and block_date
        LEAD(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_registration_status,
        LEAD(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_date,
        -- Get the previous row's registered status and block_date
        LAG(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_registered,
        LAG(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_block_date
    FROM {{ ref('operator_avs_status') }}
),
```

2. Ignore a (registration, deregistration) pair that occurs on the same day. This is to ensure that the grouping is not counted once we round deregistration down and registration up.

```sql=
 removed_same_day_deregistrations AS (
     SELECT * from marked_statuses
     WHERE NOT (
         -- Remove the registration part
         (registered = TRUE AND
          COALESCE(next_registration_status = FALSE, false) AND -- default to false if null
          COALESCE(block_date = next_block_date, false)) OR
             -- Remove the deregistration part
         (registered = FALSE AND
          COALESCE(prev_registered = TRUE, false) and
          COALESCE(block_date = prev_block_date, false)
             )
         )
 ),
```

3. Mark the`end_time` of the record as the next record. If it's not the case, mark the `end_time` as the `cutoff_date`

```sql=
 registration_periods AS (
     SELECT
         operator,
         avs,
         block_time AS start_time,
         -- Mark the next_block_time as the end_time for the range
         -- Use coalesce because if the next_block_time for a registration is not closed, then we use cutoff_date
         COALESCE(next_block_time, TIMESTAMP '{{ var("cutoff_date") }}') AS end_time,
         registered
     FROM removed_same_day_deregistrations
     WHERE registered = TRUE
 ),
```

4. Round up each `start_time` and round down each end_time

```sql=
 registration_windows_extra as (
     SELECT
         operator,
         avs,
         date_trunc('day', start_time) + interval '1' day as start_time,
         -- End time is end time non inclusive becuase the operator is not registered on the AVS at the end time OR it is current timestamp rounded up
         date_trunc('day', end_time) as end_time
     FROM registration_periods
 ),
```

5. Get rid of records with the same `start_time` and `end_time`:

```sql=
 operator_avs_registration_windows as (
     SELECT * from registration_windows_extra
     WHERE start_time != end_time
 )
select * from operator_avs_registration_windows
```

#### Operator AVS Strategy Windows

This query aggregates entries from the [operator restaked strategies](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw?view#Operator-Restaked-Strategies) cron job into windows.

1. Order all records. Round up `block_time` to 0 UTC. Log_index is irrelevant since this data is generated from RPC calls.

```sql=
with ranked_records AS (
    SELECT
        lower(operator) as operator,
        lower(avs) as avs,
        lower(strategy) as strategy,
        block_time,
        date_trunc('day', CAST(block_time as timestamp(6))) + interval '1' day as start_time,
        ROW_NUMBER() OVER (
            PARTITION BY operator, avs, strategy, date_trunc('day', CAST(block_time as timestamp(6))) + interval '1' day
            ORDER BY block_time DESC -- want latest records to be ranked highest
            ) AS rn
    -- Cannot use ref here because this table is not generated via DBT
    FROM public.operator_restaked_strategies
    -- testnet and holesky all exist together in the blocklake so the avs_directory_address allows us to filter
    WHERE avs_directory_address = lower('{{ var('avs_directory_address') }}')
),
```

2. Get the latest records for each (`operator`, `avs`, `strategy`, `day`) combination

```sql=
 latest_records AS (
     SELECT
         operator,
         avs,
         strategy,
         start_time,
         block_time
     FROM ranked_records
     WHERE rn = 1
 ),
```

3. Find the next entry for each (`operator`, `avs`, `strategy`) grouping.

```sql=
 grouped_records AS (
     SELECT
         operator,
         avs,
         strategy,
         start_time,
         LEAD(start_time) OVER (
             PARTITION BY operator, avs, strategy
             ORDER BY start_time ASC
             ) AS next_start_time
     FROM latest_records
 ),
```

4. Parse out any holes (ie. any `next_start_times` that are not exactly one day after the current record's `start_time`). This is because the operator would have deregistered from the AVS.

```sql=
 parsed_ranges AS (
     SELECT
         operator,
         avs,
         strategy,
         start_time,
         -- If the next_start_time is not on the consecutive day, close off the end_time
         CASE
             WHEN next_start_time IS NULL OR next_start_time > start_time + INTERVAL '1' DAY THEN start_time
             ELSE next_start_time
             END AS end_time
     FROM grouped_records
 ),
```

5. Remove any records where `start_time` == `end_time`

```sql=
 active_windows as (
     SELECT *
     FROM parsed_ranges
     WHERE start_time != end_time
 ),
```

6. We now use the gaps and islands algorithm to find contiguous groups of (`operator`, `avs`, `strategy`) combinations. First, we mark the `prev_end_time` for each row. If there is a new window, then the gap is empty

```sql=
 gaps_and_islands AS (
     SELECT
         operator,
         avs,
         strategy,
         start_time,
         end_time,
         LAG(end_time) OVER(PARTITION BY operator, avs, strategy ORDER BY start_time) as prev_end_time
     FROM active_windows
 ),
```

7. Next we, detect islands. If the `prev_end_time` is the same as the `start_time` then the records are part of the same continguous grouping.

```sql=
 island_detection AS (
     SELECT operator, avs, strategy, start_time, end_time, prev_end_time,
            CASE
                -- If the previous end time is equal to the start time, then mark as part of the island, else create a new island
                WHEN prev_end_time = start_time THEN 0
                ELSE 1
                END as new_island
     FROM gaps_and_islands
 ),
```

8. Create groups based on each records `new_island`. Rows that have the same `island_id` sum are part of the same grouping

```sql=
 island_groups AS (
     SELECT
         operator,
         avs,
         strategy,
         start_time,
         end_time,
         SUM(new_island) OVER (
             PARTITION BY operator, avs, strategy ORDER BY start_time
             ) AS island_id
     FROM island_detection
 ),
```

9. Coalesce groups together

```sql=
 operator_avs_strategy_windows AS (
     SELECT
         operator,
         avs,
         strategy,
         MIN(start_time) AS start_time,
         MAX(end_time) AS end_time
     FROM island_groups
     GROUP BY operator, avs, strategy, island_id
     ORDER BY operator, avs, strategy, start_time
 )
select * from operator_avs_strategy_windows
```

### Snapshots

**Once we've created windows for each table of core contract state, we unwind these windows into daily snapshots. In each of the below queries, we round down the `end_time` by one day because a new record can begin on the same day OR it will be included in a separate pipeline run after the `cutoff_date`.**

#### Staker Share Snapshot

```sql=
WITH cleaned_records as (
  SELECT * FROM {{ ref('staker_share_windows')}}
  WHERE start_time < end_time
)
SELECT
    staker,
    strategy,
    shares,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
```

We remove any records with erroneous `start_time` and `end_time` values. Then, we unwind the entire range.

#### Operator Share Snapshot

```sql=
WITH cleaned_records as (
    SELECT * FROM {{ ref('operator_share_windows')}}
        WHERE start_time < end_time
)
SELECT
    operator,
    strategy,
    shares,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
        CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
```

#### Staker Delegation Snapshots

```sql=
WITH cleaned_records as (
    SELECT * FROM {{ ref('staker_delegation_windows') }}
        WHERE start_time < end_time
)
SELECT
    staker,
    operator,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
        CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
```

#### Operator AVS Strategy Snapshots

```sql=
WITH cleaned_records AS (
    SELECT * FROM {{ ref('operator_avs_strategy_windows') }}
        WHERE start_time < end_time
)
SELECT
    operator,
    avs,
    strategy,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
        CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
```

#### Operator AVS Registration Snapshots

```sql=
WITH cleaned_records AS (
    SELECT * FROM {{ ref('operator_avs_registration_windows') }}
        WHERE start_time < end_time
)
SELECT
    operator,
    avs,
    day AS snapshot
FROM cleaned_records
CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
```

# Reward Calculation

## Key Considerations

### Calculation Ranges

Rewards distributions are calculated from daily snapshots of state. For example, if we had a rewards submission for the following range:

```
Day0------Day1------Day2------Day3------Day4------Day5------Day6------Day7

```

The rewards pipeline would calculate a reward distribution from 7 snapshots of state: Day1, Day2,... Day7. We include the last day (Day7) as a snapshot instead of the first day (Day0). You can think of each snapshot as representing the most recent state update from the last 24 hours. Said another way, $Day_i$ represents the most recent state from [$Day_{i-1}$ 00:00 UTC, $Day_{i-1}$ 23:59 UTC].

### State Entry/Exit

Since snapshots are rounded _up_ to the nearest day, except for operator<>avs deregistrations, state updates that are within the same day will receive the exact same reward amount.

For example, assuming stakers A and B have equivalent amounts of stake and are opted into the same operator at the below times, their reward for the Day 2 `rewardSnaphot` will be equal:

```
Day1---------------------Day2
      ^               ^
      A               B
```

This is a known side-effect of computing rewards from daily snapshots.

In addition, exiting from the system holds the same property. If Operators J and K exit from the same AVS within the same day, they will both not receive rewards from the AVS:

```
Day1---------------------Day2
      ^               ^
      J               K
```

As explained above in the transformation section, **a known side effect is that an operator can lose a day's worth of reward by deregistering from an AVS at the bounds of 2 snapshots** For example:

```
--------Day1---------------------Day2---------------------Day3
      ^                                                 ^
     Entry                                             Exit
```

In the above scenario, the operator will count as registered on Day1 and deregistered on Day2, even though they've only validated the AVS for nearly 2 days.

### Multiplier Calculation

Every rewards submission has two arrays of equivalent length: `strategies` and `multiplier`. The pipeline uses this value to calculate the _stakeWeight_ of earners for snapshot rewards. For a given staker, $s$, on snapshot, $d$, the stakeWeight is given by:

$stakeWeight_{s, d} = multiplier_i \cdot shares_{i,s,d}$

The calculation is also done in an AVS's `StakeRegistry` contract. [Reference solidity implementation](https://github.com/Layr-Labs/eigenlayer-middleware/blob/9968b1d99f5b85053665fdbc7657edf6d64c053e/src/StakeRegistry.sol#L484).

### Token Reward Amounts

A key invariant is that for a given rewards submission, $r$, on the reward snapshot, $d$, $Tokens_{r,d} >= \sum_{i=0}^{n=paidEarners} Earner_{i,r,d}$

In other words, the `tokensPerDay` of a rewards submission cannot be less than the sum of the reward distribution to all earners for the `rewardSnaphot`. We call this out as a key consideration from the truncation when converting shares and multipliers the double type, which holds up to 15 significant digits.

### Reward Aggregation

The`RewardsCoordinator` requires that `CALCULATION_INTERVAL_SECONDS % SNAPSHOT_CADENCE == 0`, which guarantees that every reward snapshot will lie within the bounds of a reward range.

At some cadence defined by the reward updater, the pipeline will aggregate all reward distribution snapshots up to some timestamp, $t$. For the root to be "net-new", it must merkleize state that is after a `rewardSnaphot` that is greater than the `lastRewardTimestamp`.

### Lack of reward rollover

If an AVS has made a reward for a snapshot where there are no strategies restaked, the reward will not be redistributed to future snapshots of the rewards submission. See [reward snapshot operators](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw?view#Reward-Snapshot-Operators) a concrete example.

## 1. Get Active Rewards

Each of the below queries is a set of CTEs that are part of the `active_rewards` view.

To handle retroactive rewards we look for any reward that has started prior to the `cutoff_date`. To ensure that rewards are not recalculated, we clamp the range of snapshots for which the active reward is calculated for.

The following table will be used to aid in visualizing the transformations. Let's assume the `cutoff_date` for the pipeline is 4-29-2024 at 0:00 UTC. In addition, assume that the last reward snapshot was 4-24-2024. _Note: Because of the bronze table queries & cutoff date, reward events take 2 days to show up in the gold calculation for the current snapshot._

| AVS  | Reward Hash | Start Timestamp | Duration | End Timestamp | Amount | Strategy | Multiplier |
| ---- | ----------- | --------------- | -------- | ------------- | ------ | -------- | ---------- |
| AVS1 | 0xReward1   | 4-21-2024       | 21 days  | 5-12-2024     | 21e18  | stETH    | 1e18       |
| AVS1 | 0xReward1   | 4-21-2024       | 21 days  | 5-12-2024     | 21e18  | rETH     | 2e18       |

For brevity, only the relevant rows of the table are displayed in each example.

### Active Rewards

```sql=
WITH active_rewards_modified as (
    SELECT *,
           amount/(duration/86400) as tokens_per_day,
           cast('{{ var("cutoff_date") }}' AS TIMESTAMP(6)) as global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
    FROM {{ ref('rewards_combined') }}
        WHERE end_timestamp >= TIMESTAMP '{{ var("rewards_start") }}' and start_timestamp <= TIMESTAMP '{{ var("cutoff_date") }}'
),
```

- We divide the amount by the number of days in the rewards submission duration to get the `tokens_per_day`. This is the number of tokens distributed to all earners in a given `rewardSnapshot`
- There can be multiple `rewardsSnapshot` in a given run because for each rewards submission, we can have multiple snapshots to calculate rewards for in a pipeline run
- `global_end_inclusive` is the last snapshot for this pipeline run for which to calculate rewards. It is the same as the `cutoff_date`
- The `end_timestamp` is greater than the start of UNIX time but will be properly handled in the next steps

| Tokens Per Day | Global End Inclusive |
| -------------- | -------------------- |
| 1e18           | 4-27-2024            |

### Active Rewards Updated End Timestamps

```sql=
 active_rewards_updated_end_timestamps as (
     SELECT
         avs,
         /**
          * Cut the start and end windows to handle
          * A. Retroactive rewards that came recently whose start date is less than start_timestamp
          * B. Don't make any rewards past end_timestamp for this run
          */
         start_timestamp as reward_start_exclusive,
         LEAST(global_end_inclusive, end_timestamp) as reward_end_inclusive,
         tokens_per_day,
         token,
         multiplier,
         strategy,
         reward_type,
         reward_for_all,
         global_end_inclusive,
         block_date as reward_submission_date
     FROM active_rewards_modified
),
```

- The rewards submission's original `start_timestamp` is renamed to `reward_start_exclusive`. It is marked as exclusive because we either have already taken a snapshot at this time in a previous run OR it is the day 0 of a rewards submission and no snapshot will be taken (see [calculation ranges](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw?view#Calculation-Ranges))
- `reward_end_inclusive` is the `MIN(global_end_inclsuive, end_timestamp)`. This is clamping the `end_timestamp` to be no greater than `cutoff_time` for the given run

| Reward Start Exclusive | Reward End Inclusive |
| ---------------------- | -------------------- |
| 4-21-2024              | 4-27-2024            |

### Active Rewards Updated Start Timestamps

```sql=
-- For each reward hash, find the latest snapshot
 active_rewards_updated_start_timestamps as (
     SELECT
         ap.avs,
         CASE
             WHEN '{{ var("is_backfill") }}' = 'true' THEN ap.reward_start_exclusive
             ELSE COALESCE(MAX(g.snapshot), ap.reward_start_exclusive)
             END as reward_start_exclusive,
         ap.reward_end_inclusive,
         ap.token,
         ap.tokens_per_day as tokens_per_day_decimal,
         -- Round down to 15 sigfigs for double precision, ensuring know errouneous round up or down
         ap.tokens_per_day * ((POW(10, 15) - 1)/(POW(10, 15))) as tokens_per_day,
         ap.multiplier,
         ap.strategy,
         ap.reward_hash,
         ap.reward_type,
         ap.global_end_inclusive,
         ap.reward_submission_date
     FROM active_rewards_updated_end_timestamps ap
              LEFT JOIN {{ var('schema_name') }}.gold_table g
                        ON g.reward_hash = ap.reward_hash
     GROUP BY ap.avs, ap.reward_end_inclusive, ap.token, ap.tokens_per_day, ap.multiplier, ap.strategy, ap.reward_hash, ap.global_end_inclusive, ap.reward_start_exclusive, ap.reward_for_all
 ),
```

Clamps the `reward_start_exclusive` based on the most recent `snapshot` for the `reward_hash` from the final gold table, which contains every `snapshotReward`. We do this so we do not recalculate rewards. If there is no snapshot in the gold table, then we just use the rewards submission's original `reward_start_exclusive`

This step also casts `tokens_per_day` as a double, which will be used in the rest of the reward calculation. This allows us to support values up to 1e38-1, at the cost of only having 15 decimals of precision.

Lastly, if the run is a backfill, we make the start timestamp as the earliest unix timestamp possible`1970-01-01 00:00:00`.

Let's assume that the most recent `snapshotReward` for the reward_hash is 4-24-2024. The previous value for `reward_start_exclusive` was 4-21-2024.

| Reward start exclusive | Reward end inclusive |
| ---------------------- | -------------------- |
| 4-24-2024              | 4-28-2024            |

### Active Reward Ranges

```sql=
 active_reward_ranges AS (
     SELECT * from active_rewards_updated_start_timestamps
     /** Take out (reward_start_exclusive, reward_end_inclusive) windows where
      * 1. reward_start_exclusive >= reward_end_inclusive: The reward period is done or we will handle on a subsequent run
     */
     WHERE reward_start_exclusive < reward_end_inclusive
 ),
```

Parse out invalid ranges. This can occur if the current run is a backfill and there was a snapshot from a previous run that was greater than the `cutoff_time` at which we are backfilling.

### Unwinded Active Reward Ranges

```sql=
 exploded_active_range_rewards AS (
     SELECT * FROM active_reward_ranges
     CROSS JOIN generate_series(DATE(reward_start_exclusive), DATE(reward_end_inclusive), INTERVAL '1' DAY) AS day
 ),
```

Create a row for each snapshot in the reward range

| AVS  | Reward Hash | Day       | Strategy | Multiplier | ... |
| ---- | ----------- | --------- | -------- | ---------- | --- |
| AVS1 | 0xReward1   | 4-24-2024 | stETH    | 1e18       |
| AVS1 | 0xReward1   | 4-25-2024 | stETH    | 1e18       |
| AVS1 | 0xReward1   | 4-26-2024 | stETH    | 1e18       |
| AVS1 | 0xReward1   | 4-27-2024 | stETH    | 1e18       |
| AVS1 | 0xReward1   | 4-24-2024 | rETH     | 2e18       |
| AVS1 | 0xReward1   | 4-25-2024 | rETH     | 2e18       |
| AVS1 | 0xReward1   | 4-26-2024 | rETH     | 2e18       |
| AVS1 | 0xReward1   | 4-27-2024 | rETH     | 2e18       |

### Final Active Reward

```sql=
 active_rewards_final AS (
     SELECT
         avs,
         cast(day as DATE) as snapshot,
         token,
         tokens_per_day,
         tokens_per_day_decimal,
         multiplier,
         strategy,
         reward_hash,
         reward_type,
         reward_submission_date
     FROM exploded_active_range_rewards
     -- Remove snapshots on the start day
     WHERE day != reward_start_exclusive
 )
select * from active_rewards_final
```

Remove rows whose snapshot are equal to the reward's reward_start_exclusive.

| AVS         | Reward Hash      | Snapshot         | Strategy     | Multiplier  | Reward Start Exclusive |
| ----------- | ---------------- | ---------------- | ------------ | ----------- | ---------------------- |
| <s>AVS1</s> | <s>0xReward1</s> | <s>4-24-2024</s> | <s>stETH</s> | <s>1e18</s> | <s>4-24-2024</s>       |
| AVS1        | 0xReward1        | 4-25-2024        | stETH        | 1e18        | 4-24-2024              |
| AVS1        | 0xReward1        | 4-26-2024        | stETH        | 1e18        | 4-24-2024              |
| AVS1        | 0xReward1        | 4-27-2024        | stETH        | 1e18        | 4-24-2024              |
| <s>AVS1</s> | <s>0xReward1</s> | <s>4-24-2024</s> | <s>rETH</s>  | <s>2e18</s> | <s>4-24-2024</s>       |
| AVS1        | 0xReward1        | 4-25-2024        | rETH         | 2e18        | 4-24-2024              |
| AVS1        | 0xReward1        | 4-26-2024        | rETH         | 2e18        | 4-24-2024              |
| AVS1        | 0xReward1        | 4-27-2024        | rETH         | 2e18        | 4-24-2024              |

## 2. Staker Reward Amounts

After generating the active rewards, the pipeline then calculates the reward distribution to the staker operator set.

We cast the `multiplier` of a rewards submission and `shares` of a staker to a double in order to do computation on these values. The double type has 15 significant digits, and this imprecision is reflected in the CTEs that calculate the staker's `stakeWeight`, `staker_proportion`, `total_staker_operator_reward`, and `staker_tokens`. **It must be the case that, for a given rewards submission, $r$, $\sum_{i=0}^{n=avsStakerOperatorSet} stakeroperatorSetReward_{i, r} <= tokensPerDay_r$.**

We first calculate the reward distribution to the entire staker operator set and then pass it down to operators and stakers.

### Reward Snapshot Operators

```sql=
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
  FROM {{ ref('1_active_rewards') }} ap
  JOIN {{ ref('operator_avs_registration_snapshots') }} oar
  ON ap.avs = oar.avs and ap.snapshot = oar.snapshot
  WHERE ap.reward_type = 'avs'
),
```

From the `active_rewards`, get the operators that are registered for the AVS. We filter on `reward_for_all = false` since we are only looking at AVS rewards submissions.

Let's assume that operator registration snapshots for the AVS are. 4-27-2024 is the active snapshot, but the bronze table will not have events from this date (since the query is for all events < `cutoffDate`). **No staker or operator will be paid out for 4-27-2024 on this run. This is where the two snapshot delay comes from.**

For days where the are no operators opted into the AVS, the `tokens_per_day` of 1e18 will not be redistributed to future `rewardSnapshots`.

| Operator  | AVS  | Snapshot  |
| --------- | ---- | --------- |
| Operator1 | AVS1 | 4-25-2024 |
| Operator1 | AVS1 | 4-26-2024 |
| Operator2 | AVS1 | 4-25-2024 |
| Operator2 | AVS1 | 4-26-2024 |

### Operator Restaked Strategies

```sql=
_operator_restaked_strategies AS (
  SELECT
    rso.*
  FROM reward_snapshot_operators rso
  JOIN {{ ref('operator_avs_strategy_snapshots') }} oas
  ON
    rso.operator = oas.operator AND
    rso.avs = oas.avs AND
    rso.strategy = oas.strategy AND
    rso.snapshot = oas.snapshot
),
```

From the operators that are restaked on the AVS, get the strategies and shares and shares for each AVS they have restaked on.

Let's assume the strategies for each operator on the AVS are:

| Operator  | AVS  | Strategy | Snapshot  |
| --------- | ---- | -------- | --------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 |
| Operator1 | AVS1 | stETH    | 4-26-2024 |
| Operator1 | AVS1 | rETH     | 4-26-2024 |
| Operator2 | AVS1 | stETH    | 4-25-2024 |
| Operator2 | AVS1 | stETH    | 4-26-2024 |

### Staker Delegated Operators

```sql=
staker_delegated_operators AS (
  SELECT
    ors.*,
    sds.staker
  FROM _operator_restaked_strategies ors
  JOIN {{ ref('staker_delegation_snapshots') }} sds
  ON
    ors.operator = sds.operator AND
    ors.snapshot = sds.snapshot
),
```

Get the stakers that were delegated to the operator for the snapshot.

| Operator  | AVS  | Strategy | Snapshot  | Staker  |
| --------- | ---- | -------- | --------- | ------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 |
| Operator1 | AVS1 | rETH     | 4-26-2024 | Staker1 |
| Operator1 | AVS1 | rETH     | 4-26-2024 | Staker2 |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 |

### Staker Strategy Shares

```sql=
staker_avs_strategy_shares AS (
  SELECT
    sdo.*,
    sss.shares
  FROM staker_delegated_operators sdo
  JOIN {{ ref('staker_share_snapshots') }} sss
  ON
    sdo.staker = sss.staker AND
    sdo.snapshot = sss.snapshot AND
    sdo.strategy = sss.strategy
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE sss.shares > 0 and sdo.multiplier != 0
),
```

Let's assume that stakers have the following state:

| Staker  | Strategy | Shares | Snapshot  |
| ------- | -------- | ------ | --------- |
| Staker1 | stETH    | 1e18   | 4-25-2024 |
| Staker1 | stETH    | 1e18   | 4-26-2024 |
| Staker1 | rETH     | 2e18   | 4-26-2024 |
| Staker2 | stETH    | 1e18   | 4-26-2024 |
| Staker3 | stETH    | 2e18   | 4-25-2024 |
| Staker3 | stETH    | 2e18   | 4-26-2024 |

The join would produce the following:

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Shares |
| --------- | ---- | -------- | --------- | ------- | ------ |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 1e18   |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 1e18   |
| Operator1 | AVS1 | rETH     | 4-26-2024 | Staker1 | 2e18   |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 1e18   |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 2e18   |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 2e18   |

Staker2 has no `rETH` shares, which is why this view has 1 less row.

### Staker Weights

```sql=
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM staker_avs_strategy_shares
),
```

Calculates the `stakeWeight` of the staker. A discussion on this calculation is [above](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw?view#Multiplier-Calculation).

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Shares | Multiplier | Staker Weight |
| --------- | ---- | -------- | --------- | ------- | ------ | ---------- | ------------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 1e18   | 1e18       | 1e36          |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 1e18   | 1e18       | 3e36          |
| Operator1 | AVS1 | rETH     | 4-26-2024 | Staker1 | 1e18   | 2e18       | 3e36          |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 1e18   | 1e18       | 1e36          |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 2e18   | 1e18       | 2e36          |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 2e18   | 1e18       | 2e36          |

### Distinct Stakers

```sql=
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
```

In the previous calculation, the stake weight is on a per (`reward_hash`, `staker`, `snapshot`). We need to delete any rows with the same combination since the next step is to calculate the total staker weight per snapshot. The strategy column is irrelevant because the final reward is given by
$tokensPerDay * stakerWeightProportion$. We add the order by clause in order to have the sum of staker weight in the next step be deterministic.

Remove rows with with same (`staker`, `reward_hash`, `snapshot`)

| Operator           | AVS           | Strategy      | Snapshot           | Staker           | Shares        | Multiplier    | Staker Weight |
| ------------------ | ------------- | ------------- | ------------------ | ---------------- | ------------- | ------------- | ------------- |
| Operator1          | AVS1          | stETH         | 4-25-2024          | Staker1          | 1e18          | 1e18          | 1e36          |
| Operator1          | AVS1          | stETH         | 4-26-2024          | Staker1          | 1e18          | 1e18          | 3e36          |
| <s> Operator1 </s> | <s> AVS1 </s> | <s> rETH </s> | <s> 4-26-2024 </s> | <s> Staker1 </s> | <s> 1e18 </s> | <s> 2e18 </s> | <s> 3e36 </s> |
| Operator1          | AVS1          | stETH         | 4-26-2024          | Staker2          | 1e18          | 1e18          | 1e36          |
| Operator2          | AVS1          | stETH         | 4-25-2024          | Staker3          | 2e18          | 1e18          | 2e36          |
| Operator2          | AVS1          | stETH         | 4-26-2024          | Staker3          | 2e18          | 1e18          | 2e36          |

### Staker Weight Sum

```sql=
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
  FROM distinct_stakers
),
```

Get the sum of all staker weights for a given (`reward_hash`, `snapshot`)

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Shares | Staker Weight | Total Staker Weight |
| --------- | ---- | -------- | --------- | ------- | ------ | ------------- | ------------------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 1e18   | 1e36          | 3e36                |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 1e18   | 3e36          | 6e36                |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 1e18   | 1e36          | 6e36                |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 2e18   | 2e36          | 3e36                |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 2e18   | 2e36          | 6e36                |

### Staker Proportion

```sql=
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
```

Calculate the staker's proportion of tokens for the `snapshotReward` **We round down the `staker_proportion` to ensure that the sum of staker_proportions is not greater than 1**.

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Shares | Staker Weight | Total Staker Weight | Staker Proportion |
| --------- | ---- | -------- | --------- | ------- | ------ | ------------- | ------------------- | ----------------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 1e18   | 1e36          | 3e36                | 0.333             |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 1e18   | 3e36          | 6e36                | 0.5               |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 1e18   | 1e36          | 6e36                | 0.166             |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 2e18   | 2e36          | 3e36                | 0.666             |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 2e18   | 2e36          | 6e36                | 0.333             |

### Total Tokens

```sql=
staker_operator_total_tokens AS (
  SELECT *,
    CASE
      -- For snapshots that are before the hard fork AND submitted before the hard fork, we use the old calc method
      WHEN snapshot < '{{ var("amazon_hard_fork") }}' AND reward_submission_date < '{{ var("amazon_hard_fork") }}' THEN
        cast(staker_proportion * tokens_per_day AS DECIMAL(38,0))
      WHEN snapshot < '{{ var("nile_hard_fork") }}' AND reward_submission_date < '{{ var("nile_hard_fork") }}' THEN
        (staker_proportion * tokens_per_day)::text::decimal(38,0)
      ELSE
        FLOOR(staker_proportion * tokens_per_day_decimal)
    END as total_staker_operator_payout
  FROM staker_proportion
),
```

We have had to hard fork the code 2 times:

1. Round down to text
2. Use decimal instead of double everywhere.

This code reflects handling this edge case to make calculations idempotent in the cases of a backfill.

Calculate the number of tokens that will be paid out to all stakers and operators. We cast the result from text to `DECIMAL(38,0)` to not lose any precision. Furthemore, the token value fits within this type for a given snapshot.

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Shares | Staker Weight | Total Staker Weight | Staker Proportion | Tokens Per Day | Total Staker Operator Reward |
| --------- | ---- | -------- | --------- | ------- | ------ | ------------- | ------------------- | ----------------- | -------------- | ---------------------------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 1e18   | 1e36          | 3e36                | 0.333             | 1e18           | 333333333333333000           |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 1e18   | 3e36          | 6e36                | 0.5               | 1e18           | 5e17                         |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 1e18   | 1e36          | 6e36                | 0.166             | 1e18           | 166666666666666000           |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 2e18   | 2e36          | 3e36                | 0.666             | 1e18           | 666666666666666000           |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 2e18   | 2e36          | 6e36                | 0.333             | 1e18           | 333333333333333000           |

### Token Breakdowns

```sql=
token_breakdowns AS (
  SELECT *,
    CASE
      WHEN snapshot < '{{ var("amazon_hard_fork") }}' AND reward_submission_date < '{{ var("amazon_hard_fork") }}' THEN
        cast(total_staker_operator_payout * 0.10 AS DECIMAL(38,0))
      WHEN snapshot < DATE '{{ var("nile_hard_fork") }}' AND reward_submission_date < '{{ var("nile_hard_fork") }}' THEN
        (total_staker_operator_payout * 0.10)::text::decimal(38,0)
      ELSE
        floor(total_staker_operator_payout * 0.10)
    END as operator_tokens,
    CASE
      WHEN snapshot < '{{ var("amazon_hard_fork") }}' AND reward_submission_date < '{{ var("amazon_hard_fork") }}' THEN
        total_staker_operator_payout - cast(total_staker_operator_payout * 0.10 as DECIMAL(38,0))
      WHEN snapshot < '{{ var("nile_hard_fork") }}' AND reward_submission_date < '{{ var("nile_hard_fork") }}' THEN
        total_staker_operator_payout - ((total_staker_operator_payout * 0.10)::text::decimal(38,0))
      ELSE
        total_staker_operator_payout - floor(total_staker_operator_payout * 0.10)
    END as staker_tokens
  FROM staker_operator_total_tokens
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
```

Calculate the number of tokens owed to the operator and to the staker. Operators get a fixed 10% commission. Casting from text to `DECIMAL(38,0)` will only truncate the decimal proportion of the number (ie. there will be no rounding). We add the order by clause in order to have the sum of staker weight in the next query be deterministic.

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Shares | Staker Weight | Total Staker Weight | Staker Proportion | Tokens Per Day | Total Staker Operator Reward | Operator Tokens   | Staker Tokens      |
| --------- | ---- | -------- | --------- | ------- | ------ | ------------- | ------------------- | ----------------- | -------------- | ---------------------------- | ----------------- | ------------------ |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 1e18   | 1e36          | 3e36                | 0.333             | 1e18           | 333333333333333000           | 33333333333333300 | 299999999999999700 |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 1e18   | 3e36          | 6e36                | 0.5               | 1e18           | 5e17                         | 5e16              | 4.5e17             |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 1e18   | 1e36          | 6e36                | 0.166             | 1e18           | 166666666666666000           | 16666666666666600 | 149999999999999400 |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 2e18   | 2e36          | 3e36                | 0.666             | 1e18           | 666666666666666000           | 66666666666666600 | 599999999999999400 |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 2e18   | 2e36          | 6e36                | 0.333             | 1e18           | 333333333333333000           | 33333333333333300 | 299999999999999700 |

## 3. Operator Reward Amounts

We can calculate the operator reward distribution from the sum of its staker's reward distribution because the shares for an operator, $o$, is given by:

$Shares_{o} = \sum_{i=0}^{n=operatorStakers} Shares_i$

### Operator Token Sums

```sql=
WITH operator_token_sums AS (
  SELECT
    reward_hash,
    snapshot,
    token,
    tokens_per_day,
    avs,
    strategy,
    multiplier,
    reward_type,
    operator,
    SUM(operator_tokens) OVER (PARTITION BY operator, reward_hash, snapshot) AS operator_tokens
  FROM {{ ref('2_staker_reward_amounts') }}
),
```

Gets the sum for each operator for a given `reward_hash` and `snapshot`. The tokens per day was `1e18`. If we take the operators reward distribution on `4-25-26`, that is roughly 10% of 1e18. In addition, the rows 2 and 3 are equivalent.

| Operator  | AVS  | Strategy | Snapshot  | Staker  | Operator Tokens   | Staker Tokens      | Operator Tokens (Sum) |
| --------- | ---- | -------- | --------- | ------- | ----------------- | ------------------ | --------------------- |
| Operator1 | AVS1 | stETH    | 4-25-2024 | Staker1 | 33333333333333300 | 299999999999999700 | 33333333333333300     |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker1 | 5e16              | 4.5e17             | 66666666666666600     |
| Operator1 | AVS1 | stETH    | 4-26-2024 | Staker2 | 16666666666666600 | 149999999999999400 | 66666666666666600     |
| Operator2 | AVS1 | stETH    | 4-25-2024 | Staker3 | 66666666666666600 | 599999999999999400 | 66666666666666600     |
| Operator2 | AVS1 | stETH    | 4-26-2024 | Staker3 | 33333333333333300 | 299999999999999700 | 33333333333333300     |

### Dedupe Operators

```sql=
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
```

In the previous step, we aggregated operator rewards with the same `snapshot` and `reward_hash`. Now we remove these rows so the operator is only counted once per `reward_hash` and `snapshot` in the final reward distribution.

| Operator           | AVS           | Strategy       | Snapshot           | Staker           | Operator Tokens   | Staker Tokens      | Operator Tokens (Sum)      |
| ------------------ | ------------- | -------------- | ------------------ | ---------------- | ----------------- | ------------------ | -------------------------- |
| Operator1          | AVS1          | stETH          | 4-25-2024          | Staker1          | 33333333333333300 | 299999999999999700 | 33333333333333300          |
| <s> Operator1 </s> | <s> AVS1 </s> | <s> stETH </s> | <s> 4-26-2024 </s> | <s> Staker1 </s> | <s> 5e16 </s>     | <s> 4.5e17 </s>    | <s> 66666666666666600 </s> |
| Operator1          | AVS1          | stETH          | 4-26-2024          | Staker2          | 16666666666666600 | 149999999999999400 | 66666666666666600          |
| Operator2          | AVS1          | stETH          | 4-25-2024          | Staker3          | 66666666666666600 | 599999999999999400 | 66666666666666600          |
| Operator2          | AVS1          | stETH          | 4-26-2024          | Staker3          | 33333333333333300 | 299999999999999700 | 33333333333333300          |

## 4. Reward for all stakers

This query calculates rewards made by via the `createRewardsForAllSubmission` function on the `RewardCoordinator`. This reward goes directly to stakers.

**_Note: This functionality is currently paused and is unused, hence no hardfork logic is present._**

### Staker Snapshots

```sql=
WITH reward_snapshot_stakers AS (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    sss.staker,
    sss.shares
  FROM {{ ref('1_active_rewards') }} ap
  JOIN {{ ref('staker_share_snapshots') }} as sss
  ON ap.strategy = sss.strategy and ap.snapshot = sss.snapshot
  WHERE ap.reward_type = 'all_stakers'
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  AND sss.shares > 0 and ap.multiplier != 0
),
```

Select all the rewards that set `reward_for_all` to true

### Staker Weights -> Staker Tokens

The calculation is the same as [step 2](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw?view#2-Staker-Reward-Amounts), except we do not need to check the operator a stakers is delegated to

```sql=
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM reward_snapshot_stakers
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
-- Calculate sum of all staker weights
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_staker_weight
  FROM distinct_stakers
),
-- Calculate staker token proportion
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_staker_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to staker
staker_tokens AS (
  SELECT *,
  (tokens_per_day * staker_proportion)::text::decimal(38,0) as staker_tokens
  FROM staker_proportion
)
SELECT * from staker_tokens
```

## 5. Reward For All Earners - Stakers

This reward functionality rewards all operators (and their delegated stakers) who have opted into at least one AVS. The operator gets a fixed 10% commission.

We do this by:

1. Getting all operators who have opted into at least 1 AVS
2. Get the operator's stakers
3. Calculate payout to stakers
4. Calculate payout to operators (step 7)

### AVS Opted Operators

```sql=
WITH avs_opted_operators AS (
  SELECT DISTINCT
    snapshot,
    operator
  FROM {{ ref('operator_avs_registration_snapshots') }}
),
```

Gets the unique operators who have registered for an AVS for a given snapshot. This uses the `operator_avs_registration_snapshots` preclalculation view. Note that deregistrations do not exist in this table.

### Reward Snapshot Operators

```sql=
-- Get the operators who will earn rewards for the reward submission at the given snapshot
reward_snapshot_operators as (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day_decimal,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    ap.reward_submission_date,
    aoo.operator
  FROM {{ ref('1_active_rewards') }} ap
  JOIN avs_opted_operators aoo
  ON ap.snapshot = aoo.snapshot
  WHERE ap.reward_type = 'all_earners'
),
```

We add join the active rewards with operators who have registered to at least one AVS for the snapshot.

### Staker Delegated Operators

```sql=
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
  SELECT
    rso.*,
    sds.staker
  FROM reward_snapshot_operators rso
  JOIN {{ ref('staker_delegation_snapshots') }} sds
  ON
    rso.operator = sds.operator AND
    rso.snapshot = sds.snapshot
),
```

Get the stakers that were delegated to the registered operator for the snapshot.

### Staker Strategy Shares -> Token Breakdowns

The rest of the calculation proceeds as `2_staker_reward_amounts`, without having hard forks.

```sql=
-- Get the shares of each strategy the staker has delegated to the operator
staker_strategy_shares AS (
  SELECT
    sdo.*,
    sss.shares
  FROM staker_delegated_operators sdo
  JOIN {{ ref('staker_share_snapshots') }} sss
  ON
    sdo.staker = sss.staker AND
    sdo.snapshot = sss.snapshot AND
    sdo.strategy = sss.strategy
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE sss.shares > 0 and sdo.multiplier != 0
),
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM staker_strategy_shares
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
-- Calculate sum of all staker weights for each reward and snapshot
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
  FROM distinct_stakers
),
-- Calculate staker proportion of tokens for each reward and snapshot
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to the (staker, operator) pair
staker_operator_total_tokens AS (
  SELECT *,
    FLOOR(staker_proportion * tokens_per_day_decimal) as total_staker_operator_payout
  FROM staker_proportion
),
-- Calculate the token breakdown for each (staker, operator) pair
token_breakdowns AS (
  SELECT *,
    floor(total_staker_operator_payout * 0.10) as operator_tokens,
    total_staker_operator_payout - floor(total_staker_operator_payout * 0.10) as staker_tokens
  FROM staker_operator_total_tokens
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
```

## 6. Reward for All Earners - Operators

Similar to step 3, we now have to parse out the reward for operators.

```sql=
-- Sum up the operator tokens across stakers for each operator, reward hash, and snapshot
WITH operator_token_sums AS (
  SELECT
    reward_hash,
    snapshot,
    token,
    tokens_per_day_decimal,
    avs,
    strategy,
    multiplier,
    reward_type,
    operator,
    SUM(operator_tokens) OVER (PARTITION BY operator, reward_hash, snapshot) AS operator_tokens
  FROM {{ ref('5_rfae_stakers') }}
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
```

## 7. Gold Table Staging

This query combines the rewards from steps 2,3,4,5, and 6 to generate a table with the following columns:

| Earner | Snapshot | Reward Hash | Token | Amount |
| ------ | -------- | ----------- | ----- | ------ |

### Get all rewards

Aggregates the rewards from steps 2,3, and 4. We use `DISTINCT` as a sanity check to discard rows with the same `strategy` and `earner` for a given `reward_hash` and `snapshot`.

```sql=
WITH staker_rewards AS (
  -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{ ref('2_staker_reward_amounts') }}
),
operator_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{ ref('3_operator_reward_amounts') }}
),
rewards_for_all AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{ ref('4_deprecated_rewards_for_all') }}
),
rewards_for_all_earners_stakers AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amounts
  FROM {{ ref('5_rfae_stakers.sql')}}
),
rewards_for_all_earners_operators AS (
  SELECT DISTINCT
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{ ref('6_rfae_operators.sql')}}
)
combined_rewards AS (
  SELECT * FROM operator_rewards
  UNION ALL
  SELECT * FROM staker_rewards
  UNION ALL
  SELECT * FROM rewards_for_all
  UNION ALL
  SELECT * FROM rewards_for_all_earners_stakers
  UNION ALL
  SELECT * FROM rewards_for_all_earners_operators
),
```

### Dedupe Earners

```sql=
-- Dedupe earners, primarily operators who are also their own staker.
deduped_earners AS (
  SELECT
    earner,
    snapshot,
    reward_hash,
    token,
    SUM(amount) as amount
  FROM combined_rewards
  GROUP BY
    earner,
    snapshot,
    reward_hash,
    token
)
SELECT *
FROM deduped_earners
```

Sum up the balances for earners with multiple rows for a given `reward_hash` and `snapshot`. This step handles the case for operators who are also delegated to themselves.

### 8. Gold Table Merge

The previous queries were all views. This step selects rows from the [step 7](https://hackmd.io/Fmjcckn1RoivWpPLRAPwBw#6-Gold-Table-Staging) and appends them to a table.

```sql=
SELECT
    earner,
    snapshot,
    reward_hash,
    token,
    amount
FROM {{ ref('gold_staging') }}
```

This table is merged via the `merge` `incremental_strategy` on the unique keys of `['reward_hash', 'earner', 'snapshot']`. See [dbt docs](https://docs.getdbt.com/docs/build/incremental-strategy) for more information.

Lastly, the Rewards Root Updater will query this table to get cumulative amounts merkelize the following earner, token, cumulative_amount columns.

```sql=
SELECT
    earner,
    snapshot,
    reward_hash,
    token,
    amount
FROM {{ ref('gold_staging') }}
```
