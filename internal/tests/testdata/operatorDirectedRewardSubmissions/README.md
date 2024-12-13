## preprod rewards-v2

```sql
WITH strategies AS (
    SELECT
        tl.*,
        lower(arguments #>> '{2, Value}') as reward_hash,
        output_data->'operatorDirectedRewardsSubmission'->>'token' as token,
    output_data->'operatorDirectedRewardsSubmission'->>'duration' as duration,
    output_data->'operatorDirectedRewardsSubmission'->>'startTimestamp' as start_timestamp,
    strategy_data,
    strategy_idx - 1 as strategy_idx  -- Subtract 1 for 0-based indexing
FROM transaction_logs as tl,
    jsonb_array_elements(output_data->'operatorDirectedRewardsSubmission'->'strategiesAndMultipliers')
WITH ORDINALITY AS t(strategy_data, strategy_idx)
where
    address = '0xb22ef643e1e067c994019a4c19e403253c05c2b0'
  and event_name = 'OperatorDirectedAVSRewardsSubmissionCreated'
    ),
    operators AS (
SELECT
    lower(arguments #>> '{2, Value}') as reward_hash,
    operator_data,
    operator_data->>'operator' as operator,
    output_data->'operatorDirectedRewardsSubmission' as rewards_submission,
    operator_idx - 1 as operator_idx  -- Subtract 1 to make it 0-based indexing
FROM transaction_logs,
    jsonb_array_elements(output_data->'operatorDirectedRewardsSubmission'->'operatorRewards')
WITH ORDINALITY AS t(operator_data, operator_idx)
where
    address = '0xb22ef643e1e067c994019a4c19e403253c05c2b0'
  and event_name = 'OperatorDirectedAVSRewardsSubmissionCreated'
    ),
    joined_data as (
SELECT
    lower(arguments #>> '{1, Value}') as avs,
    lower(arguments #>> '{2, Value}') as reward_hash,
    strategies.token,
    operator_data->>'operator' as operator,
    operator_idx as operator_index,
    operator_data->>'amount' as amount,
    strategy_data->>'strategy' as strategy,
    strategy_idx as strategy_index,
    strategy_data->>'multiplier' as multiplier,
    (to_timestamp((rewards_submission->>'startTimestamp')::int))::timestamp(6) as start_timestamp,
    (rewards_submission->>'duration')::int as duration,
    to_timestamp((rewards_submission->>'startTimestamp')::int + (rewards_submission->>'duration')::int)::timestamp(6) as end_timestamp,
    block_number,
    transaction_hash,
    log_index
FROM strategies
    inner join operators on(
    strategies.reward_hash = operators.reward_hash
    )
    )
select * from joined_data
```
