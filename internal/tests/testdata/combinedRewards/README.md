## Source query

Testnet
```sql
with filtered as (
    select * from dbt_testnet_holesky_rewards.rewards_combined
    where block_time < '2024-09-17'
),
expanded as (
    select
        f.avs,
        f.reward_hash,
        f.token,
        f.amount::text as amount,
        f.strategy,
        f.strategy_index,
        f.multiplier::text as multiplier,
        f.start_timestamp,
        f.end_timestamp,
        f.reward_type,
        f.duration,
        f.block_number as block_number
    from filtered as f
)
select * from expanded

```

Testnet reduced
```sql 
with filtered as (
    select * from dbt_testnet_holesky_rewards.rewards_combined
    where block_time < '2024-07-25'
),
expanded as (
    select
        f.avs,
        f.reward_hash,
        f.token,
        f.amount::text as amount,
        f.strategy,
        f.strategy_index,
        f.multiplier::text as multiplier,
        f.start_timestamp,
        f.end_timestamp,
        f.reward_type,
        f.duration,
        f.block_number as block_number
    from filtered as f
)
select * from expanded
```

Mainnet, reduced
```sql
with filtered as (
    select * from dbt_mainnet_ethereum_rewards.rewards_combined
    where block_time < '2024-08-12'
),
expanded as (
    select
        f.avs,
        f.reward_hash,
        f.token,
        f.amount::text as amount,
        f.strategy,
        f.strategy_index,
        f.multiplier::text as multiplier,
        f.start_timestamp,
        f.end_timestamp,
        f.reward_type,
        f.duration,
        f.block_number as block_number
    from filtered as f
)
select * from expanded
```
