### Mainnet reduced raw events
```sql
select
    transaction_hash,
    transaction_index,
    block_number,
    address,
    arguments,
    event_name,
    log_index,
    output_data   
from transaction_logs
where
    block_number < 20613003
    and address = '0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a'
    and event_name in ('OperatorSharesIncreased', 'OperatorSharesDecreased')
order by block_number asc, log_index asc

```

Expected results count:
```sql
with operator_shares as (
    SELECT
        operator,
        strategy,
        shares,
        transaction_hash,
        log_index,
        block_time,
        block_date,
        block_number
    FROM (
             SELECT operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number
             FROM dbt_mainnet_ethereum_rewards.operator_share_increases
             where block_number < 20613003

             UNION ALL

             SELECT operator, strategy, shares * -1 AS shares, transaction_hash, log_index, block_time, block_date, block_number
             FROM dbt_mainnet_ethereum_rewards.operator_share_decreases
             where block_number < 20613003
         ) combined_shares
)
select count(*) from operator_shares
```


---
### Mainnet reduced deltas

```sql
SELECT
    operator,
    strategy,
    shares,
    transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
FROM (
         SELECT operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_mainnet_ethereum_rewards.operator_share_increases
         where block_date < '2024-08-20'

         UNION ALL

         SELECT operator, strategy, shares * -1 AS shares, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_mainnet_ethereum_rewards.operator_share_decreases
         where block_date < '2024-08-20'
     ) combined_shares
```

### preprod-rewardsv2

```sql
SELECT
    operator,
    strategy,
    shares,
    transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
FROM (
         SELECT operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_testnet_holesky_rewards.operator_share_increases
         where block_date < '2024-12-10'

         UNION ALL

         SELECT operator, strategy, shares * -1 AS shares, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_testnet_holesky_rewards.operator_share_decreases
         where block_date < '2024-12-10'
     ) combined_shares
```
