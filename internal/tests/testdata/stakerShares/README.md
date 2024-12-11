

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
    block_number < 19613000
    and (
        -- strategy manager
        (address = '0x858646372cc42e1a627fce94aa7a7033e7cf075a' and event_name = 'Deposit')
        -- m1 withdrawals
        or (address = '0x858646372cc42e1a627fce94aa7a7033e7cf075a' and event_name = 'ShareWithdrawalQueued')
        -- m2 migration & withdrawals
        or (address = '0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a' and event_name = 'WithdrawalQueued')
        or (address = '0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a' and event_name = 'WithdrawalMigrated')
        -- eigenpod shares
        or (address = '0x91e677b07f7af907ec9a428aafa9fc14a0d3a338' and event_name = 'PodSharesUpdated')
    )
order by block_number asc, log_index asc

```

Expected results count:
```sql
with staker_shares as (
SELECT
    staker,
    strategy,
    shares,
    transaction_hash,
    log_index,
    strategy_index,
    block_time,
    block_date,
    block_number
FROM (
    SELECT staker, strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.staker_deposits
    where block_number < 19613000

    UNION ALL

    -- Subtract m1 & m2 withdrawals
    SELECT staker, strategy, shares * -1, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.m1_staker_withdrawals
    where block_number < 19613000

    UNION ALL

    SELECT staker, strategy, shares * -1, strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.m2_staker_withdrawals
    where block_number < 19613000

    UNION all

    -- Shares in eigenpod are positive or negative, so no need to multiply by -1
    SELECT staker, '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.eigenpod_shares
    where block_number < 19613000
) combined_staker_shares
)
select count(*) from staker_shares
```

---

### Mainnet reduced deltas

```sql
SELECT
    staker,
    strategy,
    shares,
    transaction_hash,
    log_index,
    strategy_index,
    block_time,
    block_date,
    block_number
FROM (
    SELECT staker, strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.staker_deposits
    where block_date < '2024-08-20'

    UNION ALL

    -- Subtract m1 & m2 withdrawals
    SELECT staker, strategy, shares * -1, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.m1_staker_withdrawals
    where block_date < '2024-08-20'

    UNION ALL

    SELECT staker, strategy, shares * -1, strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.m2_staker_withdrawals
    where block_date < '2024-08-20'

    UNION all

    -- Shares in eigenpod are positive or negative, so no need to multiply by -1
    SELECT staker, '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.eigenpod_shares
    where block_date < '2024-08-20'
) combined_staker_shares

```

preprod-rewardsV2

```sql
SELECT
    staker,
    strategy,
    shares,
    transaction_hash,
    log_index,
    strategy_index,
    block_time,
    block_date,
    block_number
FROM (
    SELECT staker, strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.staker_deposits
    where block_date < '2024-08-20'

    UNION ALL

    -- Subtract m1 & m2 withdrawals
    SELECT staker, strategy, shares * -1, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.m1_staker_withdrawals
    where block_date < '2024-08-20'

    UNION ALL

    SELECT staker, strategy, shares * -1, strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.m2_staker_withdrawals
    where block_date < '2024-08-20'

    UNION all

    -- Shares in eigenpod are positive or negative, so no need to multiply by -1
    SELECT staker, '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
    FROM dbt_mainnet_ethereum_rewards.eigenpod_shares
    where block_date < '2024-08-20'
) combined_staker_shares
```
