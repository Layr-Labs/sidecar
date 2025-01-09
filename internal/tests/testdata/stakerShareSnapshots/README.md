## Source data

Testnet
```sql
select
    staker,
    strategy,
    shares,
    strategy_index,
    transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
from dbt_testnet_holesky_rewards.staker_shares
```

Testnet reduced
```sql
select
    staker,
    strategy,
    shares,
    strategy_index,
    transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
from dbt_testnet_holesky_rewards.staker_shares
where block_time < '2024-07-25'
```

Mainnet reduced
```sql
select
    staker,
    strategy,
    shares,
    strategy_index,
    transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
from dbt_mainnet_ethereum_rewards.staker_shares
where block_time < '2024-08-20'

```

preprod rewardsV2

```sql
select
    staker,
    strategy,
    shares,
    strategy_index,
    transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
from dbt_preprod_holesky_rewards.staker_shares
where block_time < '2024-12-13'
```

## Expected results

_See `generateExpectedResults.sql`_
