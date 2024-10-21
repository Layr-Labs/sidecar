## Source data

Testnet
```sql
select
    staker,
    strategy,
    block_number,
    sum(shares)::TEXT as shares
from dbt_testnet_holesky_rewards.staker_shares
group by 1, 2, 3
```

Testnet reduced
```sql
select
    staker,
    strategy,
    block_number,
    sum(shares)::TEXT as shares
from dbt_testnet_holesky_rewards.staker_shares
where block_time < '2024-07-25'
group by 1, 2, 3
```

Mainnet reduced
```sql
select
    staker,
    strategy,
    block_number,
    sum(shares)::TEXT as shares
from dbt_mainnet_ethereum_rewards.staker_shares
where block_time < '2024-08-20'
group by 1, 2, 3

```

## Expected results

_See `generateExpectedResults.sql`_
