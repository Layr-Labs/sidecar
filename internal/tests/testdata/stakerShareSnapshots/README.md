## Source data

```sql
select
    staker,
    strategy,
    block_number,
    sum(shares)::TEXT as shares
from dbt_testnet_holesky_rewards.staker_shares
group by 1, 2, 3
```

## Expected results

_See `generateExpectedResults.sql`_
