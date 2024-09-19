## Source data

```sql
select
    operator,
    strategy,
    block_number,
    sum(shares)::text as shares
from dbt_testnet_holesky_rewards.operator_shares
where block_time < '2024-09-17'
group by 1, 2, 3
```


