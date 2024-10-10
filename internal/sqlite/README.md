_Note: Amazon only existed ever on mainnet_
## Amazon staker tokens expected values query

```sql
select
    staker_proportion,
    tokens_per_day,
    total_staker_operator_payout
from dbt_mainnet_ethereum_rewards."staker_rewards__2024-08-02 00:00:00_s_2024-08-03 03:51:30"
limit 100
```

### Amazon operator tokens query
```sql
select
    total_staker_operator_payout,
    operator_tokens
from dbt_mainnet_ethereum_rewards."staker_rewards__2024-08-02 00:00:00_s_2024-08-03 03:51:30"
limit 100
```

---

## Nile staker tokens expected values query

```sql
select
    staker_proportion,
    tokens_per_day,
    total_staker_operator_payout
from dbt_testnet_holesky_rewards."staker_rewards__2024-07-27 00:00:00_s_2024-07-28 13:00:00"
limit 100
```

## Nile operator tokens query
```sql
select
    total_staker_operator_payout,
    operator_tokens
from dbt_testnet_holesky_rewards."staker_rewards__2024-07-27 00:00:00_s_2024-07-28 13:00:00"
limit 100
```

## Nile staker final totals query

```sql
select
    total_staker_operator_payout,
    operator_tokens,
    staker_tokens
from dbt_testnet_holesky_rewards."staker_rewards__2024-07-27 00:00:00_s_2024-07-28 13:00:00"
limit 100
```

---

## Post-nile staker tokens query

```sql
select
    staker_proportion,
    tokens_per_day_decimal,
    total_staker_operator_payout
from dbt_mainnet_ethereum_rewards."staker_rewards__2024-09-01 00:00:00_s_2024-09-02 16:00:02"
limit 100
```

## Post-nile operator tokens query

```sql
select
    total_staker_operator_payout,
    operator_tokens
from dbt_mainnet_ethereum_rewards."staker_rewards__2024-09-01 00:00:00_s_2024-09-02 16:00:02"
limit 100
```

## Post-nile staker final totals query

```sql
select
    total_staker_operator_payout,
    operator_tokens,
    staker_tokens
from dbt_mainnet_ethereum_rewards."staker_rewards__2024-09-01 00:00:00_s_2024-09-02 16:00:02"
limit 100
```
