
Testnet reduced
```sql
select
    number,
    hash,
    block_time
from blocks
where block_time < '2024-07-25'
```

Mainnet reduced blocks
```sql
select
    number,
    hash,
    block_time
from blocks
where block_time < '2024-08-20'
```
