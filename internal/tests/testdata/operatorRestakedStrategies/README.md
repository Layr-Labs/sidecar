## Source

```sql
select
    block_number,
    operator,
    avs,
    strategy,
    block_time,
    avs_directory_address
from operator_restaked_strategies
where avs_directory_address = '0x055733000064333caddbc92763c58bf0192ffebf'
and block_time < '2024-09-17'
```

## Expected results

_See `generateExpectedResults.sql`_
