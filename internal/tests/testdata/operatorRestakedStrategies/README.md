## Source

Testnet
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

Testnet Reduced
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
and block_time < '2024-07-25'
```

Mainnet reduced
```sql
select
    block_number,
    operator,
    avs,
    strategy,
    block_time,
    avs_directory_address
from operator_restaked_strategies
where avs_directory_address = '0x135dda560e946695d6f155dacafc6f1f25c1f5af'
and block_time < '2024-08-20'
```

## Expected results

_See `generateExpectedResults.sql`_
