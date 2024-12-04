## Source query:

Testnet
```sql
with filtered as (
    SELECT
      lower(t.arguments #>> '{0,Value}') as operator,
      lower(t.arguments #>> '{1,Value}') as avs,
      (t.output_data -> 'status')::int as status,
      t.transaction_hash,
      t.log_index,
      b.block_time,
      to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
      t.block_number
    FROM transaction_logs t
    LEFT JOIN blocks b ON t.block_sequence_id = b.id
    WHERE t.address = '0x055733000064333caddbc92763c58bf0192ffebf'
    AND t.event_name = 'OperatorAVSRegistrationStatusUpdated'
    AND date_trunc('day', b.block_time) < TIMESTAMP '2024-09-17'
)
select
    operator,
    avs,
    status as registered,
    log_index,
    block_number
from filtered
```

Testnet Reduced
```sql
with filtered as (
    SELECT
      lower(t.arguments #>> '{0,Value}') as operator,
      lower(t.arguments #>> '{1,Value}') as avs,
      (t.output_data -> 'status')::int as status,
      t.transaction_hash,
      t.log_index,
      b.block_time,
      to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
      t.block_number
    FROM transaction_logs t
    LEFT JOIN blocks b ON t.block_sequence_id = b.id
    WHERE t.address = '0x055733000064333caddbc92763c58bf0192ffebf'
    AND t.event_name = 'OperatorAVSRegistrationStatusUpdated'
    AND date_trunc('day', b.block_time) < TIMESTAMP '2024-07-25'
)
select
    operator,
    avs,
    status as registered,
    log_index,
    block_number
from filtered
```

Mainnet reduced
```sql
with filtered as (
    SELECT
        lower(t.arguments #>> '{0,Value}') as operator,
        lower(t.arguments #>> '{1,Value}') as avs,
        case when (t.output_data ->> 'status')::integer = 1 then true else false end as status,
    t.transaction_hash,
    t.log_index,
    b.block_time::timestamp(6),
    to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
    t.block_number
FROM transaction_logs t
    LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '0x135dda560e946695d6f155dacafc6f1f25c1f5af'
  AND t.event_name = 'OperatorAVSRegistrationStatusUpdated'
  AND date_trunc('day', b.block_time) < TIMESTAMP '2024-08-20'
    )
select
    operator,
    avs,
    status as registered,
    log_index,
    block_number
from filtered
```

preprod rewardsv2

```sql
with filtered as (
    SELECT
        lower(t.arguments #>> '{0,Value}') as operator,
        lower(t.arguments #>> '{1,Value}') as avs,
        case when (t.output_data ->> 'status')::integer = 1 then true else false end as status,
    t.transaction_hash,
    t.log_index,
    b.block_time::timestamp(6),
    to_char(b.block_time, 'YYYY-MM-DD') AS block_date,
    t.block_number
FROM transaction_logs t
    LEFT JOIN blocks b ON t.block_sequence_id = b.id
WHERE t.address = '0x141d6995556135d4997b2ff72eb443be300353bc'
  AND t.event_name = 'OperatorAVSRegistrationStatusUpdated'
  AND date_trunc('day', b.block_time) < TIMESTAMP '2024-12-10'
    )
select
    operator,
    avs,
    status as registered,
    log_index,
    block_number
from filtered
```

## Expected results

_See `generateExpectedResults.sql`_
