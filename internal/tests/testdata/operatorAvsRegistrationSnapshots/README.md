## Source query:

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

## Expected results

_See `generateExpectedResults.sql`_