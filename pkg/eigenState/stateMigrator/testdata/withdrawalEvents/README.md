## transaction logs

```sql
select
    *
from transaction_logs
where
    address = '0xa44151489861fe9e3055d95adc98fbd462b948e7'
    and event_name in (
       "SlashingWithdrawalCompleted",
       "SlashingWithdrawalQueued",
    )
order by block_number asc, log_index asc;
```
