## transaction logs

```sql
select
    *
from sidecar_preprod_holesky.transaction_logs
where
    event_name in (
        'OperatorSetCreated',
        'AllocationUpdated',
        'OperatorSlashed',
        'EncumberedMagnitudeUpdated',
        'MaxMagnitudeUpdated',
        'AllocationDelaySet',
        'OperatorSharesSlashed'
    )
order by block_number asc, log_index asc;
```
