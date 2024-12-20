
## mainnet reduced transactions

```sql
select
    transaction_hash,
    transaction_index,
    block_number,
    address,
    arguments,
    event_name,
    log_index,
    output_data
from transaction_logs
where
    address = '0x135dda560e946695d6f155dacafc6f1f25c1f5af'
    and event_name = 'OperatorAVSRegistrationStatusUpdated'
    and block_number < 20613003
```

## preprod rewardsv2


```sql
select
    transaction_hash,
    transaction_index,
    block_number,
    address,
    arguments,
    event_name,
    log_index,
    output_data
from transaction_logs
where
    address = '0x141d6995556135d4997b2ff72eb443be300353bc'
    and event_name = 'OperatorAVSRegistrationStatusUpdated'
    and block_number < 2909490
```
