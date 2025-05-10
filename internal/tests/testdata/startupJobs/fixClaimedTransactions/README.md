```sql
select
    *
from transaction_logs
where
    address = '0xacc1fb458a1317e886db376fc8141540537e68fe'
    and event_name = 'RewardsClaimed'
order by block_number asc, transaction_index asc, log_index asc

select
    *
from transactions
where transaction_hash in (
    select
        distinct(transaction_hash)
    from transaction_logs
    where
        address = '0xacc1fb458a1317e886db376fc8141540537e68fe'
        and event_name = 'RewardsClaimed'
)
order by block_number, transaction_index


select
    *
from blocks
where number in (
    select
        distinct(block_number)
    from transaction_logs
    where
        address = '0xacc1fb458a1317e886db376fc8141540537e68fe'
        and event_name = 'RewardsClaimed'
)
order by number
```
