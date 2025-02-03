## blocks

```sql
with distinct_blocks as (
    select
        distinct(block_number)
    from sidecar_preprod_holesky.transaction_logs
    where
        address = '0xfdd5749e11977d60850e06bf5b13221ad95eb6b4'
        and event_name in (
            'OperatorAddedToOperatorSet',
            'OperatorRemovedFromOperatorSet',
            'StrategyAddedToOperatorSet',
            'StrategyRemovedFromOperatorSet'
        )
)
select
    *
from blocks where number in (select * from distinct_blocks)
order by number asc
```


## transactionLogs.sql
```sql
select * from sidecar_preprod_holesky.transaction_logs
where
    address = '0xfdd5749e11977d60850e06bf5b13221ad95eb6b4'
    and event_name in (
        'OperatorAddedToOperatorSet',
        'OperatorRemovedFromOperatorSet',
        'StrategyAddedToOperatorSet',
        'StrategyRemovedFromOperatorSet'
    )
```
