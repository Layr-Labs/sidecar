
## preprod rewardsV2
```sql
select
    lower(arguments #>> '{1, Value}') as operator,
    to_timestamp((output_data ->> 'activatedAt')::integer)::timestamp(6) as activated_at,
    output_data ->> 'oldOperatorPISplitBips' as old_operator_pi_split_bips,
    output_data ->> 'newOperatorPISplitBips' as new_operator_pi_split_bips,
    block_number,
    transaction_hash,
    log_index
from transaction_logs
where
    address = '0xb22ef643e1e067c994019a4c19e403253c05c2b0'
    and event_name = 'OperatorPISplitBipsSet'
order by block_number desc
```
