## preprod rewardsv2

```sql
select
    lower(arguments #>> '{1, Value}') as operator,
    lower(arguments #>> '{2, Value}') as avs,
    to_timestamp((output_data ->> 'activatedAt')::integer)::timestamp(6) as activated_at,
    output_data ->> 'oldOperatorAVSSplitBips' as old_operator_avs_split_bips,
    output_data ->> 'newOperatorAVSSplitBips' as new_operator_avs_split_bips,
    block_number,
    transaction_hash,
    log_index
from transaction_logs
where
    address = '0xb22ef643e1e067c994019a4c19e403253c05c2b0'
    and event_name = 'OperatorAVSSplitBipsSet'
order by block_number desc
```
