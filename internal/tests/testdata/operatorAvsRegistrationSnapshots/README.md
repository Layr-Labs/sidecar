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

## Expected results query:

```sql
with operator_avs_status as (
    select
    *
    from dbt_testnet_holesky_rewards.operator_avs_status
    where block_time < TIMESTAMP '2024-09-17'
),
marked_statuses AS (
    SELECT
        operator,
        avs,
        registered,
        block_time,
        block_date,
        -- Mark the next action as next_block_time
        LEAD(block_time) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_time,
        -- The below lead/lag combinations are only used in the next CTE
        -- Get the next row's registered status and block_date
        LEAD(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_registration_status,
        LEAD(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_date,
        -- Get the previous row's registered status and block_date
        LAG(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_registered,
        LAG(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_block_date
    FROM operator_avs_status
),
-- Ignore a (registration,deregistration) pairs that happen on the exact same date
 removed_same_day_deregistrations AS (
     SELECT * from marked_statuses
     WHERE NOT (
         -- Remove the registration part
         (registered = TRUE AND
          COALESCE(next_registration_status = FALSE, false) AND -- default to false if null
          COALESCE(block_date = next_block_date, false)) OR
             -- Remove the deregistration part
         (registered = FALSE AND
          COALESCE(prev_registered = TRUE, false) and
          COALESCE(block_date = prev_block_date, false)
             )
         )
 ),
-- Combine corresponding registrations into a single record
-- start_time is the beginning of the record
 registration_periods AS (
     SELECT
         operator,
         avs,
         block_time AS start_time,
         -- Mark the next_block_time as the end_time for the range
         -- Use coalesce because if the next_block_time for a registration is not closed, then we use cutoff_date
         COALESCE(next_block_time, TIMESTAMP '2024-09-01') AS end_time,
         registered
     FROM removed_same_day_deregistrations
     WHERE registered = TRUE
 ),
-- Round UP each start_time and round DOWN each end_time
 registration_windows_extra as (
     SELECT
         operator,
         avs,
         date_trunc('day', start_time) + interval '1' day as start_time,
         -- End time is end time non inclusive becuase the operator is not registered on the AVS at the end time OR it is current timestamp rounded up
         date_trunc('day', end_time) as end_time
     FROM registration_periods
 ),
-- Ignore start_time and end_time that last less than a day
 operator_avs_registration_windows as (
     SELECT * from registration_windows_extra
     WHERE start_time != end_time
 ),
cleaned_records AS (
    SELECT * FROM operator_avs_registration_windows
        WHERE start_time < end_time
)
SELECT
    operator,
    avs,
    day AS snapshot
FROM cleaned_records
CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
```
