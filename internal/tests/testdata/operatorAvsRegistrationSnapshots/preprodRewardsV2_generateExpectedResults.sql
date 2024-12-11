COPY (
     with filtered as (
    select * from dbt_preprod_holesky_rewards.operator_avs_status
    where block_time < '2024-12-11'
),
marked_statuses AS (
    SELECT
        operator,
        avs,
        registered,
        block_time,
        block_date,
        LEAD(block_time) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_time,
        LEAD(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_registration_status,
        LEAD(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_date,
        LAG(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_registered,
        LAG(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_block_date
    FROM filtered
),
     removed_same_day_deregistrations AS (
         SELECT * from marked_statuses
         WHERE NOT (
             (registered = TRUE AND
              COALESCE(next_registration_status = FALSE, false) AND
              COALESCE(block_date = next_block_date, false)) OR
             (registered = FALSE AND
              COALESCE(prev_registered = TRUE, false) and
              COALESCE(block_date = prev_block_date, false)
                 )
             )
     ),
     registration_periods AS (
         SELECT
             operator,
             avs,
             block_time AS start_time,
             COALESCE(next_block_time, TIMESTAMP '2024-12-11') AS end_time,
             registered
         FROM removed_same_day_deregistrations
         WHERE registered = TRUE
     ),
     registration_windows_extra as (
         SELECT
             operator,
             avs,
             date_trunc('day', start_time) + interval '1' day as start_time,
             date_trunc('day', end_time) as end_time
         FROM registration_periods
     ),
     operator_avs_registration_windows as (
         SELECT * from registration_windows_extra
         WHERE start_time != end_time
     ),
     cleaned_records AS (
        SELECT * FROM operator_avs_registration_windows
            WHERE start_time < end_time
    ),
    final_results as (
        SELECT
            operator,
            avs,
            to_char(d, 'YYYY-MM-DD') AS snapshot
        FROM cleaned_records
        CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
        )
        select * from final_results
) TO STDOUT WITH DELIMITER ',' CSV HEADER;
