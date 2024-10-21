copy (with ranked_records AS (
    SELECT
        lower(operator) as operator,
        lower(avs) as avs,
        lower(strategy) as strategy,
        block_time,
        date_trunc('day', CAST(block_time as timestamp(6))) + interval '1' day as start_time,
        ROW_NUMBER() OVER (
            PARTITION BY operator, avs, strategy, date_trunc('day', CAST(block_time as timestamp(6))) + interval '1' day
            ORDER BY block_time DESC
            ) AS rn
    FROM public.operator_restaked_strategies
    WHERE avs_directory_address = lower('0x135dda560e946695d6f155dacafc6f1f25c1f5af')
    and block_time < '2024-08-20'
),
     latest_records AS (
         SELECT
             operator,
             avs,
             strategy,
             start_time,
             block_time
         FROM ranked_records
         WHERE rn = 1
     ),
     grouped_records AS (
         SELECT
             operator,
             avs,
             strategy,
             start_time,
             LEAD(start_time) OVER (
                 PARTITION BY operator, avs, strategy
                 ORDER BY start_time ASC
                 ) AS next_start_time
         FROM latest_records
     ),
     parsed_ranges AS (
         SELECT
             operator,
             avs,
             strategy,
             start_time,
             CASE
                 WHEN next_start_time IS NULL OR next_start_time > start_time + INTERVAL '1' DAY THEN start_time
                 ELSE next_start_time
                 END AS end_time
         FROM grouped_records
     ),
     active_windows as (
         SELECT *
         FROM parsed_ranges
         WHERE start_time != end_time
     ),
     gaps_and_islands AS (
         SELECT
             operator,
             avs,
             strategy,
             start_time,
             end_time,
             LAG(end_time) OVER(PARTITION BY operator, avs, strategy ORDER BY start_time) as prev_end_time
         FROM active_windows
     ),
     island_detection AS (
         SELECT operator, avs, strategy, start_time, end_time, prev_end_time,
                CASE
                    WHEN prev_end_time = start_time THEN 0
                    ELSE 1
                    END as new_island
         FROM gaps_and_islands
     ),
     island_groups AS (
         SELECT
             operator,
             avs,
             strategy,
             start_time,
             end_time,
             SUM(new_island) OVER (
                 PARTITION BY operator, avs, strategy ORDER BY start_time
                 ) AS island_id
         FROM island_detection
     ),
     operator_avs_strategy_windows AS (
         SELECT
             operator,
             avs,
             strategy,
             MIN(start_time) AS start_time,
             MAX(end_time) AS end_time
         FROM island_groups
         GROUP BY operator, avs, strategy, island_id
         ORDER BY operator, avs, strategy, start_time
     ),
     cleaned_records AS (
        SELECT * FROM operator_avs_strategy_windows
            WHERE start_time < end_time
    ),
    final_results as (
SELECT
    operator,
    avs,
    strategy,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
        CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
)
select * from final_results
) to STDOUT DELIMITER ',' CSV HEADER;
