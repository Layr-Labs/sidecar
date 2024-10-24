-- Note: block_time is really what the applied cutoff date is based on.
-- e.g. if the snapshot date is 2024-08-20, then the cutoff date is 2024-08-18.

COPY (
     WITH operator_shares as (
    select *
    FROM dbt_mainnet_ethereum_rewards.operator_shares
    where block_time < '2024-08-20'
),
ranked_operator_records as (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY operator, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM operator_shares
),
     snapshotted_records as (
         SELECT
             operator,
             strategy,
             shares,
             block_time,
             date_trunc('day', block_time) + INTERVAL '1' day as snapshot_time
         from ranked_operator_records
         where rn = 1
     ),
     operator_share_windows as (
         SELECT
             operator, strategy, shares, snapshot_time as start_time,
             CASE
                 WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '2024-08-20')
                 ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time)
                 END AS end_time
         FROM snapshotted_records
     ),
     cleaned_records as (
        SELECT * FROM operator_share_windows
            WHERE start_time < end_time
    ),
    final_results as (
    SELECT
        operator,
        strategy,
        shares::text,
        cast(day AS DATE) AS snapshot
    FROM
        cleaned_records
            CROSS JOIN
        generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
    )
    select * from final_results
) TO STDOUT WITH DELIMITER ',' CSV HEADER
