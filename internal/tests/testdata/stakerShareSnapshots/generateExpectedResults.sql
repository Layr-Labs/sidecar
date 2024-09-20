COPY (
     with staker_shares as (
    select
        *
    from dbt_testnet_holesky_rewards.staker_shares
    where block_time < '2024-09-17'
),
ranked_staker_records as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY staker, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM staker_shares
),
     snapshotted_records as (
         SELECT
             staker,
             strategy,
             shares,
             block_time,
             date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
         from ranked_staker_records
         where rn = 1
     ),
     staker_share_windows as (
         SELECT
             staker, strategy, shares, snapshot_time as start_time,
             CASE
                 WHEN LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '2024-09-01')
                 ELSE LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time)
                 END AS end_time
         FROM snapshotted_records
     ),
     cleaned_records as (
  SELECT * FROM staker_share_windows
  WHERE start_time < end_time
),
final_results as (
    SELECT
        staker,
        strategy,
        shares::text,
        cast(day AS DATE) AS snapshot
    FROM
        cleaned_records
    CROSS JOIN
        generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
        )
SELECT * from final_results
) TO STDOUT WITH DELIMITER ',' CSV HEADER
