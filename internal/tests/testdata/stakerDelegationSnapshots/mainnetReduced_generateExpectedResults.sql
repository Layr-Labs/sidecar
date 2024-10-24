COPY (
     with delegated_stakers as (
    select
    *
    from dbt_mainnet_ethereum_rewards.staker_delegation_status
    where block_time < '2024-08-20'
),
ranked_delegations as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY staker, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM delegated_stakers
),
 snapshotted_records as (
     SELECT
         staker,
         operator,
         block_time,
         date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
     from ranked_delegations
     where rn = 1
 ),
 staker_delegation_windows as (
     SELECT
         staker, operator, snapshot_time as start_time,
         CASE
             -- If the range does not have the end, use the cutoff date truncated to 0 UTC
             WHEN LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '2024-08-20')
             ELSE LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time)
             END AS end_time
     FROM snapshotted_records
 ),
cleaned_records as (
    SELECT * FROM staker_delegation_windows
        WHERE start_time < end_time
),
final_results as (
    SELECT
    staker,
    operator,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
        CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
)
select * from final_results
) TO STDOUT WITH DELIMITER ',' CSV HEADER;
