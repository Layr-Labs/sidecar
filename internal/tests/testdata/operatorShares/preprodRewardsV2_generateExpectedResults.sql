COPY (
SELECT
    operator,
    strategy,
    SUM(shares) OVER (PARTITION BY operator, strategy ORDER BY block_time, log_index) AS shares,
        transaction_hash,
    log_index,
    block_time,
    block_date,
    block_number
FROM (
         SELECT operator, strategy, shares, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_preprod_holesky_rewards.operator_share_increases
         where block_date < '2024-12-11'

         UNION ALL

         SELECT operator, strategy, shares * -1 AS shares, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_preprod_holesky_rewards.operator_share_decreases
         where block_date < '2024-12-11'
     ) combined_shares
    ) TO STDOUT WITH DELIMITER ',' CSV HEADER
