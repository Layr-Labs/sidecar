COPY (
SELECT
    staker,
    strategy,
    shares,
    transaction_hash,
    log_index,
    SUM(shares) OVER (PARTITION BY staker, strategy ORDER BY block_time, log_index) AS shares,
        block_time,
    block_date,
    block_number
FROM (
         SELECT staker, strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_preprod_holesky_rewards.staker_deposits
         where block_date < '2024-12-11'

         UNION ALL

         -- Subtract m1 & m2 withdrawals
         SELECT staker, strategy, shares * -1, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_preprod_holesky_rewards.m1_staker_withdrawals
         where block_date < '2024-12-11'

         UNION ALL

         SELECT staker, strategy, shares * -1, strategy_index, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_preprod_holesky_rewards.m2_staker_withdrawals
         where block_date < '2024-12-11'

         UNION all

         -- Shares in eigenpod are positive or negative, so no need to multiply by -1
         SELECT staker, '0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0' as strategy, shares, 0 as strategy_index, transaction_hash, log_index, block_time, block_date, block_number
         FROM dbt_preprod_holesky_rewards.eigenpod_shares
         where block_date < '2024-12-11'
     ) combined_staker_shares
    ) TO STDOUT WITH DELIMITER ',' CSV HEADER
