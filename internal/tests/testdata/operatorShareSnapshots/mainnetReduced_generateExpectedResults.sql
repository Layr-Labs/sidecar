COPY (
    select *
    FROM dbt_mainnet_ethereum_rewards.operator_share_snapshots
    where snapshot < '2024-08-12'
) TO STDOUT WITH DELIMITER ',' CSV HEADER
