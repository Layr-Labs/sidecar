COPY (
select * from dbt_mainnet_ethereum_rewards."active_rewards__2024-08-20 00:00:00_s_2024-08-21 14:00:00"
    ) TO STDOUT WITH DELIMITER ',' CSV HEADER;
