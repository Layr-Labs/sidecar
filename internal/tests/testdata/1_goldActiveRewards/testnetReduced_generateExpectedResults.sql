COPY (
    select * from dbt_testnet_holesky_rewards."active_rewards__2024-07-25 00:00:00_s_2024-07-26 17:27:23"
) TO STDOUT WITH DELIMITER ',' CSV HEADER;
