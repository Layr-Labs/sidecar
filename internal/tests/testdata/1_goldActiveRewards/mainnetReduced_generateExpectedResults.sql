COPY (
select
    earner,
    snapshot,
    reward_hash,
    token,
    amount::text as amount
from dbt_mainnet_ethereum_rewards."gold_staging__2024-08-02 00:00:00_s_2024-08-03 03:51:30"
) TO STDOUT WITH DELIMITER ',' CSV HEADER;
