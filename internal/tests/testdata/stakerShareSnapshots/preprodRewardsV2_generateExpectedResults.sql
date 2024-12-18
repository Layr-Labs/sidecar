COPY (
select
    *
from dbt_preprod_holesky_rewards.staker_share_snapshots
where snapshot < '2024-12-11'
    ) TO STDOUT WITH DELIMITER ',' CSV HEADER
