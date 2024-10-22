## Source

Testnet
```sql
SELECT
    staker,
    operator,
    log_index,
    block_number,
    case when src = 'undelegations' THEN false ELSE true END AS delegated
FROM (
         SELECT *, 'undelegations' AS src FROM dbt_testnet_holesky_rewards.staker_undelegations
         UNION ALL
         SELECT *, 'delegations' AS src FROM dbt_testnet_holesky_rewards.staker_delegations
     ) as delegations_combined
where block_time < '2024-09-17'
```

Testnet reduced
```sql
SELECT
    staker,
    operator,
    log_index,
    block_number,
    case when src = 'undelegations' THEN false ELSE true END AS delegated
FROM (
         SELECT *, 'undelegations' AS src FROM dbt_testnet_holesky_rewards.staker_undelegations
         UNION ALL
         SELECT *, 'delegations' AS src FROM dbt_testnet_holesky_rewards.staker_delegations
     ) as delegations_combined
where block_time < '2024-07-25'
```

Mainnet reduced
```sql
SELECT
    staker,
    operator,
    log_index,
    block_number,
    case when src = 'undelegations' THEN false ELSE true END AS delegated
FROM (
         SELECT *, 'undelegations' AS src FROM dbt_mainnet_ethereum_rewards.staker_undelegations
         UNION ALL
         SELECT *, 'delegations' AS src FROM dbt_mainnet_ethereum_rewards.staker_delegations
     ) as delegations_combined
where block_time < '2024-08-20'
```


```bash
psql --host localhost --port 5435 --user blocklake --dbname blocklake --password < internal/tests/testdata/stakerDelegationSnapshots/generateExpectedResults.sql > internal/tests/testdata/stakerDelegationSnapshots/expectedResults.csv
```
