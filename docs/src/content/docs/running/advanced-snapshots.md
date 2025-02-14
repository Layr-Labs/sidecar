---
title: Advanced Snapshots
description: Advanced Snapshots documentation
---


## Advanced: Converting the Schema of a Dump

If you're using a custom schema and want to use a public snapshot, you likely want to convert the dump.

This section provides a step-by-step runbook for converting a snapshot dump to use a different schema name.

```bash
# Open your terminal and create a temporary database to work with:
psql -c "CREATE DATABASE temp_sidecar_dump_schema_conversion_db;"

# Use the Sidecar CLI to restore the snapshot dump into the temporary database:
./bin/sidecar restore-snapshot \
    --database.host=localhost \
    --database.user=... \
    --database.password=... \
    --database.port=5432 \
    --database.db_name=temp_sidecar_dump_schema_conversion_db \
    --database.schema_name=<input schema name> \
    --snapshot.input=snapshot.dump
    --snapshot.verify-input=false

# Connect to the temporary database and execute the SQL command to rename the schema:
psql -d temp_sidecar_dump_schema_conversion_db -c "ALTER SCHEMA <input schema name> RENAME TO <output schema name>;"

# Use the Sidecar CLI to create a new snapshot with the updated schema:
./bin/sidecar create-snapshot \
    --database.host=localhost \
    --database.user=... \
    --database.password=... \
    --database.port=5432 \
    --database.db_name=temp_sidecar_dump_schema_conversion_db \
    --database.schema_name=<output schema name> \
    --snapshot.output-file=new_snapshot.dump

# Drop the temporary database to free up resources:
psql -c "DROP DATABASE IF EXISTS temp_sidecar_dump_schema_conversion_db;"
```