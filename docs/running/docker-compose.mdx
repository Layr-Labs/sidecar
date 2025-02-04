---
title: Docker Compose
description: A guide in my new Starlight docs site.
---

### docker-compose.yaml

The Sidecar comes with a `docker-compose.yaml` that provides a basic setup for running the Sidecar with a PostgreSQL database in the same docker compose stack.

You should either copy this file or update the existing one with values for the defined environment variables.

```yaml
version: '3.8'

services:
  sidecar:
    # image: public.ecr.aws/z6g0f8n7/sidecar:latest run
    build:
      context: .
      dockerfile: Dockerfile
    command:
      - "run"
    ports:
      - "7100:7100"
      - "7101:7101"
    environment:
      - SIDECAR_DEBUG=false
      - SIDECAR_ETHEREUM_RPC_URL=http://<hostname>:8545
      - SIDECAR_CHAIN=holesky
      - SIDECAR_STATSD_URL=localhost:8125
      - SIDECAR_DATABASE_HOST=postgres
      - SIDECAR_DATABASE_PORT=5432
      - SIDECAR_DATABASE_USER=sidecar
      - SIDECAR_DATABASE_PASSWORD=sidecar
      - SIDECAR_DATABASE_DB_NAME=sidecar
    depends_on:
      - postgres
    restart: unless-stopped

  postgres:
    image: postgres:latest
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_USER=sidecar
      - POSTGRES_PASSWORD=sidecar
      - POSTGRES_DB=sidecar
    volumes:
      - ${POSTGRES_DATA_PATH:-./postgres_data}:/var/lib/postgresql/data
    restart: unless-stopped
```

### Running the stack

```bash
docker compose up -d
```
