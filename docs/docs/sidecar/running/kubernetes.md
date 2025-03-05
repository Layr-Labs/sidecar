---
title: Kubernetes
description: A guide in my new Starlight docs site.
sidebar_position: 4
---

## Helm Chart

The Sidecar comes with a Helm chart that provides a basic setup for running the Sidecar with a PostgreSQL database in a Kubernetes cluster.

## Installing the chart

```bash
helm repo add sidecar https://eigenlayer-sidecar.s3.amazonaws.com/helm  
helm repo update
```

## Configure the chart

```yaml
image:
  tag: "v2.0.0"
sidecar:
  nameOverride: sidecar
  env:
    SIDECAR_CHAIN: "mainnet"
    SIDECAR_DEBUG: "false"
    SIDECAR_ETHEREUM_RPC_URL: "http://<your rpc node>"
    SIDECAR_DATABASE_HOST: "<database host>"
    SIDECAR_DATABASE_PORT: "5432"
    SIDECAR_DATABASE_USER: "sidecar"
    SIDECAR_DATABASE_DB_NAME: "sidecar"
  metadataLabels:
    serviceName: sidecar
```

## Deploy

```bash
CHART_VERSION="1.0.0-rc.9"

helm upgrade \
    --install \
    --atomic \
    --cleanup-on-fail \
    --timeout 2m \
    --force \
    --wait  \
    --version=$CHART_VERSION \
    --set "sidecar.secret.data.SIDECAR_DATABASE_PASSWORD=${YOUR_DATABASE_PASSWORD}" \
    -f ./path/to/your/values.yaml \
    sidecar sidecar/sidecar
```
