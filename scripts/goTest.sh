#!/usr/bin/env bash

export PROJECT_ROOT=$(pwd)
export TESTING=true
export SIDECAR_DATABASE_HOST=${SIDECAR_DATABASE_HOST:-"localhost"}
export SIDECAR_DATABASE_DB_NAME=${SIDECAR_DATABASE_DB_NAME:-""}
export SIDECAR_DATABASE_PORT=${SIDECAR_DATABASE_PORT:-5432}
export SIDECAR_DATABASE_USER=${SIDECAR_DATABASE_USER:-""}
export SIDECAR_DATABASE_PASSWORD=${SIDECAR_DATABASE_PASSWORD:-""}
export SIDECAR_DEBUG=${SIDECAR_DEBUG:-"false"}

go test $@
