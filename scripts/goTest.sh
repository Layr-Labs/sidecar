#!/usr/bin/env bash

export PROJECT_ROOT=$(pwd)
export CGO_ENABLED=1
export TESTING=true

go test $@
