#!/usr/bin/env bash

export PROJECT_ROOT=$(pwd)
export CGO_CFLAGS="-I${PROJECT_ROOT}/sqlite-extensions"
export CGO_LDFLAGS="-L${PROJECT_ROOT}/sqlite-extensions/build/lib -lcalculations -Wl,-rpath,${PROJECT_ROOT}/sqlite-extensions/build/lib"
export PYTHONPATH="${PROJECT_ROOT}/sqlite-extensions:$PYTHONPATH"
export CGO_ENABLED=1
export TESTING=true

go run cmd/debugger/main.go
