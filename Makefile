.PHONY: deps

GO=$(shell which go)
ALL_FLAGS=
GO_FLAGS=-ldflags "-X 'github.com/Layr-Labs/sidecar/internal/version.Version=$(shell cat VERSION)' -X 'github.com/Layr-Labs/sidecar/internal/version.Commit=$(shell git rev-parse --short HEAD)'"

deps/dev:
	${GO} install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0
	${GO} install honnef.co/go/tools/cmd/staticcheck@latest
	${GO} install github.com/google/yamlfmt/cmd/yamlfmt@latest

deps/go:
	${GO} mod tidy


deps-system:
	./scripts/installDeps.sh

deps: deps-system deps/go deps/dev

.PHONY: clean
clean:
	rm -rf bin || true

.PHONY: build/cmd/sidecar
build/cmd/sidecar:
	$(ALL_FLAGS) $(GO) build $(GO_FLAGS) -o bin/sidecar main.go

build/cmd/sidecar/darwin-arm64:
	GOOS=darwin GOARCH=arm64 $(ALL_FLAGS) $(GO) build $(GO_FLAGS) -o release/darwin-arm64/sidecar main.go

build/cmd/sidecar/darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(ALL_FLAGS) $(GO) build $(GO_FLAGS) -o release/darwin-amd64/sidecar main.go

build/cmd/sidecar/linux-arm64:
	GOOS=linux GOARCH=arm64 $(ALL_FLAGS) $(GO) build $(GO_FLAGS) -o release/linux-arm64/sidecar main.go

build/cmd/sidecar/linux-amd64:
	GOOS=linux GOARCH=amd64 $(ALL_FLAGS) $(GO) build $(GO_FLAGS) -o release/linux-amd64/sidecar main.go

.PHONY: release
release:
	$(MAKE) build/cmd/sidecar/darwin-arm64
	$(MAKE) build/cmd/sidecar/darwin-amd64
	$(MAKE) build/cmd/sidecar/linux-arm64
	$(MAKE) build/cmd/sidecar/linux-amd64

.PHONY: release-with-docker
release-with-docker:
	docker run --rm -v `pwd`:/build golang:1.23.6 /bin/bash -c "git config --global --add safe.directory /build && cd /build && make release"

.PHONY: build
build: build/cmd/sidecar

# Docker build steps
docker-buildx-self:
	docker buildx build -t go-sidecar:latest -t go-sidecar:latest .

docker-buildx:
	docker-buildx build --platform linux/amd64 --push -t 767397703211.dkr.ecr.us-east-1.amazonaws.com/go-sidecar:$(shell date +%s) -t 767397703211.dkr.ecr.us-east-1.amazonaws.com/go-sidecar:latest .

.PHONY: yamlfmt
yamlfmt:
	yamlfmt -lint .github/workflows/*.yml .github/*.yml

.PHONY: fmt
fmt:
	gofmt -w .

.PHONY: fmtcheck
fmtcheck:
	@unformatted_files=$$(gofmt -l .); \
	if [ -n "$$unformatted_files" ]; then \
		echo "The following files are not properly formatted:"; \
		echo "$$unformatted_files"; \
		echo "Please run 'gofmt -w .' to format them."; \
		exit 1; \
	fi
.PHONY: vet
vet:
	$(ALL_FLAGS) $(GO) vet ./...

.PHONY: lint
lint:
	$(ALL_FLAGS) golangci-lint run

.PHONY: test
test:
	./scripts/goTest.sh -v -p 1 -parallel 1 ./...

.PHONY: test-file
test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE variable is not set."; \
		echo "Usage: make test-file FILE=path/to/your_test_file.go"; \
		exit 1; \
	fi
	./scripts/goTest.sh -v -p 1 -parallel 1 $(FILE)

.PHONY: staticcheck
staticcheck:
	staticcheck ./...

.PHONY: ci-test
ci-test: build test

test-rewards:
	TEST_REWARDS=true TESTING=true ${GO} test ./pkg/rewards -v -p 1

.PHONY: docs/deps
docs/deps:
	cd docs && yarn install

.PHONY: docs/update-apis
docs/update-apis:
	curl -s -L https://raw.githubusercontent.com/Layr-Labs/protocol-apis/refs/heads/master/gen/openapi/api.public.swagger.json > docs/openapi/api.public.json
	curl -s -L https://raw.githubusercontent.com/Layr-Labs/protocol-apis/refs/heads/master/gen/openapi/api.swagger.json > docs/openapi/api.json
	cd docs && yarn docusaurus clean-api-docs all && yarn docusaurus gen-api-docs all

docs/dev: docs/deps docs/update-apis
	cd docs && yarn start

.PHONY: docs
docs/build: docs/deps docs/update-apis
	cd docs && SKIP_HTML_MINIFICATION=true yarn build
