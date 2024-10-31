.PHONY: deps

CGO_ENABLED = 1
GO=$(shell which go)
ALL_FLAGS=

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
	$(ALL_FLAGS) $(GO) build -o bin/sidecar main.go

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
	gofmt -l .

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

# Run tests in a specific test file
# Usage: make test-file FILE=path/to/your_test_file.go
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
