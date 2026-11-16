VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GO_IMAGE ?= golang:1.25

.PHONY: all build test race cover lint fmt tidy test-linux bench-linux clean help

all: lint test build ## Lint, test and build

build: ## Build binaries into ./bin
	go build -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

test: ## Run unit tests
	go test ./...

race: ## Run tests with the race detector
	go test -race ./...

cover: ## Run tests with coverage report
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

lint: ## Run golangci-lint (install: https://golangci-lint.run/welcome/install/)
	golangci-lint run ./...

fmt: ## Format code
	gofmt -s -w .

tidy: ## Tidy go.mod / go.sum
	go mod tidy

# macOS uses kqueue; Linux uses epoll. This runs the test suite inside a Linux
# container so the epoll code paths get exercised from a Mac.
test-linux: ## Run race tests inside a Linux Docker container (epoll paths)
	docker run --rm -v "$(CURDIR)":/src -w /src \
		-v korbm-gomod:/go/pkg/mod -v korbm-gocache:/root/.cache/go-build \
		$(GO_IMAGE) go test -race ./...

# Memory per idle connection at 10K/50K/100K connections, on Linux.
# Raise Docker Desktop's memory limit (Settings > Resources) to 8 GB or more.
STEPS ?= 10000 50000 100000
bench-linux: ## Measure memory per connection inside a Linux container
	docker run --rm -v "$(CURDIR)":/src -w /src \
		-v korbm-gomod:/go/pkg/mod -v korbm-gocache:/root/.cache/go-build \
		--ulimit nofile=1048576:1048576 \
		--sysctl net.ipv4.ip_local_port_range="1024 65535" \
		--sysctl net.core.somaxconn=65535 \
		-e STEPS="$(STEPS)" \
		$(GO_IMAGE) ./scripts/bench.sh

clean: ## Remove build artifacts
	rm -rf bin/ coverage.out

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'
