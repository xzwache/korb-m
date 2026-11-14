VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GO_IMAGE ?= golang:1.25

.PHONY: all build test race cover lint fmt tidy test-linux clean help

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

clean: ## Remove build artifacts
	rm -rf bin/ coverage.out

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'
