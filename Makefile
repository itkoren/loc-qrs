.PHONY: build test test-integration test-all test-short clean run tidy lint \
        draft-release tag release-check help

CGO_ENABLED ?= 1
BINARY      := bin/server
VERSION     ?= $(shell git describe --tags --exact-match 2>/dev/null || echo "dev")
LDFLAGS     := -ldflags="-s -w -X main.version=$(VERSION)"

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BINARY) ./cmd/server

run: build
	./$(BINARY)

tidy:
	go mod tidy

clean:
	rm -rf bin/ dist/ data/*.jsonl data/*.csv data/*.parquet \
	       data/*.tmp.parquet data/.schema_version data/.sync.duckdb

# ── Tests ─────────────────────────────────────────────────────────────────────

## Run unit tests (no CGO-heavy integration tests)
test:
	CGO_ENABLED=$(CGO_ENABLED) go test ./... -race -count=1 -timeout=120s

## Run only integration tests (requires DuckDB, CGO_ENABLED=1)
test-integration:
	CGO_ENABLED=$(CGO_ENABLED) go test ./... -race -count=1 -timeout=180s -tags=integration

## Run unit + integration tests
test-all: test test-integration

## Run tests with -short flag (skip slow tests)
test-short:
	CGO_ENABLED=$(CGO_ENABLED) go test ./... -race -short -count=1

## Run tests with coverage report
test-coverage:
	CGO_ENABLED=$(CGO_ENABLED) go test ./... -race -count=1 -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# ── Lint & Style ──────────────────────────────────────────────────────────────

## Run golangci-lint (install: https://golangci-lint.run/welcome/install/)
lint:
	golangci-lint run --timeout=5m ./...

## Run lint with fix where possible
lint-fix:
	golangci-lint run --fix --timeout=5m ./...

## Run gofmt style check (exit non-zero if any files need formatting)
fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
	  echo "Files need formatting:"; \
	  echo "$$unformatted"; \
	  exit 1; \
	fi
	@echo "All files are properly formatted."

## Format all Go files
fmt:
	gofmt -w .

# ── Release ───────────────────────────────────────────────────────────────────

## Generate a draft RELEASE_NOTES.md from git log since last tag.
## Usage: make draft-release
##        make draft-release FROM=v0.1.0
##        make draft-release FROM=v0.1.0 TO=v0.2.0
draft-release:
	@./scripts/gen-release-notes.sh $(FROM) $(TO)

## Tag a new release version.
## Usage: make tag VERSION=v1.2.3
## Prerequisites: edit and commit RELEASE_NOTES.md first.
tag:
	@if [ -z "$(VERSION)" ] || [ "$(VERSION)" = "dev" ]; then \
	  echo "Error: VERSION must be set (e.g. make tag VERSION=v1.2.3)"; \
	  exit 1; \
	fi
	@echo "Tagging $(VERSION)..."
	git tag -a "$(VERSION)" -m "Release $(VERSION)"
	@echo "Push with: git push origin $(VERSION)"

## Validate the release is ready (lint + all tests + build).
release-check:
	$(MAKE) lint
	$(MAKE) test-all
	$(MAKE) build
	@echo "✅ Release check passed."

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo "loc-qrs Makefile targets:"
	@echo ""
	@echo "  build              Build the server binary"
	@echo "  run                Build and run the server"
	@echo "  test               Run unit tests"
	@echo "  test-integration   Run integration tests (CGO_ENABLED=1)"
	@echo "  test-all           Run unit + integration tests"
	@echo "  test-coverage      Run tests and open HTML coverage report"
	@echo "  lint               Run golangci-lint"
	@echo "  lint-fix           Run golangci-lint with auto-fix"
	@echo "  fmt                Format all Go files"
	@echo "  fmt-check          Check formatting without modifying files"
	@echo "  draft-release      Generate RELEASE_NOTES.md draft from git log"
	@echo "  tag VERSION=vX.Y.Z Tag a new release"
	@echo "  release-check      Run lint + tests + build (pre-release gate)"
	@echo "  clean              Remove build artifacts and data files"
	@echo "  tidy               Run go mod tidy"
	@echo "  help               Show this help"
