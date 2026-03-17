# Release v0.1.0

> **Initial release — 2026-03-17**

## What's Changed

### Features

- **HTTP REST API** — ingest records (`POST /api/v1/records`), execute DuckDB SQL queries (`POST /api/v1/query`), trigger sync (`POST /api/v1/sync`), rebuild Parquet index (`POST /api/v1/rebuild`), health check (`GET /health`), Prometheus metrics (`GET /metrics`)
- **MCP interface** — 7 tools (`write_record`, `query_records`, `get_schema`, `list_files`, `sync_now`, `rebuild_index`, `get_health`) over stdio and HTTP/SSE transports; compatible with Claude Desktop and remote AI agents
- **Daily-rolling JSONL storage** — records written by a single goroutine to `data/data_YYYY-MM-DD.jsonl` with 64 KB `bufio.Writer`; file rolls atomically at midnight UTC
- **CSV format support** — opt-in via `FORMAT=csv` / `--format=csv`; schema column order preserved in output
- **Atomic Parquet sync** — DuckDB `COPY TO` writes to a `.tmp` file then `os.Rename` for POSIX-atomic replacement; no partial reads
- **Live query CTE** — every query transparently unions historical `*.parquet` files with the current live JSONL file; today's Parquet is excluded to prevent double-counting
- **Schema validation** — all records validated at ingest against `schema.json`; unknown fields and type mismatches rejected with 400 and detailed error messages
- **Schema versioning** — SHA-256 of `schema.json` stored in `data/.schema_version`; process exits with an actionable error if the schema changes between restarts
- **Three sync triggers** — periodic ticker (default 30 s), record-count threshold (default every 1 000 records), and roll event (immediate sync of the previous day's file on date boundary)
- **Prometheus observability** — 8 metrics: `records_ingested_total`, `records_rejected_total{reason}`, `ingestion_channel_depth`, `parquet_sync_duration_seconds`, `sync_failures_total`, `query_latency_seconds`, `query_errors_total{reason}`, `active_syncs`
- **Structured JSON logging** — via `log/slog`; level configurable via `LOG_LEVEL` environment variable
- **Graceful shutdown** — on SIGTERM/SIGINT: stop HTTP, drain ingest channel, flush bufio, final Parquet sync, close DuckDB instances, stop MCP server — no data loss
- **SQL guard** — 21 forbidden keywords block all DDL, DML, and file operations; token-aware check avoids false positives inside string literals
- **Rebuild mode** — `--rebuild` flag and `POST /api/v1/rebuild` re-sync every source file; used after schema changes or Parquet corruption
- **Version injection** — binary version set at build time via `-X main.version=vX.Y.Z` ldflags

### CI/CD

- **GitHub Actions CI pipeline** — lint (golangci-lint v1.61.0) + unit tests + integration tests on ubuntu-latest and macos-latest; triggered on push and pull requests to `main`/`master`
- **GitHub Actions release pipeline** — triggered on `v*.*.*` tags; builds native binaries for linux/amd64, darwin/arm64, darwin/amd64; generates SHA-256 checksums; creates GitHub release with release notes
- **AI-agent-friendly release workflow** — `scripts/gen-release-notes.sh` generates a `RELEASE_NOTES.md` draft from `git log` grouped by conventional commit prefix; agent reviews, commits, tags, and pushes
- **golangci-lint configuration** — 16 linters enabled including `errcheck`, `staticcheck`, `govet/shadow`, `cyclop` (max complexity 15), `gocritic`, `misspell`, `noctx`

### Tests

- **198 unit tests** across all packages with race detector enabled
- **18 integration tests** exercising the full HTTP stack with real DuckDB: ingest→sync→query round-trip, live JSONL visibility before sync, no double-count after multiple syncs, schema validation, forbidden SQL blocking, concurrent ingestion, Prometheus metrics endpoint, health check, column selection, aggregation, filtering, and rebuild

## Breaking Changes

None — this is the initial release.

## Migration Notes

No migration required.

## Configuration

See [`README.md`](README.md#configuration) for the full list of environment variables and flags. Minimum required to run:

```bash
./bin/server
# Serves HTTP on :8080, MCP SSE on :8081
# Writes data to ./data, reads schema from ./schema.json
```

## Checksums

<!-- Populated automatically by the release CI job -->
