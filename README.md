# loc-qrs

A production-grade **Go CQRS system** for high-throughput record ingestion, columnar storage, and real-time SQL analytics. Records arrive via HTTP REST or MCP (AI agent protocol), land in daily-rolling JSONL files, are synced atomically to Parquet via an embedded DuckDB engine, and are queried through a live union of historical Parquet files and the current unflushed write file — with no double-counting, no data loss on shutdown, and full Prometheus observability.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [System Diagram](#system-diagram)
  - [Key Design Decisions](#key-design-decisions)
  - [Concurrency Model](#concurrency-model)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run](#run)
  - [Quick Smoke Test](#quick-smoke-test)
- [Configuration](#configuration)
- [Schema](#schema)
  - [Supported Column Types](#supported-column-types)
  - [Schema Versioning](#schema-versioning)
  - [Customizing the Schema](#customizing-the-schema)
- [HTTP API](#http-api)
  - [POST /api/v1/records](#post-apiv1records)
  - [POST /api/v1/query](#post-apiv1query)
  - [POST /api/v1/sync](#post-apiv1sync)
  - [POST /api/v1/rebuild](#post-apiv1rebuild)
  - [GET /health](#get-health)
  - [GET /metrics](#get-metrics)
- [MCP Interface](#mcp-interface)
  - [Tools Reference](#tools-reference)
  - [Stdio Mode (Claude Desktop)](#stdio-mode-claude-desktop)
  - [SSE Mode (Remote Agents)](#sse-mode-remote-agents)
- [Query Engine](#query-engine)
  - [SQL Rules](#sql-rules)
  - [CTE Structure](#cte-structure)
  - [Live vs Historical Data](#live-vs-historical-data)
- [Sync Mechanism](#sync-mechanism)
  - [Sync Triggers](#sync-triggers)
  - [Atomic File Replacement](#atomic-file-replacement)
  - [Rebuild](#rebuild)
- [Observability](#observability)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Structured Logging](#structured-logging)
- [Data Files](#data-files)
- [Development](#development)
  - [Running Tests](#running-tests)
  - [Linting](#linting)
  - [Code Structure](#code-structure)
- [CI/CD & Releases](#cicd--releases)
  - [CI Pipeline](#ci-pipeline)
  - [Release Pipeline](#release-pipeline)
  - [Creating a Release](#creating-a-release)
- [Deployment](#deployment)
  - [Docker](#docker)
  - [Environment Variables Reference](#environment-variables-reference)
- [Troubleshooting](#troubleshooting)

---

## Overview

**loc-qrs** solves a common problem in analytics backends: you need fast ingestion, low-latency queries against live data, and long-term efficient columnar storage — all without running a separate database server or managing complex infrastructure.

**What it does:**

- **Ingest** records at high throughput via HTTP or MCP; records are validated against a schema, encoded (JSONL or CSV), and written to a daily-rolling file by a single goroutine with no lock contention
- **Store** data in date-partitioned files (`data/data_YYYY-MM-DD.jsonl`) that roll over at midnight UTC
- **Sync** data atomically to Parquet (`.parquet`) using an embedded DuckDB instance — no separate database process required
- **Query** data via DuckDB SQL through a CTE that transparently unions historical Parquet files with the live write file, so queries always reflect data up to the most recent flush
- **Expose** all operations to AI agents via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) — agents can write records, run SQL queries, inspect the schema, and manage syncs without HTTP glue code

**What it is not:**

- A distributed system — it runs on a single host with a single writer goroutine
- A general-purpose database — the schema is defined upfront and validated at ingest
- An OLTP engine — it is optimised for append-only writes and analytical reads

---

## Architecture

### System Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│  Ingest Layer                                                        │
│                                                                      │
│   HTTP POST /api/v1/records          MCP tool: write_record          │
│              │                                    │                  │
│              └──────────────┬─────────────────────┘                  │
│                             │ validate + encode                      │
│                             ▼                                        │
│                  chan Record (capacity 10 000)                       │
│                  (non-blocking; 503 if full)                         │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Write Layer (single goroutine)                                      │
│                                                                      │
│   FileWriter.run()                                                   │
│     ├── bufio.Writer (64 KB buffer) → data/data_YYYY-MM-DD.jsonl    │
│     ├── Rolls file at midnight UTC                                   │
│     ├── Sends roll events → SyncWorker                              │
│     └── Fires sync trigger every N records                          │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
              ┌───────────────┴──────────────┐
              │ Flush + COPY TO              │
              ▼                              ▼
┌─────────────────────────┐   ┌─────────────────────────────────────┐
│  Sync Layer             │   │  Query Layer                        │
│                         │   │                                     │
│  SyncWorker             │   │  QueryEngine                        │
│   ├── Ticker (30s)      │   │   ├── GuardSQL (blocklist)          │
│   ├── Record-count trig │   │   ├── BuildQuery (CTE union)        │
│   └── Roll event        │   │   └── DuckDB queryDB (in-memory)    │
│                         │   │                                     │
│  DuckDB syncDB          │   │  WITH                               │
│  COPY (SELECT * FROM    │   │    _hist AS (read_parquet([...]))   │
│    read_json_auto(...)) │   │    _live AS (read_json_auto(...))   │
│  TO 'data.tmp.parquet'  │   │    records AS (                     │
│  os.Rename(tmp → final) │   │      SELECT * FROM _hist            │
│                         │   │      UNION ALL                      │
│  data_YYYY-MM-DD.parquet│   │      SELECT * FROM _live)           │
└─────────────────────────┘   │  <user SQL>                         │
                              └─────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| **Channel type** | `chan Record` (pre-encoded `[]byte`) | Zero allocation in writer goroutine |
| **DuckDB instances** | Two: `syncDB` (read-write) + `queryDB` (in-memory) | No global mutex; reads never block writes |
| **Write format** | JSONL default, CSV opt-in | Handles embedded commas and quotes; `read_json_auto` is robust |
| **SQL safety** | In-memory queryDB + keyword blocklist | Defence-in-depth; blocklist catches obvious attacks; in-memory DuckDB can't persist changes |
| **Live query** | CTE union of `data_*.parquet` + unsynced JSONL | Transparent to callers; `records` alias works for any SQL |
| **Sync trigger** | `syncTriggerCh chan struct{}` + `time.Ticker` | Zero-CPU dual trigger; channel coalesces concurrent fires |
| **Schema versioning** | SHA-256 of `schema.json` → `.schema_version` | Fails fast on schema drift; requires explicit `--rebuild` |
| **Graceful shutdown** | Drain channel → flush bufio → final sync → close DBs | No data loss on SIGTERM |
| **MCP transport** | stdio (local) + HTTP/SSE (remote) | Supports Claude Desktop and remote AI agents |

### Concurrency Model

```
HTTP handlers (N goroutines)
    │  validate → encode → chan <- Record  (non-blocking; 503 if full)
    ▼
chan Record (buffered, 10 000)
    │
    ▼
FileWriter goroutine (×1)                ◄── rollCh: date change events
    │  bw.Write(payload)
    │  counter++ → syncTriggerCh (every N)
    ▼
data/data_YYYY-MM-DD.jsonl (bufio, 64 KB)

SyncWorker goroutine (×1)
    │  select { case <-ticker | <-triggerCh | <-rollCh }
    │  FileWriter.Flush()   ← drains ch then flushes bufio
    │  COPY TO data.tmp.parquet  (syncDB)
    │  os.Rename(tmp → final)
    ▼
data/data_YYYY-MM-DD.parquet

QueryEngine (calls queryDB — in-memory DuckDB)
    │  CTE union of Parquet + live JSONL
    ▼
HTTP/MCP response

Shutdown sequence (SIGTERM):
  1. http.Shutdown()       — stop accepting requests
  2. FileWriter.Stop()     — close ch, drain, flush
  3. SyncWorker.SyncNow()  — final Parquet of today's data
  4. syncDB.Close()
  5. queryDB.Close()
  6. mcpServer.Shutdown()
```

---

## Getting Started

### Prerequisites

| Requirement | Minimum | Notes |
|-------------|---------|-------|
| Go | 1.24 | `go-duckdb` requires CGO |
| C compiler | any | `gcc` on Linux, `clang` on macOS (Xcode CLT) |
| Make | any | For `make build`, `make test`, etc. |

> **CGO is required.** The `go-duckdb` driver bundles DuckDB as a static library. All build and test commands must use `CGO_ENABLED=1`.

### Build

```bash
# Standard build
make build
# → bin/server

# With explicit version
make build VERSION=v1.2.3

# Manual
CGO_ENABLED=1 go build \
  -ldflags="-s -w -X main.version=v1.2.3" \
  -o bin/server \
  ./cmd/server
```

### Run

```bash
# Start with defaults (HTTP on :8080, data in ./data, schema from ./schema.json)
./bin/server

# Custom configuration via environment variables
HTTP_ADDR=:9000 \
DATA_DIR=/var/lib/loc-qrs/data \
SCHEMA_PATH=/etc/loc-qrs/schema.json \
LOG_LEVEL=debug \
./bin/server

# Force rebuild of all Parquet files on startup
./bin/server --rebuild

# Run MCP over stdio (for Claude Desktop)
./bin/server --mcp-stdio
```

### Quick Smoke Test

```bash
# 1. Start the server
./bin/server &

# 2. Ingest a record
curl -s -X POST http://localhost:8080/api/v1/records \
  -H 'Content-Type: application/json' \
  -d '{"record":{"id":1,"event_name":"pageview","value":1.0}}'
# → {"status":"accepted"}

# 3. Sync to Parquet
curl -s -X POST http://localhost:8080/api/v1/sync
# → {"status":"synced"}

# 4. Query
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) FROM records"}'
# → {"columns":["count_star()"],"rows":[[1]]}

# 5. Health check
curl -s http://localhost:8080/health
# → {"status":"ok","duckdb":"alive","channel_fill_pct":0}

# 6. Prometheus metrics
curl -s http://localhost:8080/metrics | grep records_ingested_total
# → records_ingested_total 1
```

---

## Configuration

All configuration is read from **environment variables** first, then **command-line flags**, with hardcoded defaults as fallback.

| Environment Variable | Flag | Default | Description |
|----------------------|------|---------|-------------|
| `HTTP_ADDR` | `--http-addr` | `:8080` | HTTP server listen address |
| `MCP_ADDR` | `--mcp-addr` | `:8081` | MCP SSE server listen address |
| `DATA_DIR` | `--data-dir` | `./data` | Directory for JSONL, CSV, and Parquet files |
| `SCHEMA_PATH` | `--schema` | `./schema.json` | Path to the schema definition file |
| `FORMAT` | `--format` | `jsonl` | Write format: `jsonl` or `csv` |
| `CHANNEL_CAPACITY` | `--channel-capacity` | `10000` | Ingest channel buffer depth (records) |
| `SYNC_INTERVAL` | `--sync-interval` | `30s` | Automatic periodic sync interval |
| `SYNC_RECORD_COUNT` | `--sync-record-count` | `1000` | Trigger sync every N records written |
| `SHUTDOWN_TIMEOUT` | `--shutdown-timeout` | `30s` | Graceful shutdown deadline |
| `LOG_LEVEL` | *(env only)* | `info` | Log level: `debug`, `info`, `warn`, `error` |
| — | `--mcp-stdio` | `false` | Run MCP over stdio (disables HTTP server) |
| — | `--rebuild` | `false` | Rebuild all Parquet files on startup |

**Example production configuration:**

```bash
export HTTP_ADDR=":8080"
export MCP_ADDR=":8081"
export DATA_DIR="/data/loc-qrs"
export SCHEMA_PATH="/etc/loc-qrs/schema.json"
export FORMAT="jsonl"
export CHANNEL_CAPACITY="50000"
export SYNC_INTERVAL="10s"
export SYNC_RECORD_COUNT="5000"
export SHUTDOWN_TIMEOUT="60s"
export LOG_LEVEL="info"
```

---

## Schema

The schema is defined in `schema.json` and controls which fields are accepted at ingest, what types they must be, and in what order they are written in CSV mode.

### Default Schema

```json
{
  "columns": {
    "id":         "UBIGINT",
    "event_name": "VARCHAR",
    "timestamp":  "TIMESTAMP",
    "value":      "DOUBLE",
    "metadata":   "VARCHAR"
  },
  "format": "jsonl"
}
```

### Supported Column Types

| Type | Go equivalent | Notes |
|------|---------------|-------|
| `UBIGINT` | `uint64` | Unsigned 64-bit integer; also accepts JSON number |
| `BIGINT` | `int64` | Signed 64-bit integer |
| `INTEGER` | `int32` | Signed 32-bit integer |
| `DOUBLE` | `float64` | Double-precision float |
| `FLOAT` | `float32` | Single-precision float |
| `BOOLEAN` | `bool` | `true` / `false` |
| `VARCHAR` | `string` | UTF-8 string |
| `TIMESTAMP` | `string` | ISO 8601 string; validated as parseable |
| `DATE` | `string` | ISO 8601 date string |
| `JSON` | `string` | Raw JSON string (not parsed) |

### Schema Versioning

On every startup, loc-qrs computes the **SHA-256 hash** of `schema.json` and compares it to the stored version in `data/.schema_version`.

- **First run**: Writes the current hash to `.schema_version`
- **Same schema**: Starts normally
- **Schema changed**: Exits with an error message — you must run with `--rebuild` to re-sync all existing data to the new schema

```bash
# After changing schema.json
./bin/server --rebuild
# Rebuilds all data_*.jsonl → data_*.parquet, updates .schema_version
```

### Customizing the Schema

Edit `schema.json` to match your data model. Field order in the JSON object is preserved (using streaming decode) and determines column order in CSV output. You may add, remove, or rename fields — but any change requires a rebuild.

```json
{
  "columns": {
    "user_id":    "UBIGINT",
    "session_id": "VARCHAR",
    "action":     "VARCHAR",
    "duration_ms":"BIGINT",
    "page":       "VARCHAR",
    "created_at": "TIMESTAMP"
  },
  "format": "jsonl"
}
```

> **Unknown fields at ingest are rejected.** The validator enforces that every field in the record exists in the schema and has the correct type.

---

## HTTP API

All API responses use `Content-Type: application/json`. All requests to `/api/v1/*` that include a body should set `Content-Type: application/json`.

### POST /api/v1/records

Ingest a single record. The record is validated against the schema, encoded, and enqueued for writing. Returns immediately — the write is asynchronous.

**Request body:**

```json
{
  "record": {
    "id": 42,
    "event_name": "purchase",
    "timestamp": "2026-03-17T14:22:00Z",
    "value": 99.99,
    "metadata": "{\"currency\":\"USD\"}"
  }
}
```

All fields are optional at the HTTP layer (missing fields are stored as null/zero), but required fields defined as non-nullable in DuckDB will cause query errors if null values are queried with operations that do not handle nulls.

**Responses:**

| Status | Meaning | Body |
|--------|---------|------|
| `202 Accepted` | Record enqueued | `{"status":"accepted"}` |
| `400 Bad Request` | Invalid JSON | `{"error":"invalid JSON","detail":"..."}` |
| `400 Bad Request` | Missing `record` field | `{"error":"missing 'record' field"}` |
| `400 Bad Request` | Schema validation failed | `{"error":"validation failed","detail":["..."]}` |
| `503 Service Unavailable` | Ingest channel full | `{"error":"server busy, channel full"}` |

**Example:**

```bash
curl -X POST http://localhost:8080/api/v1/records \
  -H 'Content-Type: application/json' \
  -d '{"record":{"id":1,"event_name":"click","value":1.0}}'
```

---

### POST /api/v1/query

Execute a read-only DuckDB SQL query. Reference the unified dataset as the `records` table — the query engine automatically constructs a CTE union of all available data.

**Request body:**

```json
{
  "sql": "SELECT event_name, COUNT(*) as n, SUM(value) as total FROM records GROUP BY event_name ORDER BY n DESC"
}
```

**Response (200 OK):**

```json
{
  "columns": ["event_name", "n", "total"],
  "rows": [
    ["purchase", 120, 11988.80],
    ["click",    890, 890.0],
    ["pageview", 4521, 4521.0]
  ]
}
```

**Error responses:**

| Status | Meaning | Body |
|--------|---------|------|
| `400 Bad Request` | Missing `sql` | `{"error":"missing 'sql' field"}` |
| `400 Bad Request` | Invalid JSON | `{"error":"invalid JSON"}` |
| `400 Bad Request` | Forbidden SQL keyword | `{"error":"query guard: forbidden keyword: DROP"}` |
| `400 Bad Request` | DuckDB execution error | `{"error":"query execute: <duckdb error>"}` |

**Forbidden SQL keywords** (case-insensitive):

`INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `REPLACE`, `MERGE`, `GRANT`, `REVOKE`, `ATTACH`, `DETACH`, `LOAD`, `IMPORT`, `EXPORT`, `COPY`, `SET`, `PRAGMA`, `CALL`, `EXECUTE`

**Example queries:**

```sql
-- Count by event
SELECT event_name, COUNT(*) FROM records GROUP BY event_name;

-- Time-range filter
SELECT * FROM records WHERE timestamp >= '2026-03-01' AND timestamp < '2026-04-01';

-- Aggregate with window functions
SELECT date_trunc('hour', timestamp) as hour, SUM(value) as revenue
FROM records
WHERE event_name = 'purchase'
GROUP BY 1
ORDER BY 1;

-- Recent records
SELECT * FROM records ORDER BY timestamp DESC LIMIT 100;
```

---

### POST /api/v1/sync

Trigger an immediate synchronous JSONL/CSV → Parquet sync. Blocks until the sync completes.

**Request body:** empty

**Responses:**

| Status | Meaning | Body |
|--------|---------|------|
| `200 OK` | Sync succeeded | `{"status":"synced"}` |
| `500 Internal Server Error` | Sync failed | `{"error":"sync failed: <reason>"}` |

```bash
curl -X POST http://localhost:8080/api/v1/sync
```

---

### POST /api/v1/rebuild

Re-sync all JSONL/CSV files in the data directory to Parquet. Use this after changing the schema or to repair corrupted Parquet files.

**Request body:** empty

**Responses:**

| Status | Meaning | Body |
|--------|---------|------|
| `200 OK` | Rebuild succeeded | `{"status":"rebuilt"}` |
| `500 Internal Server Error` | Rebuild failed | `{"error":"rebuild failed: <reason>"}` |

```bash
curl -X POST http://localhost:8080/api/v1/rebuild
```

---

### GET /health

Returns the system health status. Useful for load balancer health checks and monitoring probes.

**Response (200 OK — healthy):**

```json
{
  "status": "ok",
  "duckdb": "alive",
  "channel_fill_pct": 3.2
}
```

**Response (503 Service Unavailable — degraded):**

```json
{
  "status": "degraded",
  "duckdb": "unreachable",
  "channel_fill_pct": 0
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `"ok"` or `"degraded"` |
| `duckdb` | string | `"alive"` or `"unreachable"` |
| `channel_fill_pct` | float | Ingest channel fill percentage (0–100) |

---

### GET /metrics

Returns Prometheus metrics in text exposition format.

```bash
curl http://localhost:8080/metrics
```

See [Prometheus Metrics](#prometheus-metrics) for the full list.

---

## MCP Interface

loc-qrs implements the [Model Context Protocol](https://modelcontextprotocol.io/) (MCP), exposing all system operations as tools that AI agents can invoke. This enables Claude and other MCP-compatible agents to ingest data, run SQL queries, inspect the schema, and manage syncs without writing HTTP client code.

Two transport modes are supported:

- **stdio** — for local use with Claude Desktop or the `claude` CLI
- **HTTP/SSE** — for remote agents connecting over a network

### Tools Reference

#### `write_record`

Ingest a single record into the system.

```
Parameters:
  record (object, required) — The record fields as a JSON object

Returns:
  "record accepted" on success, or an error message
```

**Example agent invocation:**
> "Write a record with id=1, event_name='login', value=1.0"

---

#### `query_records`

Execute a DuckDB SQL query against all ingested records. The unified dataset is always available as `records`.

```
Parameters:
  sql (string, required) — SQL SELECT query. Use 'records' as the table name.

Returns:
  JSON with "columns" (array of strings) and "rows" (array of arrays)
```

**Example agent invocation:**
> "How many records were ingested today?"
> → `SELECT COUNT(*) FROM records WHERE timestamp >= today()`

---

#### `get_schema`

Return the current schema definition and version hash.

```
Parameters: none

Returns:
  {
    "version": "sha256hex...",
    "format": "jsonl",
    "columns": { "id": "UBIGINT", ... },
    "ordered": ["id", "event_name", ...]
  }
```

---

#### `list_files`

List all data files in the data directory with their sizes and modification times.

```
Parameters: none

Returns:
  [
    {
      "path": "/data/data_2026-03-17.jsonl",
      "size_bytes": 40960,
      "modified": "2026-03-17T14:30:00Z"
    },
    ...
  ]
```

---

#### `sync_now`

Trigger an immediate JSONL/CSV → Parquet sync and wait for it to complete.

```
Parameters: none

Returns:
  "sync completed" on success, or an error message
```

---

#### `rebuild_index`

Rebuild all Parquet files from their JSONL/CSV sources.

```
Parameters: none

Returns:
  "rebuild completed" on success, or an error message
```

---

#### `get_health`

Return system health information.

```
Parameters: none

Returns:
  {
    "status": "ok",
    "channel_fill_pct": 0.0
  }
```

---

### Stdio Mode (Claude Desktop)

Add to your Claude Desktop `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "loc-qrs": {
      "command": "/path/to/bin/server",
      "args": ["--mcp-stdio"],
      "env": {
        "DATA_DIR": "/path/to/data",
        "SCHEMA_PATH": "/path/to/schema.json"
      }
    }
  }
}
```

In stdio mode, the HTTP server is not started. The process reads JSON-RPC messages from stdin and writes responses to stdout.

### SSE Mode (Remote Agents)

Start the server normally. The MCP SSE endpoint is available at `http://localhost:8081` (configurable via `MCP_ADDR`).

```bash
# Start server (both HTTP + MCP SSE)
./bin/server

# Agents connect to the SSE endpoint
# http://localhost:8081/sse
```

---

## Query Engine

### SQL Rules

Queries are validated before execution:

1. **Forbidden keywords** are blocked to prevent data modification, file operations, and DDL. The check is token-aware — a keyword inside a string literal (e.g. `WHERE name LIKE '%DROP%'`) is **not** blocked, only standalone tokens are.

2. **The `records` table alias** is always available — you never need to reference file paths or Parquet files directly.

3. **DuckDB SQL dialect** is supported in full for read operations: window functions, CTEs, `UNNEST`, `STRUCT`, JSON extraction, date functions, `COPY TO` is blocked but aggregations, joins, and subqueries are all available.

### CTE Structure

Every query is automatically wrapped in a CTE before execution. The structure depends on what data is available:

**Full case (historical Parquet + live JSONL):**

```sql
WITH
  _hist AS (
    SELECT * FROM read_parquet([
      '/data/data_2026-03-15.parquet',
      '/data/data_2026-03-16.parquet'
    ], union_by_name=true)
  ),
  _live AS (
    SELECT * FROM read_json_auto(
      '/data/data_2026-03-17.jsonl',
      format='newline_delimited'
    )
  ),
  records AS (
    SELECT * FROM _hist
    UNION ALL
    SELECT * FROM _live
  )
<YOUR SQL>
```

**Only live file (first day, no historical):**

```sql
WITH
  _live AS (
    SELECT * FROM read_json_auto('/data/data_2026-03-17.jsonl', format='newline_delimited')
  ),
  records AS (SELECT * FROM _live)
<YOUR SQL>
```

**No data at all:**

```sql
WITH records AS (SELECT NULL LIMIT 0)
<YOUR SQL>
```

### Live vs Historical Data

The query engine always includes **today's JSONL file** in the live CTE and **excludes today's Parquet** from the historical CTE. This is the anti-double-count invariant.

| Scenario | Historical (`_hist`) | Live (`_live`) |
|----------|----------------------|----------------|
| Before first sync | — | today's JSONL |
| After sync (same day) | — | today's JSONL (now matches Parquet content) |
| Next day | yesterday's Parquet | today's new JSONL |
| Multi-day | all past Parquets | today's JSONL |

**Pre-sync queries:** Data written to the channel but not yet flushed to disk (`bufio` 64 KB buffer) will not appear in queries until a sync is triggered. Call `POST /api/v1/sync` or wait for automatic sync to ensure all data is visible.

---

## Sync Mechanism

### Sync Triggers

The sync worker fires on any of three signals, whichever arrives first:

1. **Periodic ticker** — every `SYNC_INTERVAL` (default 30 seconds)
2. **Record-count trigger** — every `SYNC_RECORD_COUNT` records written (default 1000), sent by the FileWriter goroutine
3. **Roll event** — when the date changes and the FileWriter opens a new file, it emits the old file's path for immediate syncing

Concurrent triggers are coalesced — if a sync is already in progress, additional triggers are dropped.

### Sync Algorithm

For each `data_YYYY-MM-DD.jsonl` file found in the data directory:

1. Check file exists and is non-empty (skip otherwise)
2. Compute destination path: `data_YYYY-MM-DD.parquet`
3. Execute:
   ```sql
   COPY (SELECT * FROM read_json_auto('data_YYYY-MM-DD.jsonl', format='newline_delimited'))
   TO 'data_YYYY-MM-DD.parquet.tmp' (FORMAT PARQUET)
   ```
4. Atomically rename: `os.Rename("data_YYYY-MM-DD.parquet.tmp", "data_YYYY-MM-DD.parquet")`

**Before step 3**, the sync worker calls `FileWriter.Flush()` which:
1. Drains any records still queued in the channel
2. Flushes the `bufio.Writer` to disk

This ensures the JSONL file is fully flushed before DuckDB reads it.

### Atomic File Replacement

Parquet files are written to a `.tmp` file first, then atomically renamed using `os.Rename`. This is a POSIX atomic operation — readers always see either the old complete file or the new complete file, never a partial write.

If the process crashes during a sync:
- The `.tmp` file is left on disk
- On the next startup/sync, the `.tmp` is deleted before the new write begins
- The previous complete Parquet (if any) remains intact

### Rebuild

Running with `--rebuild` (or calling `POST /api/v1/rebuild`) re-syncs every JSONL/CSV file in the data directory. This is needed when:

- The schema changes
- A Parquet file is deleted or corrupted
- Data was added to a JSONL file outside of loc-qrs

---

## Observability

### Prometheus Metrics

All metrics are exposed at `GET /metrics`. In test builds they use an isolated registry; in production they use the default Prometheus global registry.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `records_ingested_total` | Counter | — | Records successfully enqueued for writing |
| `records_rejected_total` | CounterVec | `reason` | Rejected records (`schema` \| `channel_full`) |
| `ingestion_channel_depth` | Gauge | — | Current number of records buffered in the ingest channel |
| `parquet_sync_duration_seconds` | Histogram | — | End-to-end duration of each sync operation |
| `sync_failures_total` | Counter | — | Number of failed file sync operations |
| `query_latency_seconds` | Histogram | — | Total query execution time (validation + build + DuckDB) |
| `query_errors_total` | CounterVec | `reason` | Query errors (`forbidden` \| `build` \| `execute`) |
| `active_syncs` | Gauge | — | Currently running sync operations (0 or 1 normally) |

**Example Grafana alert rules:**

```yaml
# Channel saturation
- alert: IngestChannelNearFull
  expr: ingestion_channel_depth / 10000 > 0.8
  for: 1m
  annotations:
    summary: "Ingest channel >80% full — ingestion will 503 soon"

# Sync falling behind
- alert: SyncLatencyHigh
  expr: histogram_quantile(0.99, parquet_sync_duration_seconds_bucket) > 5
  for: 5m
  annotations:
    summary: "99th percentile sync duration >5s"

# Errors
- alert: SyncFailures
  expr: rate(sync_failures_total[5m]) > 0
  annotations:
    summary: "Parquet sync failures detected"
```

### Structured Logging

loc-qrs uses Go's standard `log/slog` with a JSON handler.

**Log levels:**

| Level | Events |
|-------|--------|
| `DEBUG` | SQL queries before execution |
| `INFO` | HTTP requests (method, path, status, duration), sync completions, server start/stop |
| `WARN` | Flush errors on roll, roll channel full, HTTP shutdown errors |
| `ERROR` | Write failures, panic recovery, final flush errors |

**Request log example:**

```json
{
  "time": "2026-03-17T14:22:01.234Z",
  "level": "INFO",
  "msg": "request",
  "method": "POST",
  "path": "/api/v1/records",
  "status": 202,
  "duration_ms": 1,
  "remote": "10.0.0.1:54321"
}
```

**Set log level:**

```bash
LOG_LEVEL=debug ./bin/server
```

---

## Data Files

loc-qrs creates the following files in the data directory (`--data-dir`, default `./data`):

| Pattern | Description |
|---------|-------------|
| `data_YYYY-MM-DD.jsonl` | Daily write file; appended by FileWriter; flushed on sync |
| `data_YYYY-MM-DD.parquet` | Immutable Parquet snapshot; atomically replaced on each sync |
| `data_YYYY-MM-DD.parquet.tmp` | Transient file during sync; deleted on next sync start |
| `.schema_version` | SHA-256 of current `schema.json`; checked on startup |
| `.sync.duckdb` | DuckDB database file used by the sync worker |

**File naming convention:** The date is in UTC and uses the format `YYYY-MM-DD`. Both the JSONL and Parquet files for a given day share the same date component, which the query engine uses to match them and exclude today's Parquet when building the CTE.

**Disk usage estimate:** At 1 KB per JSON record:

| Ingest rate | Daily JSONL | Daily Parquet (est.) |
|-------------|-------------|----------------------|
| 1 000 rec/s | ~86 GB | ~5–10 GB |
| 100 rec/s | ~8.6 GB | ~500 MB–1 GB |
| 10 rec/s | ~860 MB | ~50–100 MB |

Parquet files use Snappy compression by default (DuckDB's default), typically achieving 5–15× compression over raw JSONL depending on data entropy.

---

## Development

### Running Tests

```bash
# Unit tests (no DuckDB required)
make test
# CGO_ENABLED=1 go test ./... -race -count=1 -timeout=120s

# Integration tests (requires CGO + DuckDB)
make test-integration
# CGO_ENABLED=1 go test ./... -race -count=1 -timeout=180s -tags=integration

# All tests
make test-all

# Coverage report
make test-coverage
# Opens coverage.html in current directory

# Quick tests (skip slow subtests)
make test-short
```

Integration tests are isolated with `//go:build integration` and are not run by `make test`. They wire up real DuckDB instances in temporary directories and exercise the full HTTP stack end-to-end.

### Linting

```bash
# Run all linters
make lint

# Auto-fix where possible
make lint-fix

# Check formatting only
make fmt-check

# Format all files
make fmt
```

The linter configuration is in `.golangci.yml`. Enabled linters include: `bodyclose`, `cyclop` (max complexity 15), `errcheck`, `exhaustive`, `gocritic`, `gofmt`, `goimports`, `govet` (with shadow), `ineffassign`, `misspell`, `noctx`, `revive`, `staticcheck`, and `unused`.

### Code Structure

```
loc-qrs/
├── cmd/server/
│   ├── main.go          # Wiring, signal handling, ordered shutdown
│   └── version.go       # var version = "dev"  (overridden by ldflags)
├── internal/
│   ├── api/             # HTTP handlers (ingest, query, admin, health, middleware)
│   ├── config/          # Config loading from env + flags
│   ├── mcp/             # MCP server (7 tools, stdio + SSE transports)
│   ├── observability/   # Prometheus metrics + slog setup
│   ├── query/           # QueryEngine, CTE builder, SQL guard
│   ├── schema/          # Schema parsing (order-preserving), SHA-256 versioning, validator
│   ├── sync/            # SyncWorker, DuckDB COPY TO, atomic rename
│   ├── testutil/        # Shared test helpers (no CGO dependency in main file)
│   └── writer/          # FileWriter, DailyRotator, JSONLEncoder, CSVEncoder
├── scripts/
│   └── gen-release-notes.sh   # Generate RELEASE_NOTES.md from git log
├── .github/workflows/
│   ├── ci.yml           # Lint + test + build on every push/PR
│   └── release.yml      # Multi-platform build + GitHub release on tags
├── Makefile
├── schema.json          # Default schema (edit to customise)
├── go.mod
└── .golangci.yml
```

**Package dependency order** (no cycles):

```
testutil ← schema, observability
writer   ← schema, observability
sync     ← writer, observability
query    ← writer, observability
api      ← writer, query, sync, schema, observability
mcp      ← writer, query, sync, schema, observability
cmd      ← api, mcp, config, observability
```

---

## CI/CD & Releases

### CI Pipeline

The CI pipeline (`.github/workflows/ci.yml`) runs on every push to `main`/`master` and on all pull requests.

**Jobs (run in parallel):**

1. **lint** (ubuntu-latest)
   - Runs `golangci-lint v1.61.0` with a 5-minute timeout

2. **test** (matrix: ubuntu-latest × macos-latest)
   - Unit tests with race detector
   - Integration tests with race detector
   - Uploads coverage artifact (ubuntu only)

3. **build** (ubuntu-latest, after test)
   - Builds `linux-amd64` binary
   - Uploads as artifact for inspection

### Release Pipeline

The release pipeline (`.github/workflows/release.yml`) runs on tags matching `v[0-9]+.[0-9]+.[0-9]+*`.

**Jobs:**

1. **quality** — same lint + test matrix as CI
2. **build** — matrix of 3 native runners:
   - `ubuntu-latest` → `server-linux-amd64`
   - `macos-latest` → `server-darwin-arm64`
   - `macos-13` → `server-darwin-amd64`

   > CGO cross-compilation is not used. Each platform is compiled natively to avoid go-duckdb build complexity.

3. **release** — downloads all 3 binaries, generates SHA-256 checksums, creates the GitHub release with `RELEASE_NOTES.md` as the body.

### Creating a Release

The release notes workflow is designed to be **AI-agent-friendly**: generate a draft, review it, and push the tag.

```bash
# 1. Generate RELEASE_NOTES.md from git log since the last tag
make draft-release
# Or: make draft-release FROM=v0.1.0 TO=HEAD

# 2. Review and edit RELEASE_NOTES.md
#    (or ask an AI agent to summarise the changes)

# 3. Commit the release notes
git add RELEASE_NOTES.md
git commit -m "chore: release notes for v0.2.0"

# 4. Run the pre-release validation gate
make release-check  # runs lint + test-all + build

# 5. Tag and push
make tag VERSION=v0.2.0
git push origin v0.2.0

# The release pipeline creates the GitHub release automatically.
```

**Conventional commit prefixes** recognised by `gen-release-notes.sh`:

| Prefix | Section |
|--------|---------|
| `feat:` | Features |
| `fix:` | Bug Fixes |
| `perf:` | Performance |
| `refactor:` | Refactoring |
| `docs:` | Documentation |
| `test:` | Tests |
| `chore:` | Internal / chores |
| *(other)* | Other Changes |

---

## Deployment

### Docker

loc-qrs requires CGO. Use a multi-stage build with a C compiler in the build stage:

```dockerfile
FROM golang:1.24-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o bin/server ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/bin/server /usr/local/bin/server
COPY schema.json /etc/loc-qrs/schema.json

ENV DATA_DIR=/data
ENV SCHEMA_PATH=/etc/loc-qrs/schema.json
VOLUME /data

EXPOSE 8080 8081
ENTRYPOINT ["server"]
```

```bash
docker build -t loc-qrs:latest .

docker run -d \
  -p 8080:8080 \
  -p 8081:8081 \
  -v /host/data:/data \
  -v /host/schema.json:/etc/loc-qrs/schema.json \
  -e LOG_LEVEL=info \
  loc-qrs:latest
```

### Environment Variables Reference

Quick reference for production deployments:

```bash
# Required
DATA_DIR=/data                    # Persistent volume mount
SCHEMA_PATH=/etc/loc-qrs/schema.json

# Tuning
CHANNEL_CAPACITY=50000            # Increase for high ingest rates
SYNC_INTERVAL=10s                 # Decrease for lower query latency
SYNC_RECORD_COUNT=5000            # Increase for fewer sync operations
SHUTDOWN_TIMEOUT=60s              # Increase for large final syncs

# Observability
LOG_LEVEL=info                    # debug|info|warn|error

# Network
HTTP_ADDR=:8080
MCP_ADDR=:8081
```

---

## Troubleshooting

### Query returns 0 rows after ingest

**Cause:** The `bufio.Writer` (64 KB) has not been flushed to disk, so DuckDB cannot read the data.

**Fix:** Call `POST /api/v1/sync` before querying, or wait for the automatic sync interval.

```bash
curl -X POST http://localhost:8080/api/v1/sync
curl -X POST http://localhost:8080/api/v1/query -d '{"sql":"SELECT COUNT(*) FROM records"}'
```

### Schema version mismatch on startup

**Cause:** `schema.json` has changed since the last run.

**Fix:** Run with `--rebuild` to re-sync all existing JSONL to Parquet under the new schema.

```bash
./bin/server --rebuild
```

### Address already in use

**Cause:** Another process is bound to port 8080 or 8081.

**Fix:**

```bash
lsof -i :8080    # Find the process
kill <PID>
# Or change the address:
HTTP_ADDR=:9090 ./bin/server
```

### CGO build errors

**Cause:** CGO is disabled, or no C compiler is available.

**Fix:**

```bash
# macOS
xcode-select --install

# Linux (Debian/Ubuntu)
apt-get install -y gcc

# Build
CGO_ENABLED=1 go build ./cmd/server
```

### DuckDB "Catalog Error" in queries

**Cause:** The query references a column that does not exist in the inferred schema of the JSONL file (e.g. all records have `null` for a field, and DuckDB infers a different type).

**Fix:** Ensure records include the field with a non-null value in at least one row, or cast explicitly in SQL:

```sql
SELECT CAST(metadata AS VARCHAR) FROM records
```

### Queries show stale data after sync

**Cause:** The queryDB is an in-memory DuckDB instance. It reads files fresh on every query, but DuckDB may have internal caching for Parquet metadata.

**Fix:** This should not occur in normal operation. If it does, restarting the server resets the in-memory queryDB state.

### 503 on ingest — channel full

**Cause:** The write goroutine is falling behind ingest rate and the 10 000-capacity channel is full.

**Fix:**

1. Check `ingestion_channel_depth` metric — if consistently >80%, increase `CHANNEL_CAPACITY`
2. Check `parquet_sync_duration_seconds` — if syncs are slow (>1s), check disk I/O
3. Reduce `SYNC_RECORD_COUNT` to trigger more frequent syncs and drain the channel faster

```bash
# Check channel depth
curl -s http://localhost:8080/metrics | grep ingestion_channel_depth
```

---

## License

MIT License — see [LICENSE](LICENSE).

---

## Dependencies

| Package | Version | License | Purpose |
|---------|---------|---------|---------|
| `github.com/go-chi/chi/v5` | v5.1.0 | MIT | HTTP router with middleware support |
| `github.com/google/uuid` | v1.6.0 | BSD-3 | Request ID generation |
| `github.com/marcboeker/go-duckdb` | v1.8.5 | MIT | Embedded DuckDB (CGO, bundles static library) |
| `github.com/mark3labs/mcp-go` | v0.18.0 | MIT | MCP server implementation (stdio + SSE) |
| `github.com/prometheus/client_golang` | v1.20.0 | Apache-2.0 | Prometheus metrics |
| `github.com/stretchr/testify` | v1.10.0 | MIT | Test assertions |
| `golang.org/x/sync` | v0.8.0 | BSD-3 | `errgroup` for goroutine lifecycle |
