# AGENTS.md — local-aurora-data-api

<!--toc:start-->

- [Project Overview](#project-overview)
- [Repository Layout](#repository-layout)
- [Implemented API Endpoints](#implemented-api-endpoints)
- [Configuration (Environment Variables)](#configuration-environment-variables)
- [Running Locally](#running-locally)
- [Build & Test](#build-test)
- [Key Design Decisions](#key-design-decisions)
  - [`Querier` Interface](#querier-interface)
  - [Named Parameter Rewriting (`params.go`)](#named-parameter-rewriting-paramsgo)
  - [TypeHints](#typehints)
  - [Result Formats](#result-formats)
  - [Error Responses](#error-responses)
  - [Batch Auto-Commit](#batch-auto-commit)
- [Known Limitations](#known-limitations)
- [Adding New Features](#adding-new-features)
  <!--toc:end-->

## Project Overview

A local mock of the **AWS Aurora RDS Data API V2** (`rds-data`), written in Go.
It exposes the same HTTP endpoints as the real AWS service so that local
development and CI environments can target a plain PostgreSQL instance without
changing application code.

**Module:** `github.com/reap-interactive/local-aurora-data-api`  
**Go version:** 1.23  
**Key dependency:** `github.com/jackc/pgx/v5` (PostgreSQL driver / pool)

---

## Repository Layout

```
cmd/server/main.go          # Binary entry-point; reads env, wires DB + HTTP server
internal/api/
  server.go                 # http.ServeMux route registration (Go 1.22 method+path patterns)
  handlers.go               # Handler methods: one per Data API operation
  middleware.go             # handleJSON[T] — generic decode / encode / error wrapper
  errors.go                 # APIError, ErrorCode constants, writeJSON / writeError helpers
internal/dataapi/
  models.go                 # All request/response structs and the Field union type
  executor.go               # Execute() and BatchExecute() — core query logic
  params.go                 # ParseNamedParams — :name → $N rewriter
  types.go                  # PG OID → type name, scan-dest helpers, AWS field conversion
  transactions.go           # In-memory TransactionStore (Begin / Get / Commit / Rollback)
  db.go                     # OpenPostgresDB — pgxpool connection helper
```

---

## Implemented API Endpoints

| Route                       | Operation             | Notes                                       |
| --------------------------- | --------------------- | ------------------------------------------- |
| `POST /Execute`             | ExecuteStatement      | Single SQL statement, optional named params |
| `POST /BatchExecute`        | BatchExecuteStatement | One SQL statement × N parameter sets        |
| `POST /BeginTransaction`    | BeginTransaction      | Returns a `transactionId`                   |
| `POST /CommitTransaction`   | CommitTransaction     | Commits by `transactionId`                  |
| `POST /RollbackTransaction` | RollbackTransaction   | Rolls back by `transactionId`               |
| `POST /ExecuteSql`          | ExecuteSql            | **Not implemented** — returns 404           |

`resourceArn` and `secretArn` fields in request bodies are accepted but **not validated** — any non-empty string is fine for local use.

---

## Configuration (Environment Variables)

| Variable            | Default                 | Purpose                                     |
| ------------------- | ----------------------- | ------------------------------------------- |
| `PORT`              | `8080`                  | HTTP listen port                            |
| `POSTGRES_HOST`     | `127.0.0.1`             | PostgreSQL hostname                         |
| `POSTGRES_PORT`     | `5432`                  | PostgreSQL port                             |
| `POSTGRES_USER`     | `postgres`              | PostgreSQL username                         |
| `POSTGRES_PASSWORD` | `example`               | PostgreSQL password                         |
| `POSTGRES_DB`       | `postgres`              | PostgreSQL database name                    |
| `RESOURCE_ARN`      | _(docker-compose only)_ | Passed through; not validated by the server |
| `SECRET_ARN`        | _(docker-compose only)_ | Passed through; not validated by the server |

---

## Running Locally

**With Docker Compose (recommended):**

```bash
docker compose up --build
```

Starts `api` on `localhost:8080` and a `postgres:16-alpine` sidecar.

**Without Docker (requires a running PostgreSQL):**

```bash
POSTGRES_HOST=localhost POSTGRES_PASSWORD=mysecret go run ./cmd/server
```

---

## Build & Test

```bash
# Build the binary
go build -o rds-data-api-local ./cmd/server

# Run all tests (unit + integration)
go test ./...

# Run only unit tests (no DB required)
go test ./internal/dataapi/...

# Run integration tests (requires a reachable PostgreSQL)
go test ./internal/api/... -v
```

Integration tests in `internal/api/integration_test.go` connect to the real
DB specified by the same env vars as above. They are skipped automatically
when the DB is unreachable.

---

## Key Design Decisions

### `Querier` Interface

`executor.go` defines a `Querier` interface (`Query` method only) implemented
by both `*pgxpool.Pool` and `pgx.Tx`. This lets `Execute` / `BatchExecute`
work identically in auto-commit mode and inside an explicit transaction — no
code duplication.

### Named Parameter Rewriting (`params.go`)

The AWS Data API uses `:name` style parameters. `ParseNamedParams` rewrites
them to PostgreSQL's `$N` positional syntax. The parser correctly skips:

- Single-quoted string literals (`'...'`)
- Double-quoted identifiers (`"..."`)
- Line comments (`--`)
- Block comments (`/* */`)
- Dollar-quoted strings (`$tag$...$tag$`)
- PostgreSQL cast operators (`::`)

Duplicate `:name` references reuse the same `$N` index. An error is returned
if a `:name` token has no matching entry in the params map.

### TypeHints

`SQLParameter.TypeHint` values are applied during param map construction:

- `"DATE"` → parsed to `time.Time` (format `2006-01-02`)
- `"TIMESTAMP"` → parsed to `time.Time` (several formats attempted)
- All other hints pass the string through unchanged.

### Result Formats

`ExecuteStatementRequest.FormatRecordsAs`:

- **`"JSON"`** → `FormattedRecords` field contains a JSON array of
  `map[string]any` objects keyed by column name.
- **Default** → `Records` field contains `[][]Field` using the AWS typed
  `Field` union (longValue, stringValue, booleanValue, doubleValue,
  blobValue, isNull).

### Error Responses

All errors are returned as AWS-compatible JSON:

```json
{ "message": "...", "code": "BadRequestException" }
```

Error codes: `BadRequestException` (400), `NotFoundException` (404),
`InternalServerErrorException` (500).

### Batch Auto-Commit

When `BatchExecute` is called **without** a `transactionId`, the server wraps
the entire batch in a single transaction internally (all rows inserted or none).

---

## Known Limitations

- `ExecuteSql` (the legacy V1 operation) always returns 404.
- `ArrayValue` parameters are not supported and return an error if passed.
- `resourceArn` / `secretArn` are not authenticated or validated.
- The `database` and `schema` fields on requests are accepted but ignored —
  the connected PostgreSQL database is always used.
- `ContinueAfterTimeout` is accepted but has no effect.
- `ResultSetOptions` (decimalReturnType, longReturnType) are accepted but
  have no effect; longs are always returned as `longValue`.

---

## Adding New Features

1. **New request/response types** → add structs to `internal/dataapi/models.go`.
2. **New SQL execution behaviour** → add logic to `internal/dataapi/executor.go`.
3. **New HTTP endpoint** → add a handler method to `internal/api/handlers.go`,
   register the route in `internal/api/server.go`.
4. **New PostgreSQL type mapping** → extend `oidTypeName` and `makeScanDest` /
   `destToField` in `internal/dataapi/types.go`.
5. **Tests** → unit tests sit alongside source files (`*_test.go`); integration
   tests live in `internal/api/integration_test.go`.
