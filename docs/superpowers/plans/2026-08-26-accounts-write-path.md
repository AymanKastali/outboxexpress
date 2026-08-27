# Plan 1 — Accounts Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `POST /users` commits the `accounts.users` row and its `accounts.outbox` row in one PostgreSQL transaction, with the outbox insert produced structurally by a unit of work rather than written by the use case.

**Architecture:** One Go module, two bounded contexts, four layers per context (§6.1 of the spec). This plan builds the `accounts` context's producing end only: domain aggregate, use case, CloudEvents envelope factory, pgx repositories, the unit of work that drains aggregate events during commit, and the `api` process that drives it. No Kafka client is added, no relay runs, and outbox rows simply accumulate as `pending` — which is exactly the state the pattern's guarantee rests on and therefore something the tests can assert on its own.

**Tech Stack:** Go 1.27.0, PostgreSQL 18.6, `pgx/v5`, `google/uuid` (UUIDv7), `pressly/goose/v3`, `golang.org/x/sync/errgroup`, `testcontainers-go` (test-only), stdlib `net/http`, `log/slog`, `flag`, `encoding/json`.

**Spec:** `docs/superpowers/specs/2026-08-26-transactional-outbox-demo-design.md` — read §5, §6, §8.1, §9.1, §11.1 and §14 before starting. The plan argues from the spec; where the two disagree, the spec wins and the plan is wrong.

---

## How the spec is decomposed into plans

The spec describes four processes. Building them in one pass would mean no working software until the end. Five plans, each of which produces something you can run and assert on:

| Plan | Deliverable | Spec sections |
|---|---|---|
| **1 — Accounts write path (this plan)** | `POST /users` writes user + outbox row atomically. Rows accumulate as `pending`; nothing publishes them yet. | §5, §6, §8.1, §8.3, §9.1, §11.1, §13.4, §14, §16 |
| 2 — The relay | `relay` process: claim with `SKIP LOCKED`, produce to Kafka, mark published, backoff, dead-letter, `LISTEN`/`NOTIFY`, purge, the per-pass log line. Kafka enters the compose file. | §7, §8, §9.2, §10.1, §11.2, §12.1, §12.4, §13.1–§13.3 |
| 3 — The consuming end | `notifier`: inbox row + notification + send-intent in one transaction, offset committed after. Retry ladder and DLT. | §8.2, §11.3, §10.2, §12.2 |
| 4 — The external side effect | `sender`: consumer-side outbox drives the email gateway with `Idempotency-Key`. | §11.4, §10.3, §13.2 |
| 5 — Proof and operations | Chaos endpoints, replay, the end-to-end invariant suite, README with the guided failure demos. | §12.3, §12.5, §13.5, §13.6, §15, §18 |

Each plan ends with the test suite green and the `make` targets it introduced working. Later plans do not go back and restructure earlier ones; the ports in this plan are drawn so they do not have to.

## Global Constraints

Copied from the spec. Every task's requirements implicitly include these.

- **Go 1.27.0.** `go.mod` declares `go 1.27.0`; `GOTOOLCHAIN=auto` fetches it when the local toolchain is older.
- **PostgreSQL 18.6**, image `postgres:18.6`. One container, two databases: `oe_accounts` and `oe_notifications`.
- **Module path:** `github.com/AymanKastali/outboxexpress`.
- **The dependency rule (§6.1).** `domain` imports stdlib and `github.com/google/uuid` only. `application` imports `domain` and `internal/platform/messaging` only. `presentation` and `infrastructure` are **siblings**: neither imports the other. Task 14 turns this into a test; do not wait for Task 14 to obey it.
- **No observation inside a use case (§6.1).** Use cases return result structs; the presentation layer logs them. No logger is passed into the application layer, and there is no metrics system anywhere in this project (spec §13.3) — every signal spec §18 asks for is a field on a structured log line.
- **Nothing external inside a database transaction (§11.1 rule 2).** No HTTP, no broker call, no email inside `UnitOfWork.Do`.
- **`event_id` is minted once, at insert (§8.1).** Never regenerated per attempt.
- **`payload` holds the complete serialised CloudEvents envelope (§9.1).** The relay is a dumb pipe and never parses it.
- **No ORM, no web framework, no CLI framework, no configuration library (§5 D12, §14).**
- **Errors wrap with `%w`.** Domain errors are sentinel values in `domain`; the HTTP layer maps them to status codes and never leaks a driver error to a client.
- **No test-only code in non-test files.** A fake, stub, deterministic generator or fixture belongs in a `_test.go` file, in the package whose tests use it — never exported from a production package "so tests can reach it". The sole exception is `internal/platform/pgtest`, and only because a container helper is imported by *other* packages' tests, which a `_test.go` file cannot be; its own doc comment says so.
- **Every task ends with a commit, on `main`.** Conventional commit prefixes: `feat:`, `test:`, `chore:`, `docs:`, `refactor:`. Do **not** create a branch and do not open a pull request — the repository owner works directly on `main` for this project. Each task's commit is the review unit.

---

## File Structure

Every file this plan creates, and the one thing it is responsible for. Paths are relative to the repository root. Test files sit beside the code they test, Go-style; integration tests carry `//go:build integration` and are skipped by `make test`.

**Module and tooling**

| File | Responsibility |
|---|---|
| `go.mod`, `go.sum` | Module path, Go version, dependency set |
| `Makefile` | `build test test-integration lint fmt db-up db-down migrate run-api` |
| `deploy/docker-compose.yml` | PostgreSQL 18.6 only, in this plan |
| `deploy/postgres-init.sh` | Creates `oe_accounts` and `oe_notifications` at first boot |
| `.env.example` | Every variable of §14 this plan reads, with demo values |
| `README.md` | How to run the write path and what it proves |

**Schema**

| File | Responsibility |
|---|---|
| `migrations/embed.go` | `//go:embed` of both contexts' SQL; `FS()` and `Latest()` |
| `migrations/accounts/0001_init.sql` | §8.1 DDL verbatim |
| `migrations/notifications/0001_init.sql` | §8.2 DDL verbatim |
| `cmd/migrate/main.go` | `-context` / `-action` flags, goose over `database/sql` |

**Platform (technical shared code, not a fifth layer)**

| File | Responsibility |
|---|---|
| `internal/platform/config/config.go` | Env parsing and validation, one struct per process |
| `internal/platform/logging/logging.go` | `*slog.Logger` from a level string |
| `internal/platform/clock/clock.go` | `System` clock; `Fixed` for tests |
| `internal/platform/ids/ids.go` | UUIDv7 generator |
| `internal/platform/postgres/pool.go` | Pool construction and readiness probing |
| `internal/platform/pgtest/pgtest.go` | Test-only: throwaway PostgreSQL container, migrated |
| `internal/platform/messaging/cloudevents.go` | The CloudEvents 1.0 wire format, shared by every context |
| `internal/platform/admin/admin.go` | `/healthz` and `/readyz` router |

**Accounts context**

| File | Responsibility |
|---|---|
| `internal/accounts/domain/user.go` | `User` aggregate, `Register`, `PullEvents` |
| `internal/accounts/domain/events.go` | `Event`, `EventSource`, `UserRegistered` |
| `internal/accounts/domain/errors.go` | `ErrInvalidEmail`, `ErrInvalidDisplayName`, `ErrEmailTaken` |
| `internal/accounts/domain/repository.go` | `UserRepository` interface |
| `internal/accounts/application/ports.go` | `Clock`, `IDGen`, `Wakeup`, `UnitOfWork`, `Work`, `Metadata`, `Envelope`, `EnvelopeFactory`, `OutboxAppender` |
| `internal/accounts/application/register_user.go` | The `RegisterUser` use case |
| `internal/accounts/application/envelope.go` | Domain event → CloudEvents envelope |
| `internal/accounts/infrastructure/postgres/outbox_repo.go` | Batched `INSERT` into `accounts.outbox` |
| `internal/accounts/infrastructure/postgres/user_repo.go` | `INSERT` into `accounts.users`, unique-violation mapping, event tracking |
| `internal/accounts/infrastructure/postgres/tracker.go` | Which aggregates this transaction touched |
| `internal/accounts/infrastructure/postgres/uow.go` | The transaction boundary; drains events, appends outbox rows, commits |
| `internal/accounts/infrastructure/wakeup/notify.go` | `pg_notify('outbox_new', '')`, best effort |
| `internal/accounts/presentation/http/dto.go` | Request/response bodies, the only place JSON tags for the API live |
| `internal/accounts/presentation/http/handler.go` | `POST /users`; maps domain errors to status codes |
| `internal/accounts/presentation/http/router.go` | Public mux |
| `cmd/api/main.go` | Wiring, two listeners, graceful shutdown |
| `internal/arch/arch_test.go` | The dependency rule as a test |

`internal/accounts/infrastructure/postgres` holds four small files rather than one `postgres.go` because the unit of work is the piece a reader comes for and it should not be buried in a file that also does row scanning.

---

## Task 1: Module, tooling and a PostgreSQL you can talk to

**Files:**
- Create: `go.mod`
- Create: `Makefile`
- Create: `deploy/docker-compose.yml`
- Create: `deploy/postgres-init.sh`
- Create: `.env.example`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces: module path `github.com/AymanKastali/outboxexpress`; `make db-up` / `make db-down`; a PostgreSQL 18.6 instance on `localhost:5432` with databases `oe_accounts` and `oe_notifications`, user `oe`, password `oe`.

- [ ] **Step 1: Verify the toolchain can reach Go 1.27.0**

```bash
go version
GOTOOLCHAIN=auto go version
```

Expected: the first prints the locally installed version (1.26.5 is fine); the second prints `go1.27.0` or newer, downloading the toolchain if needed. If the second fails, stop and report it — this plan pins Go 1.27.0 per the spec and there is no supported workaround.

- [ ] **Step 2: Initialise the module**

```bash
go mod init github.com/AymanKastali/outboxexpress
go mod edit -go=1.27.0
```

- [ ] **Step 3: Write the Makefile**

`.RECIPEPREFIX` is set to `>` so recipes do not depend on literal tabs surviving an edit.

```makefile
.RECIPEPREFIX := >
.DEFAULT_GOAL := build

export GOTOOLCHAIN := auto

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: build test test-integration lint fmt db-up db-down db-logs migrate run-api

build:
> go build ./...

test:
> go test ./...

test-integration:
> go test -tags=integration -count=1 -timeout=10m ./...

lint:
> go vet ./...
> test -z "$$(gofmt -l . )" || (gofmt -l . && echo "gofmt: files need formatting" && exit 1)

fmt:
> gofmt -w .

db-up:
> $(COMPOSE) up -d --wait postgres

db-down:
> $(COMPOSE) down -v

db-logs:
> $(COMPOSE) logs -f postgres
```

`.PHONY` already lists `migrate` and `run-api`; Tasks 4 and 13 add their recipes, which is when they first have something to run.

- [ ] **Step 4: Write the compose file**

```yaml
services:
  postgres:
    image: postgres:18.6
    container_name: oe-postgres
    environment:
      POSTGRES_USER: oe
      POSTGRES_PASSWORD: oe
      POSTGRES_DB: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./postgres-init.sh:/docker-entrypoint-initdb.d/10-databases.sh:ro
      # postgres:18+ stores data in a major-version subdirectory of
      # /var/lib/postgresql so that pg_upgrade --link can work across a single
      # mount point. Mounting at /var/lib/postgresql/data — correct through 17 —
      # makes the entrypoint refuse to boot.
      - pgdata:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U oe -d oe_accounts"]
      interval: 2s
      timeout: 3s
      retries: 30
    command:
      # A demo, not a benchmark. Louder logging is worth more here than throughput.
      - postgres
      - -c
      - log_statement=none
      - -c
      - log_min_duration_statement=200ms

volumes:
  pgdata:
```

- [ ] **Step 5: Write the database init script**

```bash
#!/bin/sh
# Runs once, on first boot of an empty data directory. POSTGRES_DB created
# `postgres`; the two service databases are created here so that no service can
# reach the other's tables even by accident (spec D3).
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    CREATE DATABASE oe_accounts;
    CREATE DATABASE oe_notifications;
SQL
```

Then: `chmod +x deploy/postgres-init.sh`

- [ ] **Step 6: Write `.env.example`**

```dotenv
# Accounts context (spec §14)
ACCOUNTS_DATABASE_URL=postgres://oe:oe@localhost:5432/oe_accounts?sslmode=disable
NOTIFICATIONS_DATABASE_URL=postgres://oe:oe@localhost:5432/oe_notifications?sslmode=disable

# api process
HTTP_ADDR=:8080
ADMIN_ADDR=127.0.0.1:8081
LOG_LEVEL=info
```

- [ ] **Step 7: Extend `.gitignore`**

Append:

```gitignore
# Local environment
.env
```

(The file already ignores `/bin/`, `*.exe`, `coverage.*` and `.env.*` with a `!.env.example` exception — do not duplicate those lines, and confirm `.env` itself is covered before adding it.)

- [ ] **Step 8: Verify the database comes up with both databases**

```bash
make db-up
docker compose -f deploy/docker-compose.yml exec -T postgres \
  psql -U oe -d postgres -tAc "SELECT datname FROM pg_database WHERE datname LIKE 'oe_%' ORDER BY 1"
```

Expected, exactly:

```
oe_accounts
oe_notifications
```

- [ ] **Step 9: Verify the module builds and lints**

```bash
make build
```

Expected: success with no output beyond `go: downloading` lines and a `matched no packages` warning — `go build ./...` on a module with no packages is a no-op success.

Do **not** run `make lint` yet. `go vet ./...` exits 1 with `no packages to vet` on a module that has none, so the target cannot pass until Task 2 creates the first package. It is checked from Task 2 onward.

- [ ] **Step 10: Commit**

```bash
git add go.mod Makefile deploy .env.example .gitignore
git commit -m "chore: module, Makefile and PostgreSQL 18.6 with both service databases"
```

---

## Task 2: Configuration

**Files:**
- Create: `internal/platform/config/config.go`
- Test: `internal/platform/config/config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type API struct { AccountsDatabaseURL, HTTPAddr, AdminAddr string; LogLevel slog.Level }`
  - `type Migrate struct { AccountsDatabaseURL, NotificationsDatabaseURL string; LogLevel slog.Level }`
  - `func LoadAPI(getenv func(string) string) (API, error)`
  - `func LoadMigrate(getenv func(string) string) (Migrate, error)`
  - `var ErrMissing = errors.New("config: required variable is empty")`
  - `var ErrInvalid = errors.New("config: invalid value")`

`getenv` is a parameter rather than a call to `os.Getenv` so the tests need no process-global mutation and can run in parallel.

- [ ] **Step 1: Write the failing tests**

```go
package config

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadAPI_Defaults(t *testing.T) {
	got, err := LoadAPI(env(map[string]string{
		"ACCOUNTS_DATABASE_URL": "postgres://oe:oe@localhost:5432/oe_accounts",
	}))
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if got.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", got.HTTPAddr, ":8080")
	}
	if got.AdminAddr != "127.0.0.1:8081" {
		t.Errorf("AdminAddr = %q, want %q", got.AdminAddr, "127.0.0.1:8081")
	}
	if got.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want %v", got.LogLevel, slog.LevelInfo)
	}
}

func TestLoadAPI_Overrides(t *testing.T) {
	got, err := LoadAPI(env(map[string]string{
		"ACCOUNTS_DATABASE_URL": "postgres://x/y",
		"HTTP_ADDR":             ":9090",
		"ADMIN_ADDR":            "127.0.0.1:9091",
		"LOG_LEVEL":             "debug",
	}))
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if got.HTTPAddr != ":9090" || got.AdminAddr != "127.0.0.1:9091" || got.LogLevel != slog.LevelDebug {
		t.Errorf("overrides not applied: %+v", got)
	}
}

func TestLoadAPI_Errors(t *testing.T) {
	tests := []struct {
		name string
		envs map[string]string
		want error
	}{
		{
			name: "missing database url",
			envs: map[string]string{},
			want: ErrMissing,
		},
		{
			name: "blank database url",
			envs: map[string]string{"ACCOUNTS_DATABASE_URL": "   "},
			want: ErrMissing,
		},
		{
			name: "unknown log level",
			envs: map[string]string{"ACCOUNTS_DATABASE_URL": "postgres://x/y", "LOG_LEVEL": "chatty"},
			want: ErrInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadAPI(env(tc.envs))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestLoadAPI_ErrorNamesTheVariable(t *testing.T) {
	_, err := LoadAPI(env(map[string]string{}))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "ACCOUNTS_DATABASE_URL") {
		t.Fatalf("error %q does not name the offending variable", err)
	}
}

func TestLoadMigrate_RequiresBothURLs(t *testing.T) {
	_, err := LoadMigrate(env(map[string]string{"ACCOUNTS_DATABASE_URL": "postgres://x/y"}))
	if !errors.Is(err, ErrMissing) {
		t.Fatalf("err = %v, want ErrMissing", err)
	}
	got, err := LoadMigrate(env(map[string]string{
		"ACCOUNTS_DATABASE_URL":      "postgres://x/a",
		"NOTIFICATIONS_DATABASE_URL": "postgres://x/n",
	}))
	if err != nil {
		t.Fatalf("LoadMigrate: %v", err)
	}
	if got.NotificationsDatabaseURL != "postgres://x/n" {
		t.Errorf("NotificationsDatabaseURL = %q", got.NotificationsDatabaseURL)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/platform/config/`
Expected: FAIL — `undefined: LoadAPI`, `undefined: ErrMissing`, and so on.

- [ ] **Step 3: Write the implementation**

```go
// Package config parses process configuration from the environment, once, at
// startup. A process that starts with invalid configuration is a process that
// fails later and further from the cause, so every Load function refuses to
// return a partially valid struct.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

var (
	ErrMissing = errors.New("config: required variable is empty")
	ErrInvalid = errors.New("config: invalid value")
)

// API is the configuration of the accounts HTTP process (spec §14).
type API struct {
	AccountsDatabaseURL string
	HTTPAddr            string
	AdminAddr           string
	LogLevel            slog.Level
}

// Migrate is the configuration of the schema migration command.
type Migrate struct {
	AccountsDatabaseURL      string
	NotificationsDatabaseURL string
	LogLevel                 slog.Level
}

func LoadAPI(getenv func(string) string) (API, error) {
	level, err := logLevel(getenv)
	if err != nil {
		return API{}, err
	}
	cfg := API{
		AccountsDatabaseURL: value(getenv, "ACCOUNTS_DATABASE_URL", ""),
		HTTPAddr:            value(getenv, "HTTP_ADDR", ":8080"),
		AdminAddr:           value(getenv, "ADMIN_ADDR", "127.0.0.1:8081"),
		LogLevel:            level,
	}
	if err := required("ACCOUNTS_DATABASE_URL", cfg.AccountsDatabaseURL); err != nil {
		return API{}, err
	}
	return cfg, nil
}

func LoadMigrate(getenv func(string) string) (Migrate, error) {
	level, err := logLevel(getenv)
	if err != nil {
		return Migrate{}, err
	}
	cfg := Migrate{
		AccountsDatabaseURL:      value(getenv, "ACCOUNTS_DATABASE_URL", ""),
		NotificationsDatabaseURL: value(getenv, "NOTIFICATIONS_DATABASE_URL", ""),
		LogLevel:                 level,
	}
	if err := required("ACCOUNTS_DATABASE_URL", cfg.AccountsDatabaseURL); err != nil {
		return Migrate{}, err
	}
	if err := required("NOTIFICATIONS_DATABASE_URL", cfg.NotificationsDatabaseURL); err != nil {
		return Migrate{}, err
	}
	return cfg, nil
}

func value(getenv func(string) string, key, fallback string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return fallback
}

// required takes an already-trimmed value from value(), so a second TrimSpace
// here could never change the answer.
func required(key, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s", ErrMissing, key)
	}
	return nil
}

// logLevel parses LOG_LEVEL with slog's own parser. The set of valid level
// names is a fact the standard library owns; repeating it here would be a
// second source of truth that can drift, and slog's version accepts offsets
// like "info+2" for free. Returning a slog.Level rather than a string means the
// value is parsed once, at startup, and cannot fail again later.
func logLevel(getenv func(string) string) (slog.Level, error) {
	raw := value(getenv, "LOG_LEVEL", "info")
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("%w: LOG_LEVEL=%q: %w", ErrInvalid, raw, err)
	}
	return level, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/platform/config/ -v`
Expected: PASS for all five test functions.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/config
git commit -m "feat(platform): environment configuration with validation at startup"
```

---

## Task 3: Clock, identifiers and logging

**Files:**
- Create: `internal/platform/clock/clock.go`
- Create: `internal/platform/ids/ids.go`
- Create: `internal/platform/logging/logging.go`
- Test: `internal/platform/ids/ids_test.go`
- Test: `internal/platform/clock/clock_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `clock.System{}` with `Now() time.Time` (UTC)
  - `ids.UUIDv7{}` with `New() (uuid.UUID, error)`
  - `logging.New(level slog.Level) *slog.Logger`

These satisfy the `application.Clock` and `application.IDGen` ports of Task 6 structurally; nothing here imports the application layer.

Three production types and nothing else. A test that needs a frozen clock or a
predictable id declares a two-line fake in its own `_test.go` — that is cheaper
than a shared helper, and it keeps the shipped binary free of code that exists
only for tests.

- [ ] **Step 1: Add the uuid dependency**

```bash
go get github.com/google/uuid@latest
```

- [ ] **Step 2: Write the failing tests**

`internal/platform/ids/ids_test.go`:

```go
package ids

import "testing"

func TestUUIDv7_IsVersion7AndMonotonicallyOrdered(t *testing.T) {
	g := UUIDv7{}
	prev, err := g.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := prev.Version(); got != 7 {
		t.Fatalf("version = %d, want 7", got)
	}
	for i := 0; i < 100; i++ {
		next, err := g.New()
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if next.String() <= prev.String() {
			t.Fatalf("uuid %s not sorted after %s", next, prev)
		}
		prev = next
	}
}
```

`internal/platform/clock/clock_test.go`:

```go
package clock

import (
	"testing"
	"time"
)

func TestSystem_ReturnsUTC(t *testing.T) {
	got := System{}.Now()
	if got.Location() != time.UTC {
		t.Fatalf("location = %v, want UTC", got.Location())
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/platform/ids/ ./internal/platform/clock/`
Expected: FAIL — `undefined: UUIDv7`, `undefined: System`.

- [ ] **Step 4: Write the implementations**

`internal/platform/clock/clock.go`:

```go
// Package clock exists so that no code outside it calls time.Now. A use case
// that reads the wall clock directly cannot be tested without sleeping, and a
// test suite that sleeps cannot prove an invariant (spec §15).
package clock

import "time"

// System is the real clock, and the only clock this package ships. It returns
// UTC because every timestamp in this system is stored and published in UTC, and
// converting once at the source is cheaper than remembering to convert at each
// use.
//
// A test that needs time to stand still declares its own two-line fake next to
// the test that needs it. Shipping a controllable clock from here would put code
// in the binary that only tests can reach.
type System struct{}

func (System) Now() time.Time { return time.Now().UTC() }
```

`internal/platform/ids/ids.go`:

```go
// Package ids generates identifiers. UUIDv7 is used everywhere an identifier is
// stored, because its leading timestamp gives B-tree index locality: v4 keys
// scatter inserts across the whole index, and the outbox is an insert-heavy
// table (spec §4, D10).
package ids

import "github.com/google/uuid"

// UUIDv7 is the generator, and the only one this package ships. A test that
// needs predictable identifiers declares its own generator in its own
// _test.go — see the seqIDs type in Task 7 and staticIDs in Task 12.
type UUIDv7 struct{}

// NewV7 already returns uuid.Nil alongside its error, so there is nothing to
// translate — this method exists only to name the port.
func (UUIDv7) New() (uuid.UUID, error) { return uuid.NewV7() }
```

`internal/platform/logging/logging.go`:

```go
// Package logging builds the one logger a process uses. JSON, because these
// logs are read by a machine first and a human second, and structured logs are
// how an outbox incident gets diagnosed from a row id.
package logging

import (
	"log/slog"
	"os"
)

// New takes an already-parsed level, so it has nothing to validate and no error
// to return. config.LoadAPI does the parsing, once, with slog's own parser.
func New(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/platform/... -v`
Expected: PASS. `TestUUIDv7_IsVersion7AndMonotonicallyOrdered` proves the ordering property the outbox depends on.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/platform/clock internal/platform/ids internal/platform/logging
git commit -m "feat(platform): injectable clock, UUIDv7 identifiers and JSON logging"
```

---

## Task 4: Schema, the migrate command, and a throwaway database for tests

**Files:**
- Create: `internal/platform/postgres/queryer.go`
- Create: `migrations/embed.go`
- Create: `migrations/accounts/0001_init.sql`
- Create: `migrations/notifications/0001_init.sql`
- Create: `cmd/migrate/main.go`
- Create: `internal/platform/pgtest/pgtest.go`
- Test: `migrations/migrations_test.go` (unit, no container)
- Test: `migrations/schema_integration_test.go` (`//go:build integration`)
- Modify: `Makefile` (add the `migrate` target)

`platform/postgres` is split across two tasks because `migrations.Ready` takes a
`Queryer` and therefore cannot wait for Task 8. `queryer.go` — the interface and
the package doc — is written here; `pool.go`, with `NewPool` and `WithTx`, is
written in Task 8, which is where the first caller of either appears.

**Interfaces:**
- Consumes: `config.LoadMigrate`, `logging.New` (Tasks 2, 3).
- Produces:
  - `migrations.Context` — `type Context string`, with `migrations.Accounts` and `migrations.Notifications`
  - `func migrations.Parse(name string) (Context, error)`
  - `func migrations.Apply(db *sql.DB, c Context) error`
  - `func migrations.Latest(c Context) (int64, error)`
  - `func migrations.Version(db *sql.DB) (int64, error)`
  - `func migrations.Ready(ctx context.Context, q platformpg.Queryer, expected int64) error`
  - `var migrations.ErrUnknownContext, migrations.ErrSchemaBehind error`
  - `func pgtest.Accounts(t *testing.T) (dsn string, pool *pgxpool.Pool)` — a migrated, empty `oe_accounts`
  - `func pgtest.Notifications(t *testing.T) (dsn string, pool *pgxpool.Pool)` — the same for `oe_notifications`
  - `func pgtest.Truncate(t *testing.T, pool *pgxpool.Pool, tables ...string)`
  - `func pgtest.TruncateAccounts(t *testing.T, pool *pgxpool.Pool)`
  - `func pgtest.CountAccounts(t *testing.T, pool *pgxpool.Pool) (users, outbox int)`

`migrations.Ready` lives in this package, not in `platform/postgres`, because the
name of goose's bookkeeping table and the meaning of its rows are goose
knowledge. Keeping it here means there is one place that knows how to ask "what
schema is applied", instead of three places that each write the query.

- [ ] **Step 1: Add the dependencies**

```bash
go get github.com/jackc/pgx/v5@latest
go get github.com/pressly/goose/v3@latest
go get github.com/testcontainers/testcontainers-go@latest
go get github.com/testcontainers/testcontainers-go/modules/postgres@latest
```

- [ ] **Step 2: Write the accounts DDL**

`migrations/accounts/0001_init.sql` — the schema of spec §8.1, unchanged, wrapped in goose annotations:

```sql
-- +goose Up
CREATE SCHEMA accounts;

CREATE TABLE accounts.users (
    id            UUID        PRIMARY KEY,
    email         TEXT        NOT NULL UNIQUE,
    display_name  TEXT        NOT NULL,
    version       INTEGER     NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts.outbox (
    -- identity and ordering
    id              BIGSERIAL     PRIMARY KEY,
    event_id        UUID          NOT NULL UNIQUE,

    -- routing
    aggregate_type  TEXT          NOT NULL,
    aggregate_id    TEXT          NOT NULL,
    event_type      TEXT          NOT NULL,
    schema_version  INTEGER       NOT NULL DEFAULT 1,

    -- content
    payload         JSONB         NOT NULL,
    headers         JSONB         NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- relay bookkeeping
    status          TEXT          NOT NULL DEFAULT 'pending',
    attempts        INTEGER       NOT NULL DEFAULT 0,
    available_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      TEXT,
    published_at    TIMESTAMPTZ,

    CONSTRAINT outbox_status_check
        CHECK (status IN ('pending', 'published', 'failed'))
);

-- The relay's only access path: pending rows, in id order, that are due.
CREATE INDEX outbox_pending_idx
    ON accounts.outbox (available_at, id)
    WHERE status = 'pending';

-- +goose Down
DROP TABLE accounts.outbox;
DROP TABLE accounts.users;
DROP SCHEMA accounts;
```

- [ ] **Step 3: Write the notifications DDL**

`migrations/notifications/0001_init.sql` — the schema of spec §8.2, unchanged. The consuming context has no code until Plan 3; its schema is written now because §8 defines both and because Task 4 is the only place that ever needs to learn goose.

```sql
-- +goose Up
CREATE SCHEMA notifications;

CREATE TABLE notifications.inbox (
    consumer      TEXT        NOT NULL,
    event_id      UUID        NOT NULL,
    event_type    TEXT        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE INDEX inbox_processed_at_idx ON notifications.inbox (processed_at);

CREATE TABLE notifications.notifications (
    id          UUID        PRIMARY KEY,
    user_id     UUID        NOT NULL,
    recipient   TEXT        NOT NULL,
    kind        TEXT        NOT NULL,
    state       TEXT        NOT NULL DEFAULT 'pending',
    source_event_id UUID    NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ,

    CONSTRAINT notification_state_check
        CHECK (state IN ('pending', 'sent', 'failed'))
);

-- The consumer's own outbox: durable intents for the external call (spec §13).
CREATE TABLE notifications.outbox (
    id              BIGSERIAL     PRIMARY KEY,
    event_id        UUID          NOT NULL UNIQUE,
    aggregate_type  TEXT          NOT NULL,
    aggregate_id    TEXT          NOT NULL,
    event_type      TEXT          NOT NULL,
    schema_version  INTEGER       NOT NULL DEFAULT 1,
    payload         JSONB         NOT NULL,
    headers         JSONB         NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    idempotency_key TEXT          NOT NULL,
    status          TEXT          NOT NULL DEFAULT 'pending',
    attempts        INTEGER       NOT NULL DEFAULT 0,
    available_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      TEXT,
    published_at    TIMESTAMPTZ,

    CONSTRAINT notif_outbox_status_check
        CHECK (status IN ('pending', 'published', 'failed'))
);

CREATE INDEX notif_outbox_pending_idx
    ON notifications.outbox (available_at, id)
    WHERE status = 'pending';

-- +goose Down
DROP TABLE notifications.outbox;
DROP TABLE notifications.notifications;
DROP TABLE notifications.inbox;
DROP SCHEMA notifications;
```

- [ ] **Step 4: Write the failing unit test for the embed layer**

`migrations/migrations_test.go`:

```go
package migrations

import (
	"errors"
	"io/fs"
	"testing"
)

func TestLatest(t *testing.T) {
	for _, c := range []Context{Accounts, Notifications} {
		got, err := Latest(c)
		if err != nil {
			t.Fatalf("Latest(%s): %v", c, err)
		}
		if got != 1 {
			t.Errorf("Latest(%s) = %d, want 1", c, got)
		}
	}
}

func TestParse(t *testing.T) {
	got, err := Parse("accounts")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got != Accounts {
		t.Errorf("Parse = %q, want %q", got, Accounts)
	}
	if _, err := Parse("payments"); !errors.Is(err, ErrUnknownContext) {
		t.Fatalf("err = %v, want ErrUnknownContext", err)
	}
}

func TestLatest_UnknownContext(t *testing.T) {
	if _, err := Latest(Context("payments")); !errors.Is(err, ErrUnknownContext) {
		t.Fatalf("err = %v, want ErrUnknownContext", err)
	}
}

func TestFS_ContainsOnlyThatContextsMigrations(t *testing.T) {
	f, err := FS(Accounts)
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	names, err := fs.Glob(f, "*.sql")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(names) != 1 || names[0] != "0001_init.sql" {
		t.Fatalf("names = %v, want [0001_init.sql]", names)
	}
}
```

- [ ] **Step 5: Run it to verify it fails**

Run: `go test ./migrations/`
Expected: FAIL — `undefined: Latest`, `undefined: Accounts`.

- [ ] **Step 6: Write the embed layer**

`migrations/embed.go`:

```go
// Package migrations holds the SQL schema of both bounded contexts and applies
// it. Each context has its own directory and its own goose_db_version table in
// its own database, so the two schema histories are independent (spec §8.3).
//
// Migration is a separate step, never something a service does at startup: with
// four processes and any replica count, startup migration is a race.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/pressly/goose/v3"

	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

//go:embed accounts/*.sql notifications/*.sql
var files embed.FS

// Context names a bounded context, which here means one database.
type Context string

const (
	Accounts      Context = "accounts"
	Notifications Context = "notifications"
)

var (
	ErrUnknownContext = errors.New("migrations: unknown context")

	// ErrSchemaBehind means the database is at an older migration than the
	// binary expects. A process in that state reports itself unready rather than
	// running against a schema it does not understand (spec §8.3, §13.4).
	ErrSchemaBehind = errors.New("migrations: schema is behind the expected version")
)

// Contexts lists every context in a stable order, for commands that act on all
// of them.
func Contexts() []Context { return []Context{Accounts, Notifications} }

// Parse turns a command-line value into a Context. It is exported so that
// cmd/migrate validates the flag with this package's own rule rather than
// repeating the membership check.
func Parse(name string) (Context, error) {
	c := Context(name)
	if !slices.Contains(Contexts(), c) {
		return "", fmt.Errorf("%w: %q", ErrUnknownContext, name)
	}
	return c, nil
}

// FS returns the migrations of one context, rooted so that goose sees the .sql
// files at ".".
func FS(c Context) (fs.FS, error) {
	if _, err := Parse(string(c)); err != nil {
		return nil, err
	}
	return fs.Sub(files, string(c))
}

// Latest is the highest migration version embedded for a context. It is derived
// from the filenames rather than declared in a constant, because a constant is
// something a future migration forgets to bump.
//
// The version is parsed by goose.NumericComponent — the same parser that will
// run these files — so this package cannot disagree with goose about what a
// valid migration filename is. fs.Glob returns lexically sorted names, and the
// prefixes are zero-padded, so the newest is last.
func Latest(c Context) (int64, error) {
	f, err := FS(c)
	if err != nil {
		return 0, err
	}
	names, err := fs.Glob(f, "*.sql")
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		return 0, fmt.Errorf("migrations: no .sql files embedded for %s", c)
	}
	slices.Sort(names)
	newest := names[len(names)-1]
	v, err := goose.NumericComponent(newest)
	if err != nil {
		return 0, fmt.Errorf("migrations: %s: %w", newest, err)
	}
	return v, nil
}

// Apply brings one context's database up to Latest.
func Apply(db *sql.DB, c Context) error {
	f, err := FS(c)
	if err != nil {
		return err
	}
	goose.SetBaseFS(f)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("migrations: dialect: %w", err)
	}
	if err := goose.Up(db, "."); err != nil {
		return fmt.Errorf("migrations: up %s: %w", c, err)
	}
	return nil
}

// Version is the currently applied version of the database behind db.
func Version(db *sql.DB) (int64, error) {
	if err := goose.SetDialect("postgres"); err != nil {
		return 0, fmt.Errorf("migrations: dialect: %w", err)
	}
	v, err := goose.GetDBVersion(db)
	if err != nil {
		return 0, fmt.Errorf("migrations: version: %w", err)
	}
	return v, nil
}

// Ready reports whether the database behind q has at least the expected
// migration applied. It takes a Queryer because its caller is a readiness probe
// holding a pgx pool, not a *sql.DB, which is why it cannot simply call
// goose.GetDBVersion.
//
// max(version_id) and goose.GetDBVersion disagree after a down migration —
// goose honours is_applied, this does not. That is acceptable for "is the schema
// at least as new as my binary expects" and acceptable for nothing else.
func Ready(ctx context.Context, q platformpg.Queryer, expected int64) error {
	query := fmt.Sprintf(`SELECT coalesce(max(version_id), 0) FROM %s`, goose.TableName())
	var applied int64
	if err := q.QueryRow(ctx, query).Scan(&applied); err != nil {
		return fmt.Errorf("migrations: read schema version: %w", err)
	}
	if applied < expected {
		return fmt.Errorf("%w: applied %d, expected %d", ErrSchemaBehind, applied, expected)
	}
	return nil
}
```

- [ ] **Step 7: Run the unit test to verify it passes**

Run: `go test ./migrations/ -v`
Expected: PASS for all four tests.

- [ ] **Step 8: Write the migrate command**

`cmd/migrate/main.go`:

```go
// Command migrate applies the schema of one bounded context, or of all of them.
// It is a separate binary because no service may migrate at startup (spec §8.3).
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/AymanKastali/outboxexpress/internal/platform/config"
	"github.com/AymanKastali/outboxexpress/internal/platform/logging"
	"github.com/AymanKastali/outboxexpress/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	contextFlag := flag.String("context", "all", "bounded context: accounts, notifications or all")
	action := flag.String("action", "up", "action: up or version")
	flag.Parse()

	cfg, err := config.LoadMigrate(os.Getenv)
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	targets, err := selected(*contextFlag)
	if err != nil {
		return err
	}

	for _, c := range targets {
		dsn, err := dsnFor(cfg, c)
		if err != nil {
			return err
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return fmt.Errorf("open %s: %w", c, err)
		}
		err = act(db, c, *action, log)
		closeErr := db.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("close %s: %w", c, closeErr)
		}
	}
	return nil
}

func act(db *sql.DB, c migrations.Context, action string, log *slog.Logger) error {
	switch action {
	case "up":
		if err := migrations.Apply(db, c); err != nil {
			return err
		}
		v, err := migrations.Version(db)
		if err != nil {
			return err
		}
		log.Info("migrated", "context", string(c), "version", v)
		return nil
	case "version":
		v, err := migrations.Version(db)
		if err != nil {
			return err
		}
		latest, err := migrations.Latest(c)
		if err != nil {
			return err
		}
		log.Info("version", "context", string(c), "applied", v, "latest", latest)
		return nil
	default:
		return fmt.Errorf("unknown action %q (want up or version)", action)
	}
}

func selected(name string) ([]migrations.Context, error) {
	if name == "all" {
		return migrations.Contexts(), nil
	}
	c, err := migrations.Parse(name)
	if err != nil {
		return nil, err
	}
	return []migrations.Context{c}, nil
}

func dsnFor(cfg config.Migrate, c migrations.Context) (string, error) {
	switch c {
	case migrations.Accounts:
		return cfg.AccountsDatabaseURL, nil
	case migrations.Notifications:
		return cfg.NotificationsDatabaseURL, nil
	default:
		return "", fmt.Errorf("%w: %q", migrations.ErrUnknownContext, c)
	}
}
```

- [ ] **Step 9: Add the Makefile recipe**

```makefile
migrate:
> go run ./cmd/migrate -context all -action up
```

- [ ] **Step 10: Write the test container helper**

`internal/platform/pgtest/pgtest.go`:

```go
// Package pgtest gives an integration test a real PostgreSQL 18.6, migrated and
// empty.
//
// One container per test binary, holding both databases — the same shape
// deploy/docker-compose.yml deploys. A container per test would add a boot and a
// goose run to every one of them, and would buy no isolation that TRUNCATE does
// not already give.
//
// This file carries no build tag on purpose. A package whose every file is
// excluded by a constraint makes `go build ./...` fail with "build constraints
// exclude all Go files", so the tag goes on the tests that use pgtest, never on
// pgtest itself. Importing "testing" outside a test file is the price; nothing
// in cmd/ imports this package, and the architecture test would say so if it did.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/AymanKastali/outboxexpress/migrations"
)

const image = "postgres:18.6"

// The container is started once and never terminated explicitly: testcontainers'
// reaper removes it when the test binary exits, which is the only moment at
// which terminating it is safe.
var (
	instance = sync.OnceValues(startInstance)

	accountsDB = sync.OnceValues(func() (database, error) {
		return open(migrations.Accounts, "oe_accounts")
	})
	notificationsDB = sync.OnceValues(func() (database, error) {
		return open(migrations.Notifications, "oe_notifications")
	})
)

type database struct {
	dsn  string
	pool *pgxpool.Pool
}

// Accounts returns a DSN and pool for a migrated, empty oe_accounts.
func Accounts(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	d, err := accountsDB()
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	TruncateAccounts(t, d.pool)
	return d.dsn, d.pool
}

// Notifications is the same for oe_notifications.
func Notifications(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	d, err := notificationsDB()
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	Truncate(t, d.pool,
		"notifications.outbox", "notifications.notifications", "notifications.inbox")
	return d.dsn, d.pool
}

// TruncateAccounts empties the accounts tables, resetting the outbox sequence so
// that row ids are predictable per test.
func TruncateAccounts(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	Truncate(t, pool, "accounts.outbox", "accounts.users")
}

// Truncate empties the named tables in one statement. The table list is a
// parameter because it is the only thing that varies between contexts.
func Truncate(t *testing.T, pool *pgxpool.Pool, tables ...string) {
	t.Helper()
	if len(tables) == 0 {
		return
	}
	stmt := "TRUNCATE " + strings.Join(tables, ", ") + " RESTART IDENTITY"
	if _, err := pool.Exec(context.Background(), stmt); err != nil {
		t.Fatalf("pgtest: truncate: %v", err)
	}
}

// CountAccounts is the assertion every write-path test starts from: how many
// users, and how many outbox rows.
func CountAccounts(t *testing.T, pool *pgxpool.Pool) (users, outbox int) {
	t.Helper()
	ctx := context.Background()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts.users`).Scan(&users); err != nil {
		t.Fatalf("pgtest: count users: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts.outbox`).Scan(&outbox); err != nil {
		t.Fatalf("pgtest: count outbox: %v", err)
	}
	return users, outbox
}

func startInstance() (*tcpostgres.PostgresContainer, error) {
	ctx := context.Background()

	// BasicWaitStrategies is the module's own strategy: the "ready to accept
	// connections" log twice (initdb restarts the server) *and* a listening-port
	// check. Hand-writing only the log wait replaces this rather than extending
	// it, and the module documents that dropping the port check is what makes
	// these tests flaky on macOS and Windows.
	ctr, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("oe_accounts"),
		tcpostgres.WithUsername("oe"),
		tcpostgres.WithPassword("oe"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("pgtest: start %s: %w", image, err)
	}

	// The second database, exactly as deploy/postgres-init.sh creates it.
	adminDSN, err := dsnFor(ctx, ctr, "oe_accounts")
	if err != nil {
		return nil, err
	}
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return nil, fmt.Errorf("pgtest: open admin connection: %w", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE oe_notifications`); err != nil {
		return nil, fmt.Errorf("pgtest: create oe_notifications: %w", err)
	}
	return ctr, nil
}

func open(c migrations.Context, database_ string) (database, error) {
	ctr, err := instance()
	if err != nil {
		return database{}, err
	}
	ctx := context.Background()

	dsn, err := dsnFor(ctx, ctr, database_)
	if err != nil {
		return database{}, err
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return database{}, fmt.Errorf("pgtest: open %s: %w", database_, err)
	}
	err = migrations.Apply(db, c)
	closeErr := db.Close()
	if err != nil {
		return database{}, fmt.Errorf("pgtest: migrate %s: %w", database_, err)
	}
	if closeErr != nil {
		return database{}, fmt.Errorf("pgtest: close migration connection: %w", closeErr)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return database{}, fmt.Errorf("pgtest: pool for %s: %w", database_, err)
	}
	return database{dsn: dsn, pool: pool}, nil
}

func dsnFor(ctx context.Context, ctr *tcpostgres.PostgresContainer, database_ string) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("pgtest: host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", fmt.Errorf("pgtest: mapped port: %w", err)
	}
	return fmt.Sprintf("postgres://oe:oe@%s:%s/%s?sslmode=disable",
		host, port.Port(), database_), nil
}
```

- [ ] **Step 11: Write the failing schema integration test**

`migrations/schema_integration_test.go`:

```go
//go:build integration

package migrations_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
	"github.com/AymanKastali/outboxexpress/migrations"
)

func TestAccountsSchema(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	t.Run("tables exist", func(t *testing.T) {
		for _, table := range []string{"users", "outbox"} {
			var exists bool
			err := pool.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				                 WHERE table_schema = 'accounts' AND table_name = $1)`,
				table).Scan(&exists)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if !exists {
				t.Errorf("accounts.%s missing", table)
			}
		}
	})

	t.Run("partial index on pending rows", func(t *testing.T) {
		var def string
		err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes
			  WHERE schemaname = 'accounts' AND indexname = 'outbox_pending_idx'`).Scan(&def)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		for _, want := range []string{"available_at", "id", "WHERE"} {
			if !strings.Contains(def, want) {
				t.Errorf("indexdef %q missing %q", def, want)
			}
		}
	})

	t.Run("status is constrained", func(t *testing.T) {
		_, err := pool.Exec(ctx,
			`INSERT INTO accounts.outbox
			   (event_id, aggregate_type, aggregate_id, event_type, payload, status)
			 VALUES (gen_random_uuid(), 'User', 'x', 'y', '{}', 'nonsense')`)
		if err == nil {
			t.Fatal("expected outbox_status_check to reject status 'nonsense'")
		}
	})

	t.Run("Ready agrees with the embedded latest", func(t *testing.T) {
		latest, err := migrations.Latest(migrations.Accounts)
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if err := migrations.Ready(ctx, pool, latest); err != nil {
			t.Fatalf("Ready(%d) = %v, want nil", latest, err)
		}
		if err := migrations.Ready(ctx, pool, latest+1); !errors.Is(err, migrations.ErrSchemaBehind) {
			t.Fatalf("Ready(%d) = %v, want ErrSchemaBehind", latest+1, err)
		}
	})
}

func TestNotificationsSchema(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	ctx := context.Background()

	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
		                 WHERE table_schema = 'notifications' AND table_name = 'inbox')`).Scan(&exists)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !exists {
		t.Error("notifications.inbox missing")
	}
}
```

- [ ] **Step 12: Run the integration test to verify it fails, then passes**

Run: `go test -tags=integration -count=1 -run 'TestAccountsSchema|TestNotificationsSchema' ./migrations/ -v`

Expected on the first run, before Steps 6–10 are complete: FAIL with a build or migration error. With everything in place: PASS, taking roughly 20–40 seconds while Docker pulls and starts `postgres:18.6`.

If Docker is not available the test fails at `pgtest.Accounts`; that is correct behaviour. Integration tests are not skipped silently — a suite that skips its only proof of the schema is a suite that reports success for an empty database.

- [ ] **Step 13: Verify the command works against the compose database**

```bash
make db-up
set -a && . ./.env.example && set +a
go run ./cmd/migrate -context all -action up
go run ./cmd/migrate -context accounts -action version
```

Expected: `{"level":"INFO","msg":"migrated","context":"accounts","version":1}` then the same for `notifications`, then a `version` line showing `applied=1 latest=1`.

- [ ] **Step 14: Commit**

```bash
git add go.mod go.sum migrations cmd/migrate internal/platform/pgtest internal/platform/postgres Makefile
git commit -m "feat(migrations): accounts and notifications schema, migrate command, test harness"
```

---

## Task 5: The accounts domain

**Files:**
- Create: `internal/accounts/domain/events.go`
- Create: `internal/accounts/domain/errors.go`
- Create: `internal/accounts/domain/user.go`
- Create: `internal/accounts/domain/repository.go`
- Test: `internal/accounts/domain/user_test.go`

**Interfaces:**
- Consumes: stdlib and `github.com/google/uuid` only. This package must not import anything else, ever.
- Produces:
  - `type Event interface { EventType() string; AggregateType() string; AggregateID() string; SchemaVersion() int; OccurredAt() time.Time }`
  - `type EventSource interface { PullEvents() []Event }`
  - `type UserRegistered struct { UserID uuid.UUID; Email, DisplayName string; Version int; RegisteredAt time.Time }`
  - `const AggregateTypeUser = "User"`, `const EventTypeUserRegistered = "com.outboxexpress.accounts.user.registered"`
  - `type User struct { ID uuid.UUID; Email, DisplayName string; Version int; CreatedAt time.Time }` (plus an unexported `events` field)
  - `func Register(id uuid.UUID, email, displayName string, now time.Time) (*User, error)`
  - `func (u *User) PullEvents() []Event`
  - `type UserRepository interface { Insert(ctx context.Context, u *User) error }`
  - `var ErrInvalidEmail, ErrInvalidDisplayName, ErrInvalidID, ErrEmailTaken error`

- [ ] **Step 1: Write the failing tests**

`internal/accounts/domain/user_test.go`:

```go
package domain

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testID  = uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1")
	testNow = time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC)
)

func TestRegister_NormalisesEmailAndTrimsName(t *testing.T) {
	u, err := Register(testID, "  ADA@Example.COM ", "  Ada Lovelace  ", testNow)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if u.Email != "ada@example.com" {
		t.Errorf("Email = %q, want %q", u.Email, "ada@example.com")
	}
	if u.DisplayName != "Ada Lovelace" {
		t.Errorf("DisplayName = %q, want %q", u.DisplayName, "Ada Lovelace")
	}
	if u.Version != 1 {
		t.Errorf("Version = %d, want 1", u.Version)
	}
	if !u.CreatedAt.Equal(testNow) {
		t.Errorf("CreatedAt = %v, want %v", u.CreatedAt, testNow)
	}
}

func TestRegister_EmitsExactlyOneEvent(t *testing.T) {
	u, err := Register(testID, "ada@example.com", "Ada Lovelace", testNow)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	events := u.PullEvents()
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	got, ok := events[0].(UserRegistered)
	if !ok {
		t.Fatalf("event type = %T, want UserRegistered", events[0])
	}
	want := UserRegistered{
		UserID:       testID,
		Email:        "ada@example.com",
		DisplayName:  "Ada Lovelace",
		Version:      1,
		RegisteredAt: testNow,
	}
	if got != want {
		t.Errorf("event = %+v, want %+v", got, want)
	}
}

func TestUserRegistered_RoutingFields(t *testing.T) {
	e := UserRegistered{UserID: testID, RegisteredAt: testNow}
	if e.EventType() != "com.outboxexpress.accounts.user.registered" {
		t.Errorf("EventType = %q", e.EventType())
	}
	if e.AggregateType() != "User" {
		t.Errorf("AggregateType = %q", e.AggregateType())
	}
	if e.AggregateID() != testID.String() {
		t.Errorf("AggregateID = %q, want %q", e.AggregateID(), testID)
	}
	if e.SchemaVersion() != 1 {
		t.Errorf("SchemaVersion = %d, want 1", e.SchemaVersion())
	}
	if !e.OccurredAt().Equal(testNow) {
		t.Errorf("OccurredAt = %v, want %v", e.OccurredAt(), testNow)
	}
}

func TestPullEvents_DrainsOnce(t *testing.T) {
	u, err := Register(testID, "ada@example.com", "Ada Lovelace", testNow)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := len(u.PullEvents()); got != 1 {
		t.Fatalf("first pull = %d events, want 1", got)
	}
	if got := len(u.PullEvents()); got != 0 {
		t.Fatalf("second pull = %d events, want 0 — a drained aggregate must not "+
			"re-emit, or one commit would append the same outbox row twice", got)
	}
}

func TestRegister_Rejects(t *testing.T) {
	tests := []struct {
		name        string
		id          uuid.UUID
		email       string
		displayName string
		want        error
	}{
		{"empty email", testID, "", "Ada", ErrInvalidEmail},
		{"blank email", testID, "   ", "Ada", ErrInvalidEmail},
		{"no at sign", testID, "ada.example.com", "Ada", ErrInvalidEmail},
		{"no domain", testID, "ada@", "Ada", ErrInvalidEmail},
		{"no local part", testID, "@example.com", "Ada", ErrInvalidEmail},
		{"display name form", testID, "Ada <ada@example.com>", "Ada", ErrInvalidEmail},
		{"two addresses", testID, "a@b.com, c@d.com", "Ada", ErrInvalidEmail},
		{"empty display name", testID, "ada@example.com", "", ErrInvalidDisplayName},
		{"blank display name", testID, "ada@example.com", "   ", ErrInvalidDisplayName},
		{"nil id", uuid.Nil, "ada@example.com", "Ada", ErrInvalidID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Register(tc.id, tc.email, tc.displayName, testNow)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if u != nil {
				t.Error("a rejected registration must not return a user")
			}
		})
	}
}

func TestRegister_RejectsOverlongInput(t *testing.T) {
	long := strings.Repeat("a", 400)
	if _, err := Register(testID, long+"@example.com", "Ada", testNow); !errors.Is(err, ErrInvalidEmail) {
		t.Errorf("overlong email: err = %v, want ErrInvalidEmail", err)
	}
	if _, err := Register(testID, "ada@example.com", long, testNow); !errors.Is(err, ErrInvalidDisplayName) {
		t.Errorf("overlong display name: err = %v, want ErrInvalidDisplayName", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/accounts/domain/`
Expected: FAIL — `undefined: Register`, `undefined: UserRegistered`.

- [ ] **Step 3: Write the domain errors**

`internal/accounts/domain/errors.go`:

```go
package domain

import "errors"

// The domain's vocabulary of refusal. These are sentinels so that every outer
// layer can map them — the HTTP layer to a status code, a test to an assertion —
// without matching on message text.
var (
	ErrInvalidEmail       = errors.New("accounts: email is not a valid address")
	ErrInvalidDisplayName = errors.New("accounts: display name is empty or too long")
	ErrInvalidID          = errors.New("accounts: user id must not be the nil UUID")

	// ErrEmailTaken is a domain rule the database enforces: the uniqueness of an
	// email is a fact about the whole collection of users, which no in-memory
	// check can establish under concurrency. The repository translates the
	// unique-violation into this error; the rule still belongs to the domain.
	ErrEmailTaken = errors.New("accounts: email is already registered")
)
```

- [ ] **Step 4: Write the events**

`internal/accounts/domain/events.go`:

```go
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Event is what an aggregate emits when its state changes. Routing fields are
// methods rather than payload data because the relay routes on them and must
// never parse business content (spec §8.1).
type Event interface {
	EventType() string
	AggregateType() string
	AggregateID() string
	SchemaVersion() int
	OccurredAt() time.Time
}

// EventSource is anything the unit of work can drain during a commit. Keeping
// this in the domain is what lets the persistence layer collect events without
// the domain knowing an outbox exists (spec §5, §6.4).
type EventSource interface {
	PullEvents() []Event
}

const (
	// AggregateTypeUser selects the topic at publish time.
	AggregateTypeUser = "User"

	// EventTypeUserRegistered is reverse-DNS and versionless: a breaking change
	// becomes a new schema_version or a new type, never a redefinition of this
	// one (spec §9.3).
	EventTypeUserRegistered = "com.outboxexpress.accounts.user.registered"
)

// UserRegistered carries the state a consumer needs as of the moment it
// occurred, plus the aggregate version, so that no consumer has to call back
// into accounts (event-carried state transfer, spec §9.1).
type UserRegistered struct {
	UserID       uuid.UUID
	Email        string
	DisplayName  string
	Version      int
	RegisteredAt time.Time
}

func (e UserRegistered) EventType() string     { return EventTypeUserRegistered }
func (e UserRegistered) AggregateType() string { return AggregateTypeUser }
func (e UserRegistered) AggregateID() string   { return e.UserID.String() }
func (e UserRegistered) SchemaVersion() int    { return 1 }
func (e UserRegistered) OccurredAt() time.Time { return e.RegisteredAt }
```

- [ ] **Step 5: Write the aggregate**

`internal/accounts/domain/user.go`:

```go
package domain

import (
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	maxEmailLength       = 320 // RFC 3696 local part + domain
	maxDisplayNameLength = 200
)

// User is the aggregate. Its invariants are checked in one place — the
// constructor — so no code path can produce a User that violates them.
type User struct {
	ID          uuid.UUID
	Email       string
	DisplayName string
	Version     int
	CreatedAt   time.Time

	events []Event
}

// Register creates a user and records the fact that it happened. It performs no
// I/O: the identifier and the current time arrive as values so that this
// function is deterministic and the layer above owns both sources of
// nondeterminism.
func Register(id uuid.UUID, email, displayName string, now time.Time) (*User, error) {
	if id == uuid.Nil {
		return nil, ErrInvalidID
	}
	addr, err := normaliseEmail(email)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(displayName)
	if name == "" || len(name) > maxDisplayNameLength {
		return nil, ErrInvalidDisplayName
	}

	u := &User{
		ID:          id,
		Email:       addr,
		DisplayName: name,
		Version:     1,
		CreatedAt:   now.UTC(),
	}
	u.record(UserRegistered{
		UserID:       u.ID,
		Email:        u.Email,
		DisplayName:  u.DisplayName,
		Version:      u.Version,
		RegisteredAt: u.CreatedAt,
	})
	return u, nil
}

// PullEvents returns the recorded events and forgets them. Draining is what
// makes a second commit of the same aggregate emit nothing: an aggregate that
// re-emitted would turn one state change into two published events.
func (u *User) PullEvents() []Event {
	events := u.events
	u.events = nil
	return events
}

func (u *User) record(e Event) { u.events = append(u.events, e) }

var _ EventSource = (*User)(nil)

// normaliseEmail lowercases and trims, then requires a bare address. The
// display-name form ("Ada <ada@example.com>") parses but is not an address, and
// storing it would make the email column mean two different things.
func normaliseEmail(raw string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" || len(s) > maxEmailLength {
		return "", ErrInvalidEmail
	}
	parsed, err := mail.ParseAddress(s)
	if err != nil {
		return "", ErrInvalidEmail
	}
	if parsed.Name != "" || parsed.Address != s {
		return "", ErrInvalidEmail
	}
	// ParseAddress has already established that there is an "@"; Cut says so in
	// the code rather than relying on an index that would panic if it had not.
	_, domainPart, ok := strings.Cut(s, "@")
	if !ok || !strings.Contains(domainPart, ".") {
		return "", ErrInvalidEmail
	}
	return s, nil
}
```

- [ ] **Step 6: Write the repository interface**

`internal/accounts/domain/repository.go`:

```go
package domain

import "context"

// UserRepository is declared in the domain because in the Onion/DDD vocabulary
// the brief names, a Repository is a domain concept: it is the collection of
// users, spoken about the way a domain expert would (spec D5). An outbox is not,
// which is why the outbox port lives in the application layer instead.
//
// Insert returns ErrEmailTaken when the address is already registered.
type UserRepository interface {
	Insert(ctx context.Context, u *User) error
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./internal/accounts/domain/ -v`
Expected: PASS, including every subtest of `TestRegister_Rejects`.

- [ ] **Step 8: Verify the domain imports nothing it should not**

A package is external exactly when the first element of its import path
contains a dot — the same rule Task 14's architecture test uses. `vendor/...`
and `crypto/internal/...` paths are the standard library's own vendored trees,
which `net/mail` pulls in, so they are excluded by name rather than by the dot
test:

```bash
go list -deps ./internal/accounts/domain \
  | awk -F/ '$1 ~ /\./ && $1 != "vendor"' \
  | grep -v '^github.com/AymanKastali/outboxexpress/'
```

Expected: exactly one line, `github.com/google/uuid`. Anything else is a dependency-rule violation to fix now rather than in Task 14.

- [ ] **Step 9: Commit**

```bash
git add internal/accounts/domain
git commit -m "feat(accounts): User aggregate, UserRegistered event and domain invariants"
```

---

## Task 6: Application ports and the RegisterUser use case

**Files:**
- Create: `internal/accounts/application/ports.go`
- Create: `internal/accounts/application/register_user.go`
- Test: `internal/accounts/application/register_user_test.go`
- Test: `internal/accounts/application/fakes_test.go`

**Interfaces:**
- Consumes: `domain.Register`, `domain.User`, `domain.UserRepository`, `domain.ErrEmailTaken` (Task 5).
- Produces:
  - `type Clock interface { Now() time.Time }`
  - `type IDGen interface { New() (uuid.UUID, error) }`
  - `type Wakeup interface { Notify(ctx context.Context) }`
  - `type Metadata struct { CorrelationID, Traceparent string }`
  - `type Work struct { Users domain.UserRepository }`
  - `type UnitOfWork interface { Do(ctx context.Context, meta Metadata, fn func(Work) error) error }`
  - `type Envelope struct { EventID uuid.UUID; AggregateType, AggregateID, EventType string; SchemaVersion int; Payload []byte; Headers map[string]string; OccurredAt time.Time }`
  - `type EnvelopeFactory interface { From(events []domain.Event, meta Metadata) ([]Envelope, error) }`
  - `type OutboxAppender interface { Append(ctx context.Context, envelopes []Envelope) error }`
  - `type RegisterUserCommand struct { Email, DisplayName string; Meta Metadata }`
  - `type RegisterUserResult struct { UserID uuid.UUID }`
  - `func NewRegisterUser(uow UnitOfWork, clock Clock, ids IDGen, wakeup Wakeup) *RegisterUser`
  - `func (uc *RegisterUser) Execute(ctx context.Context, cmd RegisterUserCommand) (RegisterUserResult, error)`

`Work` is a struct of domain interfaces rather than an interface with getters: the infrastructure constructs it with transaction-bound repositories, and the use case reads `w.Users` as a field. A later plan adds fields for the notifications context's own `Work`; this one has a single repository and does not pretend otherwise.

- [ ] **Step 1: Write the fakes**

`internal/accounts/application/fakes_test.go`:

```go
package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

// fakeUOW runs fn immediately and records what happened. It models the two
// outcomes the use case can distinguish: fn failed, or the transaction
// committed.
type fakeUOW struct {
	calls     int
	committed bool
	lastMeta  Metadata
	repo      *fakeUserRepo
	commitErr error
}

func (f *fakeUOW) Do(ctx context.Context, meta Metadata, fn func(Work) error) error {
	f.calls++
	f.lastMeta = meta
	if err := fn(Work{Users: f.repo}); err != nil {
		return err
	}
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = true
	return nil
}

type fakeUserRepo struct {
	inserted []*domain.User
	err      error
}

func (f *fakeUserRepo) Insert(ctx context.Context, u *domain.User) error {
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, u)
	return nil
}

type fakeWakeup struct{ calls int }

func (f *fakeWakeup) Notify(ctx context.Context) { f.calls++ }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fixedIDs struct {
	next uuid.UUID
	err  error
}

func (f fixedIDs) New() (uuid.UUID, error) {
	if f.err != nil {
		return uuid.Nil, f.err
	}
	return f.next, nil
}

var errIDs = errors.New("no entropy")
```

- [ ] **Step 2: Write the failing use-case tests**

`internal/accounts/application/register_user_test.go`:

```go
package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

var (
	ucID  = uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1")
	ucNow = time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC)
)

func newUseCase(repo *fakeUserRepo) (*RegisterUser, *fakeUOW, *fakeWakeup) {
	uow := &fakeUOW{repo: repo}
	wake := &fakeWakeup{}
	uc := NewRegisterUser(uow, fixedClock{now: ucNow}, fixedIDs{next: ucID}, wake)
	return uc, uow, wake
}

func TestRegisterUser_CommitsThenWakesTheRelay(t *testing.T) {
	repo := &fakeUserRepo{}
	uc, uow, wake := newUseCase(repo)

	got, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
		Meta:        Metadata{CorrelationID: "corr-1", Traceparent: "tp-1"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.UserID != ucID {
		t.Errorf("UserID = %s, want %s", got.UserID, ucID)
	}
	if uow.calls != 1 {
		t.Errorf("uow.calls = %d, want 1", uow.calls)
	}
	if !uow.committed {
		t.Error("transaction did not commit")
	}
	if uow.lastMeta.CorrelationID != "corr-1" || uow.lastMeta.Traceparent != "tp-1" {
		t.Errorf("metadata not passed to the unit of work: %+v", uow.lastMeta)
	}
	if len(repo.inserted) != 1 {
		t.Fatalf("inserted %d users, want 1", len(repo.inserted))
	}
	if repo.inserted[0].Email != "ada@example.com" {
		t.Errorf("Email = %q", repo.inserted[0].Email)
	}
	if !repo.inserted[0].CreatedAt.Equal(ucNow) {
		t.Errorf("CreatedAt = %v, want the injected clock's time %v", repo.inserted[0].CreatedAt, ucNow)
	}
	if wake.calls != 1 {
		t.Errorf("wakeup.calls = %d, want 1", wake.calls)
	}
}

func TestRegisterUser_DoesNotTouchTheDatabaseOnInvalidInput(t *testing.T) {
	repo := &fakeUserRepo{}
	uc, uow, wake := newUseCase(repo)

	_, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "not-an-address",
		DisplayName: "Ada Lovelace",
	})
	if !errors.Is(err, domain.ErrInvalidEmail) {
		t.Fatalf("err = %v, want ErrInvalidEmail", err)
	}
	if uow.calls != 0 {
		t.Errorf("uow.calls = %d, want 0 — validation happens before the transaction opens", uow.calls)
	}
	if wake.calls != 0 {
		t.Errorf("wakeup.calls = %d, want 0", wake.calls)
	}
}

func TestRegisterUser_SurfacesEmailTaken(t *testing.T) {
	repo := &fakeUserRepo{err: domain.ErrEmailTaken}
	uc, uow, wake := newUseCase(repo)

	_, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
	})
	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("err = %v, want ErrEmailTaken", err)
	}
	if uow.committed {
		t.Error("the transaction must not commit when the insert fails")
	}
	if wake.calls != 0 {
		t.Error("no event was written, so there is nothing to wake the relay for")
	}
}

func TestRegisterUser_DoesNotWakeTheRelayWhenTheCommitFails(t *testing.T) {
	repo := &fakeUserRepo{}
	uow := &fakeUOW{repo: repo, commitErr: errors.New("connection reset")}
	wake := &fakeWakeup{}
	uc := NewRegisterUser(uow, fixedClock{now: ucNow}, fixedIDs{next: ucID}, wake)

	if _, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
	}); err == nil {
		t.Fatal("expected the commit error to surface")
	}
	if wake.calls != 0 {
		t.Error("waking the relay for an uncommitted transaction advertises an event that does not exist")
	}
}

func TestRegisterUser_FailsBeforeAnyWorkWhenIDGenerationFails(t *testing.T) {
	repo := &fakeUserRepo{}
	uow := &fakeUOW{repo: repo}
	uc := NewRegisterUser(uow, fixedClock{now: ucNow}, fixedIDs{err: errIDs}, &fakeWakeup{})

	if _, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
	}); !errors.Is(err, errIDs) {
		t.Fatalf("err = %v, want errIDs", err)
	}
	if uow.calls != 0 {
		t.Errorf("uow.calls = %d, want 0", uow.calls)
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/accounts/application/`
Expected: FAIL — `undefined: NewRegisterUser`, `undefined: Metadata`.

- [ ] **Step 4: Write the ports**

`internal/accounts/application/ports.go`:

```go
// Package application holds the accounts use cases and the technical ports they
// need. It imports the domain and nothing concrete: no SQL, no HTTP, no Kafka,
// no logger (spec §6.1).
package application

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

// Clock and IDGen exist so that a use case is deterministic under test. Reading
// the wall clock or generating a UUID is I/O against the machine.
type Clock interface {
	Now() time.Time
}

type IDGen interface {
	New() (uuid.UUID, error)
}

// Wakeup shortens the relay's polling latency. It has no error return by
// design: this is an optimisation and never the delivery path, so there is no
// failure a caller could sensibly handle (spec §11.1 rule 3, §13.1).
type Wakeup interface {
	Notify(ctx context.Context)
}

// Metadata is the ambient context of a command — where it came from, and which
// trace it belongs to. It travels into the envelope so the trace survives the
// asynchronous hop (spec §9.2).
type Metadata struct {
	CorrelationID string
	Traceparent   string
}

// Work is the set of repositories bound to one transaction. The infrastructure
// constructs it; a use case can only reach persistence through it, which is what
// makes an untransacted write unexpressible.
//
// Do not add an outbox to this struct. The moment Work exposes one, a use case
// can append to it by hand, and the invariant this whole design exists to make
// structural becomes a rule people have to remember again — with no failing test
// to notice, because the arch test walks imports and this would be an
// intra-package reference. A future use case that genuinely needs a different
// transaction boundary (the relay's claim-publish-mark pass, for instance)
// declares its own boundary port and its own work set.
type Work struct {
	Users domain.UserRepository
}

// UnitOfWork is the transaction boundary. Everything fn does commits together or
// not at all — including the outbox rows, which fn never mentions: the
// implementation drains the aggregates' events and appends them inside the same
// transaction (spec §5, §6.4).
type UnitOfWork interface {
	Do(ctx context.Context, meta Metadata, fn func(Work) error) error
}

// Envelope is one outbox row's worth of content: the routing columns the relay
// reads, and the already-serialised message it must not read.
type Envelope struct {
	EventID       uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	SchemaVersion int
	Payload       []byte
	Headers       map[string]string
	OccurredAt    time.Time
}

// EnvelopeFactory maps domain events onto the message contract. This is an
// application concern: the domain must not know what CloudEvents is, and the
// infrastructure must not decide what a message means.
type EnvelopeFactory interface {
	From(events []domain.Event, meta Metadata) ([]Envelope, error)
}

// OutboxAppender appends envelopes to the outbox. Unlike UserRepository it is
// declared here rather than in the domain, because an outbox is not a domain
// concept — no domain expert has heard of one (spec D5).
//
// It is named for the role, not for the table. The relay's needs — claim, mark
// published, mark failed — are a different role with a different consumer, and
// Plan 2 declares its own port rather than widening this one into a fat
// interface with two disjoint users.
type OutboxAppender interface {
	Append(ctx context.Context, envelopes []Envelope) error
}
```

- [ ] **Step 5: Write the use case**

`internal/accounts/application/register_user.go`:

```go
package application

import (
	"context"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

type RegisterUserCommand struct {
	Email       string
	DisplayName string
	Meta        Metadata
}

type RegisterUserResult struct {
	UserID uuid.UUID
}

// RegisterUser is the whole producing use case. Read it and notice what is
// missing: there is no outbox here. That is the point of spec §5 — the invariant
// "every state change emits its event" is structural, enforced by the unit of
// work, not a line a future call site has to remember to write.
type RegisterUser struct {
	uow    UnitOfWork
	clock  Clock
	ids    IDGen
	wakeup Wakeup
}

func NewRegisterUser(uow UnitOfWork, clock Clock, ids IDGen, wakeup Wakeup) *RegisterUser {
	return &RegisterUser{uow: uow, clock: clock, ids: ids, wakeup: wakeup}
}

func (uc *RegisterUser) Execute(ctx context.Context, cmd RegisterUserCommand) (RegisterUserResult, error) {
	id, err := uc.ids.New()
	if err != nil {
		return RegisterUserResult{}, err
	}

	user, err := domain.Register(id, cmd.Email, cmd.DisplayName, uc.clock.Now())
	if err != nil {
		return RegisterUserResult{}, err // pure domain, no I/O has happened yet
	}

	if err := uc.uow.Do(ctx, cmd.Meta, func(w Work) error {
		return w.Users.Insert(ctx, user) // ErrEmailTaken surfaces here
	}); err != nil {
		return RegisterUserResult{}, err // <- the atomic moment
	}

	// Outside the transaction, best effort, error discarded: a failed wakeup
	// costs latency and nothing else, because the poll loop is authoritative.
	uc.wakeup.Notify(ctx)

	return RegisterUserResult{UserID: user.ID}, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/accounts/application/ -v`
Expected: PASS for all five tests.

- [ ] **Step 7: Commit**

```bash
git add internal/accounts/application
git commit -m "feat(accounts): RegisterUser use case and the application ports"
```

---

## Task 7: The CloudEvents envelope factory

**Files:**
- Create: `internal/platform/messaging/cloudevents.go`
- Create: `internal/accounts/application/envelope.go`
- Test: `internal/platform/messaging/cloudevents_test.go`
- Test: `internal/accounts/application/envelope_test.go`

**Interfaces:**
- Consumes: `Envelope`, `EnvelopeFactory`, `Metadata`, `IDGen` (Task 6); `domain.Event`, `domain.UserRegistered`, `domain.EventTypeUserRegistered` (Task 5).
- Produces:
  - `const messaging.SpecVersion`, `messaging.DataContentType`, `messaging.TimeFormat`
  - `type messaging.Attributes struct { ID, Type, Source, Subject string; Time time.Time; SchemaName, SchemaBase string; Version int; Traceparent string }`
  - `func messaging.EncodeCloudEvent(attrs Attributes, data any) ([]byte, error)`
  - `func NewCloudEventFactory(ids IDGen) CloudEventFactory` implementing `EnvelopeFactory`
  - `var ErrUnmappedEvent = errors.New("application: no message mapping for event type")`
  - Header keys written into the row: `correlation_id`, `traceparent`

**Why the encoder is in `platform/messaging` and the mapping is not.** Spec §9.1
defines *one* envelope for the whole system, and every context publishes it: the
notifications context gets its own `EnvelopeFactory` in Plan 3 (§7, §16). If the
field order, the millisecond precision and the `dataschema` composition lived in
each context, there would be two copies of a published contract, each pinned by
its own test, free to drift while both suites stay green. So the *encoding* is
shared and the *mapping* — which domain event becomes which `data` shape — stays
per-context, where it belongs.

`platform/messaging` is the one platform package §6.1 makes visible to the
application layer, and this is why.

Why only those two headers: spec §9.2 lists `ce_id`, `ce_type`, `ce_specversion`, `schema_version` and `content-type` as Kafka headers too, and §11.2's relay pseudocode composes them from the row's own columns at publish time (`headers = row.headers + ce_* + schema_version`). Storing them twice would create two sources of truth for the same fact. Correlation and trace have no column, so they are stored.

- [ ] **Step 1: Write the failing test**

`internal/accounts/application/envelope_test.go`:

```go
package application

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

type seqIDs struct{ n uint64 }

func (s *seqIDs) New() (uuid.UUID, error) {
	s.n++
	var id uuid.UUID
	id[0] = 0xAA
	id[15] = byte(s.n)
	return id, nil
}

type unmappedEvent struct{}

func (unmappedEvent) EventType() string     { return "com.outboxexpress.accounts.user.forgotten" }
func (unmappedEvent) AggregateType() string { return "User" }
func (unmappedEvent) AggregateID() string   { return "x" }
func (unmappedEvent) SchemaVersion() int    { return 1 }
func (unmappedEvent) OccurredAt() time.Time { return time.Unix(0, 0).UTC() }

func TestCloudEventFactory_ProducesTheExactWireFormat(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})

	event := domain.UserRegistered{
		UserID:       uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1"),
		Email:        "ada@example.com",
		DisplayName:  "Ada Lovelace",
		Version:      1,
		RegisteredAt: time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC),
	}
	meta := Metadata{
		CorrelationID: "corr-7",
		Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	envs, err := f.From([]domain.Event{event}, meta)
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("len(envs) = %d, want 1", len(envs))
	}
	got := envs[0]

	if got.EventID.String() != "aa000000-0000-0000-0000-000000000001" {
		t.Errorf("EventID = %s", got.EventID)
	}
	if got.AggregateType != "User" || got.AggregateID != event.UserID.String() {
		t.Errorf("routing columns = %q/%q", got.AggregateType, got.AggregateID)
	}
	if got.EventType != domain.EventTypeUserRegistered {
		t.Errorf("EventType = %q", got.EventType)
	}
	if got.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d", got.SchemaVersion)
	}
	if !got.OccurredAt.Equal(event.RegisteredAt) {
		t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, event.RegisteredAt)
	}
	if got.Headers["correlation_id"] != "corr-7" {
		t.Errorf("correlation_id = %q", got.Headers["correlation_id"])
	}
	if got.Headers["traceparent"] != meta.Traceparent {
		t.Errorf("traceparent = %q", got.Headers["traceparent"])
	}
	if _, ok := got.Headers["ce_id"]; ok {
		t.Error("ce_* headers are composed by the relay from the row's columns, not stored")
	}

	const want = `{"specversion":"1.0","id":"aa000000-0000-0000-0000-000000000001",` +
		`"type":"com.outboxexpress.accounts.user.registered","source":"/services/accounts",` +
		`"subject":"9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1","time":"2026-08-26T10:15:30.100Z",` +
		`"dataschema":"https://schemas.outboxexpress.dev/accounts/user.registered/1.json",` +
		`"datacontenttype":"application/json",` +
		`"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",` +
		`"data":{"user_id":"9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1","email":"ada@example.com",` +
		`"display_name":"Ada Lovelace","version":1,"registered_at":"2026-08-26T10:15:30.100Z"}}`

	if string(got.Payload) != want {
		t.Errorf("payload mismatch\n got: %s\nwant: %s", got.Payload, want)
	}
}

func TestCloudEventFactory_OmitsTraceparentWhenAbsent(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	envs, err := f.From([]domain.Event{domain.UserRegistered{
		UserID:       uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1"),
		Email:        "ada@example.com",
		DisplayName:  "Ada Lovelace",
		Version:      1,
		RegisteredAt: time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
	}}, Metadata{})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(envs[0].Payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := decoded["traceparent"]; ok {
		t.Error("traceparent must be omitted rather than sent empty")
	}
	if _, ok := envs[0].Headers["traceparent"]; ok {
		t.Error("an empty traceparent must not become a header")
	}
}

func TestCloudEventFactory_MintsOneIDPerEvent(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	at := time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC)
	envs, err := f.From([]domain.Event{
		domain.UserRegistered{UserID: uuid.New(), Email: "a@b.com", DisplayName: "A", Version: 1, RegisteredAt: at},
		domain.UserRegistered{UserID: uuid.New(), Email: "c@d.com", DisplayName: "C", Version: 1, RegisteredAt: at},
	}, Metadata{})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if envs[0].EventID == envs[1].EventID {
		t.Fatal("two events shared one event_id; the consumer would deduplicate one away")
	}
}

func TestCloudEventFactory_RejectsUnmappedEvent(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	_, err := f.From([]domain.Event{unmappedEvent{}}, Metadata{})
	if !errors.Is(err, ErrUnmappedEvent) {
		t.Fatalf("err = %v, want ErrUnmappedEvent", err)
	}
}

func TestCloudEventFactory_EmptyInput(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	envs, err := f.From(nil, Metadata{})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if len(envs) != 0 {
		t.Fatalf("len(envs) = %d, want 0", len(envs))
	}
}
```

`internal/platform/messaging/cloudevents_test.go`:

```go
package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEncodeCloudEvent_FieldOrderAndPrecision(t *testing.T) {
	got, err := EncodeCloudEvent(Attributes{
		ID:         "e-1",
		Type:       "com.example.thing.happened",
		Source:     "/services/example",
		Subject:    "thing-1",
		Time:       time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC),
		SchemaName: "example/thing.happened",
		SchemaBase: "https://schemas.example.test",
		Version:    1,
	}, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("EncodeCloudEvent: %v", err)
	}

	const want = `{"specversion":"1.0","id":"e-1","type":"com.example.thing.happened",` +
		`"source":"/services/example","subject":"thing-1","time":"2026-08-26T10:15:30.100Z",` +
		`"dataschema":"https://schemas.example.test/example/thing.happened/1.json",` +
		`"datacontenttype":"application/json","data":{"k":"v"}}`
	if string(got) != want {
		t.Errorf("mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeCloudEvent_TrailingZerosSurvive(t *testing.T) {
	got, err := EncodeCloudEvent(Attributes{
		Time: time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
	}, nil)
	if err != nil {
		t.Fatalf("EncodeCloudEvent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// RFC3339Nano would render this "…:30Z". The contract is fixed-width.
	if decoded["time"] != "2026-08-26T10:15:30.000Z" {
		t.Errorf("time = %v, want fixed millisecond precision", decoded["time"])
	}
}

func TestEncodeCloudEvent_OmitsEmptyTraceparent(t *testing.T) {
	got, err := EncodeCloudEvent(Attributes{Time: time.Now()}, nil)
	if err != nil {
		t.Fatalf("EncodeCloudEvent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := decoded["traceparent"]; ok {
		t.Error("traceparent must be omitted rather than sent empty")
	}
}
```

- [ ] **Step 2: Run both to verify they fail**

Run: `go test ./internal/platform/messaging/ ./internal/accounts/application/ -run 'CloudEvent|EncodeCloudEvent'`
Expected: FAIL — `undefined: EncodeCloudEvent`, `undefined: NewCloudEventFactory`.

- [ ] **Step 3: Write the factory**

`internal/platform/messaging/cloudevents.go`:

```go
// Package messaging holds the wire contract: the CloudEvents 1.0 envelope every
// bounded context publishes. From Plan 2 it also holds the transport Message,
// the only type that crosses the context boundary (spec §6.5).
//
// This is the one platform package the application layer may import (spec §6.1),
// because a message contract is an application concern — it is what a use case
// promises the outside world, not a detail of how bytes reach a broker.
package messaging

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	SpecVersion     = "1.0"
	DataContentType = "application/json"

	// TimeFormat is RFC 3339 with fixed millisecond precision. time.RFC3339Nano
	// trims trailing zeros, which would make the wire format of a timestamp
	// depend on its value — a needless variation in a published contract.
	TimeFormat = "2006-01-02T15:04:05.000Z07:00"
)

// Attributes are the CloudEvents context attributes the caller supplies.
// Everything else in the envelope is constant or derived from these.
type Attributes struct {
	ID          string
	Type        string
	Source      string
	Subject     string
	Time        time.Time
	SchemaName  string // e.g. "accounts/user.registered"
	SchemaBase  string
	Version     int
	Traceparent string
}

// cloudEvent is the wire format. Field order here is field order in the emitted
// JSON, which is what makes a payload byte-comparable in a test.
type cloudEvent struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Type            string `json:"type"`
	Source          string `json:"source"`
	Subject         string `json:"subject"`
	Time            string `json:"time"`
	DataSchema      string `json:"dataschema"`
	DataContentType string `json:"datacontenttype"`
	Traceparent     string `json:"traceparent,omitempty"`
	Data            any    `json:"data"`
}

// EncodeCloudEvent serialises one envelope. Every context calls this, so there
// is one definition of the published format rather than one per context.
func EncodeCloudEvent(attrs Attributes, data any) ([]byte, error) {
	payload, err := json.Marshal(cloudEvent{
		SpecVersion: SpecVersion,
		ID:          attrs.ID,
		Type:        attrs.Type,
		Source:      attrs.Source,
		Subject:     attrs.Subject,
		Time:        attrs.Time.UTC().Format(TimeFormat),
		DataSchema: fmt.Sprintf("%s/%s/%d.json",
			attrs.SchemaBase, attrs.SchemaName, attrs.Version),
		DataContentType: DataContentType,
		Traceparent:     attrs.Traceparent,
		Data:            data,
	})
	if err != nil {
		return nil, fmt.Errorf("messaging: marshal %s: %w", attrs.Type, err)
	}
	return payload, nil
}
```

`internal/accounts/application/envelope.go`:

```go
package application

import (
	"errors"
	"fmt"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

const (
	// This context's identity on the wire. Constants rather than constructor
	// parameters: there is one accounts service, no configuration path sets
	// either value, and a parameter with one possible argument advertises
	// flexibility that does not exist.
	source     = "/services/accounts"
	schemaBase = "https://schemas.outboxexpress.dev"
)

var ErrUnmappedEvent = errors.New("application: no message mapping for event type")

// CloudEventFactory maps this context's domain events onto the message contract.
//
// The mapping is an explicit type switch rather than reflection over the domain
// struct, for two reasons. The domain carries no struct tags (spec §6.1), so it
// has no opinion about JSON; and a wire contract that changes silently when
// someone renames a domain field is not a contract.
type CloudEventFactory struct {
	ids IDGen
}

func NewCloudEventFactory(ids IDGen) CloudEventFactory {
	return CloudEventFactory{ids: ids}
}

type userRegisteredData struct {
	UserID       string `json:"user_id"`
	Email        string `json:"email"`
	DisplayName  string `json:"display_name"`
	Version      int    `json:"version"`
	RegisteredAt string `json:"registered_at"`
}

func (f CloudEventFactory) From(events []domain.Event, meta Metadata) ([]Envelope, error) {
	if len(events) == 0 {
		return nil, nil
	}

	// Loop-invariant: every envelope from one transaction carries the same
	// correlation and trace. The map is shared deliberately — nothing mutates it
	// after this point, and it is written to a JSONB column as-is.
	h := headers(meta)

	envelopes := make([]Envelope, 0, len(events))
	for _, event := range events {
		data, schemaName, err := mapData(event)
		if err != nil {
			return nil, err
		}

		// One id per event, minted here, at insert time, and never again:
		// regenerating it per publish attempt is the single most common way to
		// break the pattern while appearing to implement it (spec §8.1).
		eventID, err := f.ids.New()
		if err != nil {
			return nil, fmt.Errorf("application: event id: %w", err)
		}

		occurred := event.OccurredAt().UTC()
		payload, err := messaging.EncodeCloudEvent(messaging.Attributes{
			ID:          eventID.String(),
			Type:        event.EventType(),
			Source:      source,
			Subject:     event.AggregateID(),
			Time:        occurred,
			SchemaName:  schemaName,
			SchemaBase:  schemaBase,
			Version:     event.SchemaVersion(),
			Traceparent: meta.Traceparent,
		}, data)
		if err != nil {
			return nil, err
		}

		envelopes = append(envelopes, Envelope{
			EventID:       eventID,
			AggregateType: event.AggregateType(),
			AggregateID:   event.AggregateID(),
			EventType:     event.EventType(),
			SchemaVersion: event.SchemaVersion(),
			Payload:       payload,
			Headers:       h,
			OccurredAt:    occurred,
		})
	}
	return envelopes, nil
}

// mapData is the only context-specific part of the envelope: which domain event
// becomes which `data` shape, and which schema names it.
func mapData(event domain.Event) (any, string, error) {
	switch e := event.(type) {
	case domain.UserRegistered:
		return userRegisteredData{
			UserID:       e.UserID.String(),
			Email:        e.Email,
			DisplayName:  e.DisplayName,
			Version:      e.Version,
			RegisteredAt: e.RegisteredAt.UTC().Format(messaging.TimeFormat),
		}, "accounts/user.registered", nil
	default:
		return nil, "", fmt.Errorf("%w: %s (%T)", ErrUnmappedEvent, event.EventType(), event)
	}
}

// headers holds only what has no column of its own. The relay composes ce_id,
// ce_type, ce_specversion, schema_version and content-type from the row itself
// at publish time (spec §9.2, §11.2).
func headers(meta Metadata) map[string]string {
	h := make(map[string]string, 2)
	if meta.CorrelationID != "" {
		h["correlation_id"] = meta.CorrelationID
	}
	if meta.Traceparent != "" {
		h["traceparent"] = meta.Traceparent
	}
	return h
}

// Compile-time proof that the factory satisfies the port. A test would catch
// this too, but a wiring mistake should not require running tests to find.
var _ EnvelopeFactory = CloudEventFactory{}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/platform/messaging/ ./internal/accounts/application/ -v`
Expected: PASS. The two exact-bytes assertions are the valuable ones: they are the published contract of §9.1, and they fail the moment someone reorders a field or changes the timestamp precision.

- [ ] **Step 5: Run the whole suite and lint**

Run: `make test && make lint`
Expected: PASS, no gofmt complaints.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/messaging internal/accounts/application
git commit -m "feat(messaging): shared CloudEvents 1.0 encoder and the accounts envelope factory"
```

---

## Task 8: The connection pool and the outbox repository

**Files:**
- Create: `internal/platform/postgres/pool.go` (`queryer.go` and the package doc were written in Task 4)
- Create: `internal/accounts/infrastructure/postgres/outbox_repo.go`
- Test: `internal/platform/postgres/pool_test.go`
- Test: `internal/platform/postgres/pool_integration_test.go` (`//go:build integration`)
- Test: `internal/accounts/infrastructure/postgres/outbox_repo_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `application.Envelope` (Task 6); `pgtest.Accounts` (Task 4).
- Produces:
  - `type platformpg.Queryer interface { Exec(...) (pgconn.CommandTag, error); QueryRow(...) pgx.Row; SendBatch(context.Context, *pgx.Batch) pgx.BatchResults }` — satisfied by both `*pgxpool.Pool` and `pgx.Tx`
  - `func platformpg.NewPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error)`
  - `func platformpg.WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error`
  - `func postgres.NewOutboxRepository(q platformpg.Queryer) *postgres.OutboxRepository` implementing `application.OutboxAppender`

`Queryer` has no `Query` method: nothing in this plan reads rows through the
interface, and shipping a method ahead of its caller is the same mistake as
shipping a column nothing reads. Plan 2's claiming repository adds it in the
line that needs it.

`internal/platform/postgres` is imported as `platformpg` wherever a context's own `postgres` package is in scope.

- [ ] **Step 1: Write the failing unit test for the pool**

`internal/platform/postgres/pool_test.go`:

```go
package postgres

import (
	"context"
	"testing"
)

func TestNewPool_RejectsAnUnparseableDSN(t *testing.T) {
	pool, err := NewPool(context.Background(), "://not-a-dsn", 4)
	if err == nil {
		pool.Close()
		t.Fatal("expected an error for an unparseable DSN")
	}
}

func TestNewPool_RejectsANonPositiveMaxConns(t *testing.T) {
	_, err := NewPool(context.Background(), "postgres://oe:oe@localhost:5432/oe_accounts", 0)
	if err == nil {
		t.Fatal("expected an error for maxConns = 0")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/platform/postgres/`
Expected: FAIL — `undefined: NewPool`.

- [ ] **Step 3: Write the pool**

`internal/platform/postgres/pool.go`:

```go
// Package postgres holds the pgx plumbing every context shares: pool
// construction, the small interface that lets a repository work equally against
// a pool or a transaction, and the readiness check.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Queryer is what a repository needs. Both *pgxpool.Pool and pgx.Tx satisfy it,
// which is the whole trick behind the unit of work: the same repository code is
// transaction-bound or not depending only on what it was handed, and the
// application layer is never handed a pool.
type Queryer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

func NewPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	if maxConns <= 0 {
		return nil, fmt.Errorf("postgres: maxConns must be positive, got %d", maxConns)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	// A warm floor, so that the first registration after an idle period does not
	// pay TCP connect, startup and auth inside a user-facing request. NewPool
	// already blocks on Ping, so this work happens where it belongs.
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

// WithTx runs fn inside a transaction, committing if it returns nil.
//
// It exists because three non-obvious rules have to be got right every time, and
// this plan is the first of four places that need them:
//
//  1. Rollback after a successful Commit is a no-op in pgx, so the deferred
//     rollback needs no "did we commit?" flag.
//  2. Begin and Commit errors are wrapped — they are this layer's failures.
//  3. fn's error is returned **unwrapped**, so that a caller several layers up
//     can still errors.Is it against a domain sentinel. Wrapping it here is how
//     a 409 Conflict silently becomes a 500.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `go test ./internal/platform/postgres/ -v`
Expected: PASS. The second test proves the guard runs before any network call, which is why it does not need a database.

- [ ] **Step 5: Write the pool integration test**

`internal/platform/postgres/pool_integration_test.go`:

```go
//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

func TestNewPool_ConnectsAndPings(t *testing.T) {
	dsn, _ := pgtest.Accounts(t)
	ctx := context.Background()

	pool, err := platformpg.NewPool(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestWithTx(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	t.Run("commits", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		err := platformpg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO accounts.users (id, email, display_name)
			                        VALUES (gen_random_uuid(), 'a@example.com', 'A')`)
			return err
		})
		if err != nil {
			t.Fatalf("WithTx: %v", err)
		}
		if users, _ := pgtest.CountAccounts(t, pool); users != 1 {
			t.Fatalf("users = %d, want 1", users)
		}
	})

	t.Run("rolls back and returns fn's error unwrapped", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		sentinel := errors.New("sentinel")
		err := platformpg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `INSERT INTO accounts.users (id, email, display_name)
			                           VALUES (gen_random_uuid(), 'a@example.com', 'A')`); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want the sentinel — wrapping it here is how a 409 becomes a 500", err)
		}
		if users, _ := pgtest.CountAccounts(t, pool); users != 0 {
			t.Fatalf("users = %d, want 0", users)
		}
	})
}
```

- [ ] **Step 6: Write the failing outbox repository integration test**

`internal/accounts/infrastructure/postgres/outbox_repo_integration_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func envelope(t *testing.T, aggregateID string, payload string) application.Envelope {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return application.Envelope{
		EventID:       id,
		AggregateType: "User",
		AggregateID:   aggregateID,
		EventType:     "com.outboxexpress.accounts.user.registered",
		SchemaVersion: 1,
		Payload:       []byte(payload),
		Headers:       map[string]string{"correlation_id": "corr-1"},
		OccurredAt:    time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
	}
}

func TestOutboxRepository_Append(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	a := envelope(t, "user-a", `{"specversion":"1.0","data":{"email":"a@b.com"}}`)
	b := envelope(t, "user-b", `{"specversion":"1.0","data":{"email":"c@d.com"}}`)

	if err := repo.Append(ctx, []application.Envelope{a, b}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, schema_version,
		       payload::text, headers::text, occurred_at, status, attempts,
		       available_at <= now() AS due, last_error, published_at
		  FROM accounts.outbox
		 ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		id            int64
		eventID       uuid.UUID
		aggType       string
		aggID         string
		eventType     string
		schemaVersion int
		payload       string
		headers       string
		occurredAt    time.Time
		status        string
		attempts      int
		due           bool
		lastError     *string
		publishedAt   *time.Time
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.eventID, &r.aggType, &r.aggID, &r.eventType,
			&r.schemaVersion, &r.payload, &r.headers, &r.occurredAt, &r.status,
			&r.attempts, &r.due, &r.lastError, &r.publishedAt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(got))
	}
	if got[0].id >= got[1].id {
		t.Errorf("ids %d, %d are not ascending — BIGSERIAL is the ordering authority", got[0].id, got[1].id)
	}
	if got[0].eventID != a.EventID || got[1].eventID != b.EventID {
		t.Error("event ids were not stored as given; the relay must never mint one")
	}
	if got[0].aggID != "user-a" || got[1].aggID != "user-b" {
		t.Errorf("aggregate ids = %q, %q", got[0].aggID, got[1].aggID)
	}
	// Append writes eight columns and leaves the relay's five to their defaults.
	// Asserting that once is enough — it is a property of the INSERT, not of the
	// row — and it keeps this test from breaking when Plan 2 touches those columns.
	if first := got[0]; first.status != "pending" || first.attempts != 0 || !first.due ||
		first.lastError != nil || first.publishedAt != nil || first.schemaVersion != 1 {
		t.Errorf("relay bookkeeping was not left at its defaults: %+v", first)
	}

	// JSONB normalises: it sorts object keys and drops insignificant whitespace,
	// so the stored bytes are not the factory's bytes. Compare meaning, not
	// bytes. The byte-exact contract test lives in the application layer, where
	// the bytes are still the ones that will be published.
	assertSameJSON(t, got[0].payload, string(a.Payload))
	assertSameJSON(t, got[0].headers, `{"correlation_id":"corr-1"}`)

	if !got[0].occurredAt.Equal(a.OccurredAt) {
		t.Errorf("occurred_at = %v, want %v", got[0].occurredAt, a.OccurredAt)
	}
}

func TestOutboxRepository_Append_RejectsADuplicateEventID(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	e := envelope(t, "user-a", `{"specversion":"1.0"}`)
	if err := repo.Append(ctx, []application.Envelope{e}); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if err := repo.Append(ctx, []application.Envelope{e}); err == nil {
		t.Fatal("expected the event_id UNIQUE constraint to reject the duplicate")
	}
}

func TestOutboxRepository_Append_EmptyIsANoOp(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	if err := NewOutboxRepository(pool).Append(ctx, nil); err != nil {
		t.Fatalf("Append(nil): %v", err)
	}
	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 0 {
		t.Fatalf("outbox = %d, want 0", outbox)
	}
}

func assertSameJSON(t *testing.T, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	// Both sides are now trees of map[string]any / []any / float64 / string, which
	// DeepEqual compares directly — no need to re-marshal just to sort keys.
	if !reflect.DeepEqual(g, w) {
		t.Errorf("json mismatch\n got: %s\nwant: %s", got, want)
	}
}
```

- [ ] **Step 7: Run it to verify it fails**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/postgres/`
Expected: FAIL — `undefined: NewOutboxRepository`.

- [ ] **Step 8: Write the outbox repository**

`internal/accounts/infrastructure/postgres/outbox_repo.go`:

```go
// Package postgres is the accounts context's persistence layer: repositories,
// and the unit of work that makes the outbox insert structural.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// status, attempts and available_at are left to their column defaults rather
// than written here: the relay owns that bookkeeping, and a producer that set
// them would be making a decision that is not its to make.
const insertOutbox = `
INSERT INTO accounts.outbox
    (event_id, aggregate_type, aggregate_id, event_type, schema_version,
     payload, headers, occurred_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

type OutboxRepository struct {
	q platformpg.Queryer
}

func NewOutboxRepository(q platformpg.Queryer) *OutboxRepository {
	return &OutboxRepository{q: q}
}

// Append writes every envelope in one round trip. A pgx.Batch rather than a loop
// of Exec calls because these inserts sit inside the business transaction, and a
// transaction that is open for N network round trips holds its locks — and the
// xmin horizon — N times longer than it needs to (spec §11.1 rule 4).
func (r *OutboxRepository) Append(ctx context.Context, envelopes []application.Envelope) error {
	if len(envelopes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range envelopes {
		batch.Queue(insertOutbox,
			e.EventID,
			e.AggregateType,
			e.AggregateID,
			e.EventType,
			e.SchemaVersion,
			e.Payload,
			e.Headers,
			e.OccurredAt,
		)
	}
	results := r.q.SendBatch(ctx, batch)
	for i := range envelopes {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("postgres: append outbox row %d (event_id %s): %w",
				i, envelopes[i].EventID, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("postgres: close outbox batch: %w", err)
	}
	return nil
}

var _ application.OutboxAppender = (*OutboxRepository)(nil)
```

- [ ] **Step 9: Run the integration tests to verify they pass**

Run: `go test -tags=integration -count=1 ./internal/platform/postgres/ ./internal/accounts/infrastructure/postgres/ -v`
Expected: PASS. Note in particular that `available_at <= now()` is true — a row is due the instant it is written, which is what makes the relay's first pass pick it up.

- [ ] **Step 10: Commit**

```bash
git add internal/platform/postgres internal/accounts/infrastructure/postgres
git commit -m "feat(accounts): pgx pool, schema readiness check and batched outbox append"
```

---

## Task 9: The user repository and the aggregate tracker

**Files:**
- Create: `internal/accounts/infrastructure/postgres/tracker.go`
- Create: `internal/accounts/infrastructure/postgres/user_repo.go`
- Test: `internal/accounts/infrastructure/postgres/tracker_test.go`
- Test: `internal/accounts/infrastructure/postgres/user_repo_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `platformpg.Queryer` (Task 8); `domain.User`, `domain.EventSource`, `domain.ErrEmailTaken` (Task 5).
- Produces (all unexported — only the unit of work constructs them):
  - `type tracker struct`, `func newTracker() *tracker`, `func (t *tracker) track(s domain.EventSource)`, `func (t *tracker) drain() []domain.Event`
  - `type userRepository struct`, `func newUserRepository(q platformpg.Queryer, tr *tracker) *userRepository` implementing `domain.UserRepository`

- [ ] **Step 1: Write the failing tracker test**

`internal/accounts/infrastructure/postgres/tracker_test.go`:

```go
package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

func mustRegister(t *testing.T, email string) *domain.User {
	t.Helper()
	u, err := domain.Register(uuid.New(), email, "Ada Lovelace",
		time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return u
}

func TestTracker_DrainsInTrackingOrder(t *testing.T) {
	a := mustRegister(t, "a@example.com")
	b := mustRegister(t, "b@example.com")

	tr := newTracker()
	tr.track(a)
	tr.track(b)

	events := tr.drain()
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	if events[0].AggregateID() != a.ID.String() {
		t.Errorf("first event is %s, want %s — tracking order is publish order",
			events[0].AggregateID(), a.ID)
	}
	if events[1].AggregateID() != b.ID.String() {
		t.Errorf("second event is %s, want %s", events[1].AggregateID(), b.ID)
	}
}

func TestTracker_DrainIsIdempotent(t *testing.T) {
	tr := newTracker()
	tr.track(mustRegister(t, "a@example.com"))

	if got := len(tr.drain()); got != 1 {
		t.Fatalf("first drain = %d, want 1", got)
	}
	if got := len(tr.drain()); got != 0 {
		t.Fatalf("second drain = %d, want 0", got)
	}
}

// This is a test of PullEvents' drain semantics, which is what makes tracking
// idempotent without a membership check.
func TestTracker_TrackingTheSameAggregateTwiceDoesNotDuplicateItsEvents(t *testing.T) {
	u := mustRegister(t, "a@example.com")
	tr := newTracker()
	tr.track(u)
	tr.track(u)

	if got := len(tr.drain()); got != 1 {
		t.Fatalf("drain = %d events, want 1 — a repository that writes an "+
			"aggregate twice in one transaction must not publish its event twice", got)
	}
}

func TestTracker_EmptyDrain(t *testing.T) {
	if got := newTracker().drain(); len(got) != 0 {
		t.Fatalf("drain = %d, want 0", len(got))
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/accounts/infrastructure/postgres/`
Expected: FAIL — `undefined: newTracker`.

- [ ] **Step 3: Write the tracker**

`internal/accounts/infrastructure/postgres/tracker.go`:

```go
package postgres

import "github.com/AymanKastali/outboxexpress/internal/accounts/domain"

// tracker remembers which aggregates a transaction touched, so that the unit of
// work can drain their events at commit time without the use case listing them.
// This is the mechanism behind spec §5: the outbox insert is not something a
// caller remembers to do, it is something the persistence layer does because the
// caller persisted an aggregate.
type tracker struct {
	sources []domain.EventSource
}

func newTracker() *tracker { return &tracker{} }

// track records an aggregate. Repositories call it after a successful write, and
// only after: an aggregate whose INSERT failed has not changed any state, so it
// has nothing to announce.
//
// There is no de-duplication here, and none is needed: PullEvents drains, so an
// aggregate recorded twice yields its events on the first pull and nothing on
// the second.
func (t *tracker) track(s domain.EventSource) {
	t.sources = append(t.sources, s)
}

// drain pulls every tracked aggregate's events, in the order they were tracked,
// and forgets them. Ordering matters: these become outbox rows, and outbox row
// order is publish order.
func (t *tracker) drain() []domain.Event {
	events := make([]domain.Event, 0, len(t.sources))
	for _, s := range t.sources {
		events = append(events, s.PullEvents()...)
	}
	t.sources = nil
	return events
}
```

- [ ] **Step 4: Run the tracker test to verify it passes**

Run: `go test ./internal/accounts/infrastructure/postgres/ -v -run Tracker`
Expected: PASS for all four tests.

- [ ] **Step 5: Write the failing user repository integration test**

`internal/accounts/infrastructure/postgres/user_repo_integration_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func TestUserRepository_Insert(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	tr := newTracker()
	repo := newUserRepository(pool, tr)

	user := mustRegister(t, "ada@example.com")
	if err := repo.Insert(ctx, user); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var (
		email, displayName string
		version            int
		createdAt          time.Time
	)
	err := pool.QueryRow(ctx,
		`SELECT email, display_name, version, created_at FROM accounts.users WHERE id = $1`,
		user.ID).Scan(&email, &displayName, &version, &createdAt)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if email != "ada@example.com" || displayName != "Ada Lovelace" || version != 1 {
		t.Errorf("row = %q/%q/%d", email, displayName, version)
	}
	if !createdAt.UTC().Equal(user.CreatedAt) {
		t.Errorf("created_at = %v, want %v", createdAt.UTC(), user.CreatedAt)
	}
	if got := len(tr.drain()); got != 1 {
		t.Errorf("tracker holds %d events after a successful insert, want 1", got)
	}
}

func TestUserRepository_Insert_DuplicateEmailBecomesADomainError(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	tr := newTracker()
	repo := newUserRepository(pool, tr)

	if err := repo.Insert(ctx, mustRegister(t, "ada@example.com")); err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	tr.drain()

	err := repo.Insert(ctx, mustRegister(t, "ada@example.com"))
	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("err = %v, want domain.ErrEmailTaken", err)
	}
	if got := len(tr.drain()); got != 0 {
		t.Errorf("tracker holds %d events after a failed insert, want 0 — a state "+
			"change that did not happen must not be announced", got)
	}
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/postgres/ -run UserRepository`
Expected: FAIL — `undefined: newUserRepository`.

- [ ] **Step 7: Write the user repository**

`internal/accounts/infrastructure/postgres/user_repo.go`:

```go
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

const insertUser = `
INSERT INTO accounts.users (id, email, display_name, version, created_at)
VALUES ($1, $2, $3, $4, $5)`

// uniqueViolation is PostgreSQL's SQLSTATE for a unique constraint breach.
const uniqueViolation = "23505"

// emailUniqueConstraint is the constraint name PostgreSQL generates for the
// UNIQUE on accounts.users.email. Matching the name rather than just the
// SQLSTATE means a future unique index on this table cannot be silently
// misreported as a taken email.
const emailUniqueConstraint = "users_email_key"

type userRepository struct {
	q       platformpg.Queryer
	tracker *tracker
}

func newUserRepository(q platformpg.Queryer, tr *tracker) *userRepository {
	return &userRepository{q: q, tracker: tr}
}

func (r *userRepository) Insert(ctx context.Context, u *domain.User) error {
	_, err := r.q.Exec(ctx, insertUser, u.ID, u.Email, u.DisplayName, u.Version, u.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) &&
			pgErr.Code == uniqueViolation &&
			pgErr.ConstraintName == emailUniqueConstraint {
			// The domain owns the rule; the database is where it can actually be
			// enforced under concurrency. Translating here keeps the rule's
			// vocabulary in the domain and its enforcement where it belongs.
			return fmt.Errorf("%w: %s", domain.ErrEmailTaken, u.Email)
		}
		return fmt.Errorf("postgres: insert user %s: %w", u.ID, err)
	}
	// Only after a successful write. The unit of work will drain this aggregate's
	// events into the outbox inside the same transaction.
	r.tracker.track(u)
	return nil
}

var _ domain.UserRepository = (*userRepository)(nil)
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/postgres/ -v`
Expected: PASS, including the Task 8 outbox tests.

- [ ] **Step 9: Commit**

```bash
git add internal/accounts/infrastructure/postgres
git commit -m "feat(accounts): user repository with unique-violation mapping and aggregate tracking"
```

---

## Task 10: The unit of work — the atomic moment

This is the task the plan exists for. Everything before it was scaffolding; everything after it is delivery. Review it slowly.

**Files:**
- Create: `internal/accounts/infrastructure/postgres/uow.go`
- Test: `internal/accounts/infrastructure/postgres/uow_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `newTracker`, `newUserRepository` (Task 9); `NewOutboxRepository`, `platformpg.WithTx`, `platformpg.Queryer` (Task 8); `application.UnitOfWork`, `application.Work`, `application.Metadata`, `application.EnvelopeFactory`, `application.OutboxAppender` (Task 6).
- Produces: `func NewUnitOfWork(pool *pgxpool.Pool, envelopes application.EnvelopeFactory) *UnitOfWork` implementing `application.UnitOfWork`.

- [ ] **Step 1: Write the failing tests**

`internal/accounts/infrastructure/postgres/uow_integration_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

var uowNow = time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC)

// The frozen clock lives here, in the test that needs it, not in the clock
// package. Two lines beat an exported type that production code never calls.
type frozenClock struct{ at time.Time }

func (c frozenClock) Now() time.Time { return c.at }

type countingWakeup struct{ calls int }

func (w *countingWakeup) Notify(ctx context.Context) { w.calls++ }

type failingFactory struct{ err error }

func (f failingFactory) From([]domain.Event, application.Metadata) ([]application.Envelope, error) {
	return nil, f.err
}

type failingAppender struct{ err error }

func (f failingAppender) Append(context.Context, []application.Envelope) error { return f.err }

func newUOW(pool *pgxpool.Pool) *UnitOfWork {
	return NewUnitOfWork(pool, application.NewCloudEventFactory(ids.UUIDv7{}))
}

// The invariant. A use case that never mentions the outbox produces an outbox
// row, in the same transaction, because it persisted an aggregate (spec §5).
func TestUnitOfWork_RegistrationWritesUserAndOutboxRowTogether(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	wake := &countingWakeup{}
	uc := application.NewRegisterUser(newUOW(pool), frozenClock{at: uowNow}, ids.UUIDv7{}, wake)

	res, err := uc.Execute(ctx, application.RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
		Meta:        application.Metadata{CorrelationID: "corr-1", Traceparent: "tp-1"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	users, outbox := pgtest.CountAccounts(t, pool)
	if users != 1 || outbox != 1 {
		t.Fatalf("users = %d, outbox = %d; want 1 and 1", users, outbox)
	}

	var (
		eventID       string
		aggregateID   string
		aggregateType string
		eventType     string
		status        string
		payload       string
		headers       string
		occurredAt    time.Time
	)
	err = pool.QueryRow(ctx, `
		SELECT event_id::text, aggregate_id, aggregate_type, event_type, status,
		       payload::text, headers::text, occurred_at
		  FROM accounts.outbox`).Scan(&eventID, &aggregateID, &aggregateType,
		&eventType, &status, &payload, &headers, &occurredAt)
	if err != nil {
		t.Fatalf("select outbox: %v", err)
	}

	if aggregateID != res.UserID.String() {
		t.Errorf("aggregate_id = %s, want the user id %s — this is the partition key",
			aggregateID, res.UserID)
	}
	if aggregateType != domain.AggregateTypeUser {
		t.Errorf("aggregate_type = %q, want %q", aggregateType, domain.AggregateTypeUser)
	}
	if eventType != domain.EventTypeUserRegistered {
		t.Errorf("event_type = %q", eventType)
	}
	if status != "pending" {
		t.Errorf("status = %q, want pending — nothing has published it yet", status)
	}
	if !occurredAt.UTC().Equal(uowNow) {
		t.Errorf("occurred_at = %v, want the injected clock's time %v", occurredAt.UTC(), uowNow)
	}

	var envelope map[string]any
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if envelope["id"] != eventID {
		t.Errorf("payload id %v != event_id column %s — the column and the "+
			"envelope must carry the same deduplication key", envelope["id"], eventID)
	}
	if envelope["subject"] != res.UserID.String() {
		t.Errorf("subject = %v, want %s", envelope["subject"], res.UserID)
	}
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is %T, want an object", envelope["data"])
	}
	if data["email"] != "ada@example.com" {
		t.Errorf("data.email = %v", data["email"])
	}

	var hdrs map[string]string
	if err := json.Unmarshal([]byte(headers), &hdrs); err != nil {
		t.Fatalf("headers is not JSON: %v", err)
	}
	if hdrs["correlation_id"] != "corr-1" || hdrs["traceparent"] != "tp-1" {
		t.Errorf("headers = %v", hdrs)
	}

	if wake.calls != 1 {
		t.Errorf("wakeup.calls = %d, want 1", wake.calls)
	}
}

// The dual-write problem, refused. If the business write fails there is no
// event, and if the event cannot be written there is no user.
func TestUnitOfWork_NeitherRowSurvivesAFailure(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	t.Run("the work fails", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		boom := errors.New("boom")
		err := newUOW(pool).Do(ctx, application.Metadata{}, func(w application.Work) error {
			if err := w.Users.Insert(ctx, mustRegister(t, "ada@example.com")); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0", users, outbox)
		}
	})

	t.Run("the envelope cannot be built", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		boom := errors.New("no mapping")
		uow := NewUnitOfWork(pool, failingFactory{err: boom})
		err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
			return w.Users.Insert(ctx, mustRegister(t, "ada@example.com"))
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the factory error", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0 — a message that "+
				"cannot be built must take the business write down with it", users, outbox)
		}
	})

	t.Run("the outbox append fails", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		boom := errors.New("outbox is unwritable")
		uow := newUOW(pool)
		uow.outbox = func(platformpg.Queryer) application.OutboxAppender {
			return failingAppender{err: boom}
		}
		err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
			return w.Users.Insert(ctx, mustRegister(t, "ada@example.com"))
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the appender error", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0 — this is the dual "+
				"write refused in its purest form: the event could not be "+
				"recorded, so the state change must not have happened either",
				users, outbox)
		}
	})

	t.Run("the email is taken", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		uc := application.NewRegisterUser(newUOW(pool), frozenClock{at: uowNow}, ids.UUIDv7{}, &countingWakeup{})
		cmd := application.RegisterUserCommand{Email: "ada@example.com", DisplayName: "Ada Lovelace"}

		if _, err := uc.Execute(ctx, cmd); err != nil {
			t.Fatalf("first Execute: %v", err)
		}
		if _, err := uc.Execute(ctx, cmd); !errors.Is(err, domain.ErrEmailTaken) {
			t.Fatalf("second Execute: err = %v, want ErrEmailTaken", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 1 || outbox != 1 {
			t.Fatalf("users = %d, outbox = %d; want 1 and 1 — the rejected "+
				"registration must leave nothing behind", users, outbox)
		}
	})
}

func TestUnitOfWork_TwoAggregatesInOneTransactionProduceTwoRowsInOrder(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	first := mustRegister(t, "ada@example.com")
	second := mustRegister(t, "grace@example.com")

	err := newUOW(pool).Do(ctx, application.Metadata{}, func(w application.Work) error {
		if err := w.Users.Insert(ctx, first); err != nil {
			return err
		}
		return w.Users.Insert(ctx, second)
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	rows, err := pool.Query(ctx, `SELECT aggregate_id FROM accounts.outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var order []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		order = append(order, id)
	}
	if len(order) != 2 {
		t.Fatalf("len = %d, want 2", len(order))
	}
	if order[0] != first.ID.String() || order[1] != second.ID.String() {
		t.Errorf("order = %v, want [%s %s] — id order is publish order",
			order, first.ID, second.ID)
	}
}

func TestUnitOfWork_CommitsWorkThatEmitsNothing(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	err := newUOW(pool).Do(ctx, application.Metadata{}, func(w application.Work) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 0 {
		t.Fatalf("outbox = %d, want 0", outbox)
	}
}

// The limitation of the structural approach, stated as a test: events are
// collected from aggregates the repositories were given, so SQL that goes around
// a repository changes state and announces nothing. That is a real cost of §5's
// design and the reason raw UPDATEs do not belong in a use case.
func TestUnitOfWork_AWriteThatBypassesTheRepositoryEmitsNothing(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	uow := newUOW(pool)
	user := mustRegister(t, "ada@example.com")

	if err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
		return w.Users.Insert(ctx, user)
	}); err != nil {
		t.Fatalf("first Do: %v", err)
	}

	// A second transaction that changes the row with raw SQL rather than through
	// the repository. No aggregate is tracked, so no event is collected.
	if err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
		_, err := pool.Exec(ctx, `UPDATE accounts.users SET display_name = 'Ada L.' WHERE id = $1`, user.ID)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("second Do: %v", err)
	}

	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 1 {
		t.Fatalf("outbox = %d, want 1", outbox)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/postgres/ -run UnitOfWork`
Expected: FAIL — `undefined: NewUnitOfWork`.

- [ ] **Step 3: Write the unit of work**

`internal/accounts/infrastructure/postgres/uow.go`:

```go
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// UnitOfWork is the transaction boundary, and the reason this project has an
// outbox at all.
//
// Spec §5 is the argument: writing the business row and the outbox row in one
// transaction is the entire pattern, and making that structural — something the
// persistence layer does because an aggregate was persisted — is "the only way
// 'every state change emits its event' becomes structural rather than a rule
// people must remember at each call site".
//
// The cost is that the two inserts are no longer adjacent in the code a reader
// follows. That is why this file is short, and why Task 10's tests assert the
// invariant directly rather than trusting it.
type UnitOfWork struct {
	pool      *pgxpool.Pool
	envelopes application.EnvelopeFactory

	// outbox builds the appender for a transaction. It is a field rather than a
	// direct call to NewOutboxRepository so that a test can substitute an
	// appender that fails: "the outbox insert failed, so the user must not
	// exist" is the dual-write refusal in its purest form, and there is no way
	// to provoke it from outside — the factory mints a fresh UUIDv7 per event,
	// so not even the event_id constraint can be made to fire.
	outbox func(platformpg.Queryer) application.OutboxAppender
}

func NewUnitOfWork(pool *pgxpool.Pool, envelopes application.EnvelopeFactory) *UnitOfWork {
	return &UnitOfWork{
		pool:      pool,
		envelopes: envelopes,
		outbox: func(q platformpg.Queryer) application.OutboxAppender {
			return NewOutboxRepository(q)
		},
	}
}

func (u *UnitOfWork) Do(ctx context.Context, meta application.Metadata, fn func(application.Work) error) error {
	// WithTx owns the begin/rollback/commit rules — including returning fn's
	// error unwrapped, so that domain sentinels survive the trip out (spec §6.4).
	return platformpg.WithTx(ctx, u.pool, func(tx pgx.Tx) error {
		tracker := newTracker()

		// Repositories are bound to tx and handed to fn as the only way to reach
		// persistence. The pool is not reachable from here, so there is no path
		// to an untransacted write (spec §11.1 rule 1).
		if err := fn(application.Work{Users: newUserRepository(tx, tracker)}); err != nil {
			return err
		}

		// Drain what the repositories touched. Nothing external happens in here:
		// no HTTP, no produce, no email (spec §11.1 rule 2).
		if events := tracker.drain(); len(events) > 0 {
			envelopes, err := u.envelopes.From(events, meta)
			if err != nil {
				return err
			}
			if err := u.outbox(tx).Append(ctx, envelopes); err != nil {
				return err
			}
		}
		return nil
	})
}

var _ application.UnitOfWork = (*UnitOfWork)(nil)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/postgres/ -v`
Expected: PASS for every test in the package. `TestUnitOfWork_NeitherRowSurvivesAFailure` is the dual-write problem being refused four different ways; if any of its subtests fails, stop and fix it before continuing — nothing later in the plan is meaningful without it.

- [ ] **Step 5: Commit**

```bash
git add internal/accounts/infrastructure/postgres/uow.go internal/accounts/infrastructure/postgres/uow_integration_test.go
git commit -m "feat(accounts): unit of work that drains aggregate events into the outbox on commit"
```

---

## Task 11: The relay wakeup

**Files:**
- Create: `internal/accounts/infrastructure/wakeup/notify.go`
- Test: `internal/accounts/infrastructure/wakeup/notify_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `application.Wakeup` (Task 6).
- Produces:
  - `const wakeup.ChannelOutboxNew = "outbox_new"`
  - `func wakeup.NewNotifier(pool *pgxpool.Pool, channel string, log *slog.Logger) *Notifier` implementing `application.Wakeup`

There is no `Discard` no-op wakeup. `RELAY_USE_NOTIFY` is a relay variable
(spec §14 gives it to `relay` only), `config.API` has no field for it, and every
test here declares its own fake — so a no-op shipped now would be production code
with no production caller, which Task 3 already argued against for the clock.
Plan 2 adds it, in the process that reads the flag.

- [ ] **Step 1: Write the failing test**

`internal/accounts/infrastructure/wakeup/notify_integration_test.go`:

```go
//go:build integration

package wakeup

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func TestNotifier_DeliversToAListener(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	listener, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+ChannelOutboxNew); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	n := NewNotifier(pool, ChannelOutboxNew, slog.New(slog.DiscardHandler))
	n.Notify(ctx)

	notification, err := listener.Conn().WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("WaitForNotification: %v", err)
	}
	if notification.Channel != ChannelOutboxNew {
		t.Errorf("channel = %q, want %q", notification.Channel, ChannelOutboxNew)
	}
}

func TestNotifier_SwallowsFailure(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	n := NewNotifier(pool, ChannelOutboxNew, slog.New(slog.DiscardHandler))

	// A cancelled context makes the Exec fail. Notify must return quietly: a
	// failed wakeup costs latency, and a registration that has already committed
	// must not be reported as failed because an optimisation did not fire.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n.Notify(ctx)
}
```

`slog.DiscardHandler` is available in Go 1.24 and later; this project is on 1.27.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/wakeup/`
Expected: FAIL — `undefined: NewNotifier`.

- [ ] **Step 3: Write the notifier**

`internal/accounts/infrastructure/wakeup/notify.go`:

```go
// Package wakeup tells the relay that there is something to publish, so it does
// not have to wait out its idle backoff.
//
// This is an optimisation and never the delivery path (spec §13.1). PostgreSQL
// notifications are not durable and are not delivered to a listener that was
// disconnected when they were sent; the relay's poll loop, with its timeout,
// remains the source of truth. Every failure here is therefore logged and
// dropped.
package wakeup

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// ChannelOutboxNew is the channel the relay listens on.
const ChannelOutboxNew = "outbox_new"

type Notifier struct {
	pool    *pgxpool.Pool
	channel string
	log     *slog.Logger
}

func NewNotifier(pool *pgxpool.Pool, channel string, log *slog.Logger) *Notifier {
	return &Notifier{pool: pool, channel: channel, log: log}
}

// Notify fires pg_notify outside any transaction. It is called after the commit
// returns, never inside it: NOTIFY inside a transaction is deferred to commit
// anyway, and holding the intent inside the business transaction would tie the
// write path to the notify queue's health.
//
// pg_notify is used rather than NOTIFY because the channel name is a parameter,
// and NOTIFY takes only a literal.
func (n *Notifier) Notify(ctx context.Context) {
	if _, err := n.pool.Exec(ctx, `SELECT pg_notify($1, '')`, n.channel); err != nil {
		n.log.Warn("outbox wakeup failed; the relay will find the row by polling",
			"channel", n.channel, "error", err)
	}
}

var _ application.Wakeup = (*Notifier)(nil)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags=integration -count=1 ./internal/accounts/infrastructure/wakeup/ -v`
Expected: PASS. `TestNotifier_DeliversToAListener` is the only proof that the channel name the relay will listen on in Plan 2 is the one the api sends to.

- [ ] **Step 5: Commit**

```bash
git add internal/accounts/infrastructure/wakeup
git commit -m "feat(accounts): pg_notify relay wakeup, best effort by design"
```

---

## Task 12: The HTTP presentation layer

**Files:**
- Create: `internal/accounts/presentation/http/dto.go`
- Create: `internal/accounts/presentation/http/handler.go`
- Create: `internal/accounts/presentation/http/router.go`
- Test: `internal/accounts/presentation/http/handler_test.go`

**Interfaces:**
- Consumes: `application.RegisterUserCommand`, `application.RegisterUserResult`, `application.Metadata`, `application.IDGen` (Task 6); `domain.ErrInvalidEmail`, `domain.ErrInvalidDisplayName`, `domain.ErrEmailTaken` (Task 5).
- Produces:
  - `type httpapi.Registrar interface { Execute(ctx context.Context, cmd application.RegisterUserCommand) (application.RegisterUserResult, error) }`
  - `func httpapi.NewHandler(register Registrar, ids application.IDGen, log *slog.Logger) *Handler`
  - `func (h *Handler) RegisterUser(w http.ResponseWriter, r *http.Request)`
  - `func httpapi.NewRouter(h *Handler) *http.ServeMux`

There is no liveness handler here. Spec §13.5 puts `GET /healthz` on the public
listener as well as the admin one, but that is one operator contract, not two:
Task 13 exports it from `platform/admin` and `cmd/api` mounts the same handler on
both muxes. Two implementations would mean a probe configuration that is not
portable between ports.

The directory is `presentation/http` per the spec's layout, but the package is named `httpapi`: a package literally named `http` that imports `net/http` compiles, and reads like a puzzle. `Registrar` is declared here, by the consumer, rather than in the application layer — the handler states what it needs and the use case happens to satisfy it.

- [ ] **Step 1: Write the failing tests**

`internal/accounts/presentation/http/handler_test.go`:

```go
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

var handlerUserID = uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1")

type fakeRegistrar struct {
	err     error
	lastCmd application.RegisterUserCommand
	calls   int
}

func (f *fakeRegistrar) Execute(ctx context.Context, cmd application.RegisterUserCommand) (application.RegisterUserResult, error) {
	f.calls++
	f.lastCmd = cmd
	if f.err != nil {
		return application.RegisterUserResult{}, f.err
	}
	return application.RegisterUserResult{UserID: handlerUserID}, nil
}

type staticIDs struct{}

func (staticIDs) New() (uuid.UUID, error) {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111"), nil
}

func serve(t *testing.T, reg Registrar, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	h := NewHandler(reg, staticIDs{}, slog.New(slog.DiscardHandler))
	rec := httptest.NewRecorder()
	NewRouter(h).ServeHTTP(rec, req)
	return rec
}

func postUsers(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestRegisterUser_Created(t *testing.T) {
	reg := &fakeRegistrar{}
	rec := serve(t, reg, postUsers(`{"email":"ada@example.com","display_name":"Ada Lovelace"}`))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/users/"+handlerUserID.String() {
		t.Errorf("Location = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	var body struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.UserID != handlerUserID.String() {
		t.Errorf("user_id = %q", body.UserID)
	}
	if reg.lastCmd.Email != "ada@example.com" || reg.lastCmd.DisplayName != "Ada Lovelace" {
		t.Errorf("command = %+v", reg.lastCmd)
	}
}

func TestRegisterUser_GeneratesACorrelationIDWhenTheClientSendsNone(t *testing.T) {
	reg := &fakeRegistrar{}
	rec := serve(t, reg, postUsers(`{"email":"ada@example.com","display_name":"Ada"}`))

	want := "11111111-1111-1111-1111-111111111111"
	if reg.lastCmd.Meta.CorrelationID != want {
		t.Errorf("CorrelationID = %q, want the generated %q", reg.lastCmd.Meta.CorrelationID, want)
	}
	if got := rec.Header().Get("X-Correlation-ID"); got != want {
		t.Errorf("response X-Correlation-ID = %q, want %q", got, want)
	}
}

func TestRegisterUser_PassesThroughCorrelationAndTrace(t *testing.T) {
	reg := &fakeRegistrar{}
	req := postUsers(`{"email":"ada@example.com","display_name":"Ada"}`)
	req.Header.Set("X-Correlation-ID", "corr-from-client")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	serve(t, reg, req)

	if reg.lastCmd.Meta.CorrelationID != "corr-from-client" {
		t.Errorf("CorrelationID = %q", reg.lastCmd.Meta.CorrelationID)
	}
	if reg.lastCmd.Meta.Traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("Traceparent = %q", reg.lastCmd.Meta.Traceparent)
	}
}

// One valid body, four use-case outcomes: this table is about error mapping and
// nothing else.
func TestRegisterUser_ErrorMapping(t *testing.T) {
	const validBody = `{"email":"ada@example.com","display_name":"Ada Lovelace"}`

	tests := []struct {
		name       string
		regErr     error
		wantStatus int
		wantCode   string
	}{
		{"invalid email", domain.ErrInvalidEmail, http.StatusBadRequest, "invalid_email"},
		{"invalid display name", domain.ErrInvalidDisplayName, http.StatusBadRequest, "invalid_display_name"},
		{"email taken", domain.ErrEmailTaken, http.StatusConflict, "email_taken"},
		{"unexpected failure", errors.New("connection reset by peer"), http.StatusInternalServerError, "internal_error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := &fakeRegistrar{err: tc.regErr}
			rec := serve(t, reg, postUsers(validBody))
			assertError(t, rec, tc.wantStatus, tc.wantCode)
			if reg.calls != 1 {
				t.Errorf("use case calls = %d, want 1", reg.calls)
			}
		})
	}
}

// Decoding failures are a different concern: they are rejected before the use
// case is reached at all.
func TestRegisterUser_RejectsMalformedBody(t *testing.T) {
	bodies := map[string]string{
		"not json":      `{`,
		"unknown field": `{"email":"ada@example.com","display_name":"Ada","admin":true}`,
		"oversized":     `{"email":"` + strings.Repeat("a", 64<<10) + `@example.com","display_name":"Ada"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			reg := &fakeRegistrar{}
			rec := serve(t, reg, postUsers(body))
			assertError(t, rec, http.StatusBadRequest, "malformed_body")
			if reg.calls != 0 {
				t.Errorf("use case calls = %d, want 0 — the body never decoded", reg.calls)
			}
		})
	}
}

func assertError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, wantStatus, rec.Body.String())
	}
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal %q: %v", rec.Body.String(), err)
	}
	if body.Code != wantCode {
		t.Errorf("code = %q, want %q", body.Code, wantCode)
	}
	if body.Message == "" {
		t.Error("message is empty; an error body with no message helps nobody")
	}
}

func TestRegisterUser_DoesNotLeakTheUnderlyingError(t *testing.T) {
	reg := &fakeRegistrar{err: errors.New("pq: password authentication failed for user \"oe\"")}
	rec := serve(t, reg, postUsers(`{"email":"ada@example.com","display_name":"Ada"}`))

	if strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("the response leaked a driver error: %s", rec.Body.String())
	}
}

func TestRouter_MethodAndRoute(t *testing.T) {
	reg := &fakeRegistrar{}

	rec := serve(t, reg, httptest.NewRequest(http.MethodGet, "/users", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /users = %d, want 405", rec.Code)
	}

	rec = serve(t, reg, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /nope = %d, want 404", rec.Code)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/accounts/presentation/http/`
Expected: FAIL — `undefined: NewHandler`, `undefined: NewRouter`.

- [ ] **Step 3: Write the DTOs**

`internal/accounts/presentation/http/dto.go`:

```go
// Package httpapi is the accounts context's HTTP presentation layer: the only
// place that knows what a request looks like, and the only place that turns a
// domain error into a status code.
//
// The directory is presentation/http per the spec's layout; the package is
// httpapi because a package named http that imports net/http is a riddle.
package httpapi

// registerRequest is the wire shape of a registration. It is unexported and
// separate from the command: the API's JSON contract and the use case's input
// change for different reasons, and a shared struct would couple them.
type registerRequest struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type registerResponse struct {
	UserID string `json:"user_id"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
```

- [ ] **Step 4: Write the handler**

`internal/accounts/presentation/http/handler.go`:

```go
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

// maxBodyBytes is generous for two short strings and small enough that a
// malicious body cannot make the process allocate.
const maxBodyBytes = 8 << 10

// Registrar is what this handler needs, declared where it is needed. The use
// case satisfies it without knowing this interface exists.
type Registrar interface {
	Execute(ctx context.Context, cmd application.RegisterUserCommand) (application.RegisterUserResult, error)
}

type Handler struct {
	register Registrar
	ids      application.IDGen
	log      *slog.Logger
}

func NewHandler(register Registrar, ids application.IDGen, log *slog.Logger) *Handler {
	return &Handler{register: register, ids: ids, log: log}
}

func (h *Handler) RegisterUser(w http.ResponseWriter, r *http.Request) {
	meta := h.metadata(r)
	w.Header().Set("X-Correlation-ID", meta.CorrelationID)

	var body registerRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		// Strict on input, tolerant on consumed events: those are different
		// contracts. A client sending a field this API does not know is a client
		// expecting something to happen that will not.
		h.fail(w, http.StatusBadRequest, "malformed_body",
			"request body must be a JSON object with the fields email and display_name")
		return
	}

	res, err := h.register.Execute(r.Context(), application.RegisterUserCommand{
		Email:       body.Email,
		DisplayName: body.DisplayName,
		Meta:        meta,
	})
	if err != nil {
		h.failFor(w, meta, err)
		return
	}

	w.Header().Set("Location", "/users/"+res.UserID.String())
	h.write(w, http.StatusCreated, registerResponse{UserID: res.UserID.String()})
}

// failFor is the whole error contract of this endpoint, in one place you can
// read without following the happy path around it.
func (h *Handler) failFor(w http.ResponseWriter, meta application.Metadata, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidEmail):
		h.fail(w, http.StatusBadRequest, "invalid_email", "email is not a valid address")
	case errors.Is(err, domain.ErrInvalidDisplayName):
		h.fail(w, http.StatusBadRequest, "invalid_display_name", "display name is empty or too long")
	case errors.Is(err, domain.ErrEmailTaken):
		h.fail(w, http.StatusConflict, "email_taken", "that email is already registered")
	default:
		// The client gets a code and nothing else. The operator gets everything.
		h.log.Error("register user failed", "correlation_id", meta.CorrelationID, "error", err)
		h.fail(w, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}
}

func (h *Handler) metadata(r *http.Request) application.Metadata {
	correlation := r.Header.Get("X-Correlation-ID")
	if correlation == "" {
		if id, err := h.ids.New(); err == nil {
			correlation = id.String()
		}
	}
	return application.Metadata{
		CorrelationID: correlation,
		Traceparent:   r.Header.Get("traceparent"),
	}
}

func (h *Handler) fail(w http.ResponseWriter, status int, code, message string) {
	h.write(w, status, errorResponse{Code: code, Message: message})
}

func (h *Handler) write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent; there is nowhere to report this but
		// the log.
		h.log.Error("write response", "error", err)
	}
}
```

- [ ] **Step 5: Write the router**

`internal/accounts/presentation/http/router.go`:

```go
package httpapi

import "net/http"

// NewRouter is the public surface of the api process: one command. cmd/api adds
// the shared liveness handler from platform/admin; readiness and the chaos hooks
// live on the admin listener, which is bound to loopback (spec §13.5).
func NewRouter(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", h.RegisterUser)
	return mux
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/accounts/presentation/http/ -v`
Expected: PASS, including every subtest of `TestRegisterUser_ErrorMapping`. Method patterns (`"POST /users"`) are what make the 405 assertion pass without any code of ours.

- [ ] **Step 7: Commit**

```bash
git add internal/accounts/presentation/http
git commit -m "feat(accounts): HTTP handler, DTOs and router with domain error mapping"
```

---

## Task 13: The api process, its admin listener, and the write path end to end

**Files:**
- Create: `internal/platform/admin/admin.go`
- Create: `cmd/api/main.go`
- Create: `README.md`
- Test: `internal/platform/admin/admin_test.go`
- Test: `internal/accounts/presentation/http/writepath_integration_test.go` (`//go:build integration`)
- Modify: `Makefile` (add `run-api`)

**Interfaces:**
- Consumes: everything from Tasks 2–12.
- Produces:
  - `func admin.Healthz() http.HandlerFunc` — the one liveness handler, mounted on both listeners
  - `func admin.Router(ready func(context.Context) error) *http.ServeMux`
  - the `api` binary

The admin listener serves liveness and readiness and nothing else. There is no
`/metrics`, no registry and no Prometheus dependency anywhere in this repository
(spec §13.3): the relay's per-pass `slog` line carries every signal spec §18 asks
an operator to watch, which keeps the reader's attention on the outbox instead of
on a telemetry stack. `Router` takes the readiness function directly rather than
an options struct — it has one dependency, and Plan 5 can widen the signature when
it mounts the chaos handlers.

- [ ] **Step 1: Add the dependency**

```bash
go get golang.org/x/sync@latest
```

- [ ] **Step 2: Write the failing admin test**

`internal/platform/admin/admin_test.go`:

```go
package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, mux *http.ServeMux, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthz_IsAlwaysOK(t *testing.T) {
	rec := get(t, Router(func(context.Context) error { return errors.New("db is down") }), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — liveness must not depend on a dependency", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	ok := get(t, Router(func(context.Context) error { return nil }), "/readyz")
	if ok.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", ok.Code)
	}

	bad := get(t, Router(func(context.Context) error { return errors.New("schema is behind") }), "/readyz")
	if bad.Code != http.StatusServiceUnavailable {
		t.Errorf("unready status = %d, want 503", bad.Code)
	}
	if !strings.Contains(bad.Body.String(), "schema is behind") {
		t.Errorf("body %q does not say why", bad.Body.String())
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./internal/platform/admin/`
Expected: FAIL — `undefined: Router`.

- [ ] **Step 4: Write the admin router**

`internal/platform/admin/admin.go`:

```go
// Package admin serves the operator surface — liveness and readiness — on a
// listener bound to loopback. It is separate from the public router because an
// unauthenticated surface has no business sharing a port with public traffic
// (spec §13.5). There is no /metrics: this project has no metrics system, and
// every operational signal is a field on a structured log line (spec §13.3).
package admin

import (
	"context"
	"net/http"
	"time"
)

// readyTimeout bounds the readiness check. A probe that can hang is a probe that
// makes an orchestrator's timeout the real behaviour.
const readyTimeout = 2 * time.Second

// Healthz is liveness: this process is running. It deliberately touches nothing
// else — a liveness probe that checks a dependency restarts a healthy process
// because something else broke.
//
// It is exported because spec §13.5 puts /healthz on the public listener too,
// and one operator contract deserves one implementation: cmd/api mounts this
// same handler on both muxes.
func Healthz() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// Router mounts the operator surface. ready reports whether this process can do
// its job: pools reachable, schema at the expected version. It must not check the
// broker — a broker outage is a condition this system tolerates, and failing
// readiness on it would turn a tolerated outage into a deployment incident
// (spec §13.4).
func Router(ready func(context.Context) error) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("GET /healthz", Healthz())

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := ready(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(err.Error() + "\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	return mux
}
```

- [ ] **Step 5: Run the admin test to verify it passes**

Run: `go test ./internal/platform/admin/ -v`
Expected: PASS for both tests.

- [ ] **Step 6: Write the api process**

`cmd/api/main.go`:

```go
// Command api is the accounts context's write path. It writes the user row and
// its outbox row in one transaction and returns; it never talks to a broker,
// which is why a broker outage cannot make it fail (spec §11.1).
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	accountspg "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/postgres"
	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	httpapi "github.com/AymanKastali/outboxexpress/internal/accounts/presentation/http"
	"github.com/AymanKastali/outboxexpress/internal/platform/admin"
	"github.com/AymanKastali/outboxexpress/internal/platform/clock"
	"github.com/AymanKastali/outboxexpress/internal/platform/config"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/logging"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
	"github.com/AymanKastali/outboxexpress/migrations"
)

const (
	maxDBConns      = 10
	shutdownTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI(os.Getenv)
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := platformpg.NewPool(ctx, cfg.AccountsDatabaseURL, maxDBConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	expectedSchema, err := migrations.Latest(migrations.Accounts)
	if err != nil {
		return err
	}

	// The wiring, in dependency order. This function is the only place in the
	// process where a concrete type meets a port.
	generator := ids.UUIDv7{}
	envelopes := application.NewCloudEventFactory(generator)
	uow := accountspg.NewUnitOfWork(pool, envelopes)
	notifier := wakeup.NewNotifier(pool, wakeup.ChannelOutboxNew, log)
	registerUser := application.NewRegisterUser(uow, clock.System{}, generator, notifier)
	handler := httpapi.NewHandler(registerUser, generator, log)

	publicMux := httpapi.NewRouter(handler)
	publicMux.Handle("GET /healthz", admin.Healthz()) // spec §13.5, one implementation

	public := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           publicMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	adminSrv := &http.Server{
		Addr: cfg.AdminAddr,
		Handler: admin.Router(func(ctx context.Context) error {
			return migrations.Ready(ctx, pool, expectedSchema)
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("api starting",
		"http", cfg.HTTPAddr, "admin", cfg.AdminAddr, "expected_schema", expectedSchema)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return listen(public, "public") })
	g.Go(func() error { return listen(adminSrv, "admin") })
	g.Go(func() error {
		<-gctx.Done()
		log.Info("api shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		// Drain in-flight requests first: a request that has committed but not
		// yet responded is a request whose client does not know it succeeded.
		// Arguments evaluate left to right, so public still drains first.
		return errors.Join(public.Shutdown(shutdownCtx), adminSrv.Shutdown(shutdownCtx))
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("api stopped")
	return nil
}

func listen(srv *http.Server, name string) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	return nil
}
```

- [ ] **Step 7: Add the Makefile recipe**

```makefile
run-api:
> go run ./cmd/api
```

- [ ] **Step 8: Write the failing end-to-end test**

`internal/accounts/presentation/http/writepath_integration_test.go`:

```go
//go:build integration

package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	accountspg "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/postgres"
	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	httpapi "github.com/AymanKastali/outboxexpress/internal/accounts/presentation/http"
	"github.com/AymanKastali/outboxexpress/internal/platform/clock"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

// The whole write path, wired as cmd/api wires it, against a real PostgreSQL.
func TestWritePath(t *testing.T) {
	_, pool := pgtest.Accounts(t)

	generator := ids.UUIDv7{}
	envelopes := application.NewCloudEventFactory(generator)
	useCase := application.NewRegisterUser(
		accountspg.NewUnitOfWork(pool, envelopes),
		clock.System{},
		generator,
		wakeup.NewNotifier(pool, wakeup.ChannelOutboxNew, slog.New(slog.DiscardHandler)),
	)
	srv := httptest.NewServer(httpapi.NewRouter(
		httpapi.NewHandler(useCase, generator, slog.New(slog.DiscardHandler))))
	defer srv.Close()

	// The body is read and closed before returning, so the client can reuse one
	// connection instead of opening one per request.
	type response struct {
		status   int
		location string
		body     string
	}
	post := func(t *testing.T, body string) response {
		t.Helper()
		resp, err := http.Post(srv.URL+"/users", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /users: %v", err)
		}
		defer resp.Body.Close()
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		return response{
			status:   resp.StatusCode,
			location: resp.Header.Get("Location"),
			body:     string(payload),
		}
	}

	// What only this altitude can prove: the wiring exists and speaks HTTP. The
	// column-to-envelope contract is asserted once, against the database, in
	// TestUnitOfWork_RegistrationWritesUserAndOutboxRowTogether — repeating it
	// here would break three tests in three packages for one envelope change.
	t.Run("a registration leaves exactly one pending event", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		resp := post(t, `{"email":"ada@example.com","display_name":"Ada Lovelace"}`)
		if resp.status != http.StatusCreated {
			t.Fatalf("status = %d, want 201", resp.status)
		}
		var created struct {
			UserID string `json:"user_id"`
		}
		if err := json.Unmarshal([]byte(resp.body), &created); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if want := "/users/" + created.UserID; resp.location != want {
			t.Errorf("Location = %q, want %q", resp.location, want)
		}

		users, outbox := pgtest.CountAccounts(t, pool)
		if users != 1 || outbox != 1 {
			t.Fatalf("users = %d, outbox = %d; want 1 and 1", users, outbox)
		}

		var status string
		if err := pool.QueryRow(context.Background(),
			`SELECT status FROM accounts.outbox`).Scan(&status); err != nil {
			t.Fatalf("select: %v", err)
		}
		if status != "pending" {
			t.Errorf("status = %q, want pending — no relay exists yet, and that is "+
				"precisely why the row must be safe where it is", status)
		}
	})

	t.Run("a duplicate email changes nothing", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		if resp := post(t, `{"email":"ada@example.com","display_name":"Ada"}`); resp.status != http.StatusCreated {
			t.Fatalf("first status = %d, want 201", resp.status)
		}
		if resp := post(t, `{"email":"ADA@example.com","display_name":"Ada Again"}`); resp.status != http.StatusConflict {
			t.Fatalf("second status = %d, want 409 — email normalisation must make "+
				"these the same address", resp.status)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 1 || outbox != 1 {
			t.Fatalf("users = %d, outbox = %d; want 1 and 1", users, outbox)
		}
	})

	t.Run("a rejected registration writes nothing", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		if resp := post(t, `{"email":"not-an-address","display_name":"Ada"}`); resp.status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.status)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0", users, outbox)
		}
	})

	t.Run("many registrations produce exactly as many events", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		const n = 50
		for i := 0; i < n; i++ {
			body := `{"email":"user` + strconv.Itoa(i) + `@example.com","display_name":"User"}`
			if resp := post(t, body); resp.status != http.StatusCreated {
				t.Fatalf("registration %d: status = %d", i, resp.status)
			}
		}
		users, outbox := pgtest.CountAccounts(t, pool)
		if users != n || outbox != n {
			t.Fatalf("users = %d, outbox = %d; want %d each — one event per state "+
				"change, no more and no fewer", users, outbox, n)
		}
		var distinct int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(DISTINCT event_id) FROM accounts.outbox`).Scan(&distinct); err != nil {
			t.Fatalf("count distinct: %v", err)
		}
		if distinct != n {
			t.Fatalf("distinct event_id = %d, want %d", distinct, n)
		}
	})
}

```

- [ ] **Step 9: Run it to verify it passes**

Run: `go test -tags=integration -count=1 ./internal/accounts/presentation/http/ -v`
Expected: PASS. This is Plan 1's deliverable in one test.

- [ ] **Step 10: Verify the process runs**

```bash
make db-up
set -a && . ./.env.example && set +a
go run ./cmd/migrate -context all -action up
go run ./cmd/api &
sleep 1
curl -isS -X POST localhost:8080/users \
  -H 'Content-Type: application/json' \
  -d '{"email":"ada@example.com","display_name":"Ada Lovelace"}'
curl -sS localhost:8081/readyz
docker compose -f deploy/docker-compose.yml exec -T postgres \
  psql -U oe -d oe_accounts -c \
  "SELECT id, event_type, status, attempts FROM accounts.outbox"
kill %1
```

Expected: `201 Created` with a `Location` header, `ready`, and one outbox row with `status = pending` and `attempts = 0`.

- [ ] **Step 11: Write the README**

`README.md`:

````markdown
# outboxexpress

A textbook implementation of the transactional outbox pattern: Go 1.27,
PostgreSQL 18.6, Apache Kafka, clean architecture in four layers across two
bounded contexts.

The design document is [`docs/superpowers/specs/2026-08-26-transactional-outbox-demo-design.md`](docs/superpowers/specs/2026-08-26-transactional-outbox-demo-design.md).
It cites the reference in [`docs/transactional-outbox.md`](docs/transactional-outbox.md) throughout.

## Status

| Plan | Scope | State |
|---|---|---|
| 1 | Accounts write path: `POST /users` writes the user and its event in one transaction | done |
| 2 | The relay: claim, publish to Kafka, mark published | not started |
| 3 | The consuming end: inbox, notifications, offsets | not started |
| 4 | The external side effect: consumer-side outbox and the email gateway | not started |
| 5 | Chaos endpoints, replay, the end-to-end invariant suite | not started |

Until Plan 2 lands, outbox rows accumulate as `pending`. That is not a bug being
tolerated — it is the guarantee: the event is durable the moment the
registration is, and nothing downstream can lose it.

## Running the write path

```bash
make db-up                                  # PostgreSQL 18.6, two databases
set -a && . ./.env.example && set +a
make migrate                                # both contexts, explicit, never at startup
make run-api                                # :8080 public, 127.0.0.1:8081 admin

curl -isS -X POST localhost:8080/users \
  -H 'Content-Type: application/json' \
  -d '{"email":"ada@example.com","display_name":"Ada Lovelace"}'
```

Then look at what the one transaction wrote:

```sql
SELECT u.email, o.event_type, o.status, o.attempts, o.payload->>'id' AS event_id
  FROM accounts.users u JOIN accounts.outbox o ON o.aggregate_id = u.id::text;
```

## Tests

```bash
make test              # unit: domain, use cases, envelope contract, handlers
make test-integration   # needs Docker: real PostgreSQL 18.6 via testcontainers
make lint
```

`make test-integration` starts throwaway containers. It does not skip when Docker
is missing: a suite that silently skips its only proof reports success for an
empty database.

## Layout

```
cmd/            one main per process
internal/
  accounts/     domain · application · infrastructure · presentation
  platform/     technical shared code, not a fifth layer
migrations/     plain SQL, one directory per context
deploy/         docker compose
```

The dependency rule is enforced by a test, not by a comment: see
`internal/arch/arch_test.go`.
````

- [ ] **Step 12: Run everything**

Run: `make lint && make test && make test-integration`
Expected: all green.

- [ ] **Step 13: Commit**

```bash
git add go.mod go.sum internal/platform/admin cmd/api Makefile README.md internal/accounts/presentation/http
git commit -m "feat(api): api process with admin listener, graceful shutdown and an end-to-end write path test"
```

---

## Task 14: The dependency rule as a test

**Files:**
- Create: `internal/arch/arch_test.go`

**Interfaces:**
- Consumes: every package written so far, as data.
- Produces: a test that fails when the layering is violated.

- [ ] **Step 1: Add the dependency**

```bash
go get golang.org/x/tools@latest
```

- [ ] **Step 2: Write the failing test**

`internal/arch/arch_test.go`:

```go
// Package arch_test enforces the dependency rule of spec §6.1 by walking the
// import graph. A boundary that is not checked is a comment.
package arch_test

import (
	"strings"
	"sync"
	"testing"

	"golang.org/x/tools/go/packages"
)

const module = "github.com/AymanKastali/outboxexpress"

// The import graph is read-only and identical for every rule, so it is loaded
// once per test binary rather than once per test. packages.Load takes seconds.
var loadOnce = sync.OnceValues(func() ([]*packages.Package, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
		Dir:  "../..", // the module root
	}
	return packages.Load(cfg, "./...")
})

func load(t *testing.T) []*packages.Package {
	t.Helper()
	pkgs, err := loadOnce()
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("no packages loaded")
	}
	if n := packages.PrintErrors(pkgs); n > 0 {
		t.Fatalf("%d packages failed to load", n)
	}
	return pkgs
}

// stdlib recognises a standard library import path: it has no dot in its first
// segment, because every module path is a domain name.
func stdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func TestDomainImportsOnlyStdlibAndUUID(t *testing.T) {
	const allowed = "github.com/google/uuid"
	for _, pkg := range load(t) {
		if !strings.Contains(pkg.PkgPath, "/domain") {
			continue
		}
		for imported := range pkg.Imports {
			if stdlib(imported) || imported == allowed {
				continue
			}
			t.Errorf("%s imports %s; the domain may import only the standard "+
				"library and %s", pkg.PkgPath, imported, allowed)
		}
	}
}

func TestApplicationImportsNothingConcrete(t *testing.T) {
	for _, pkg := range load(t) {
		if !strings.Contains(pkg.PkgPath, "/application") {
			continue
		}
		context, ok := contextOf(pkg.PkgPath)
		if !ok {
			t.Fatalf("cannot determine the bounded context of %s", pkg.PkgPath)
		}
		// This allowlist is also what keeps platform out of the application
		// layer: every platform package except messaging falls through to the
		// error below.
		allowed := map[string]bool{}
		allowed["github.com/google/uuid"] = true
		allowed[module+"/internal/platform/messaging"] = true
		allowed[module+"/internal/"+context+"/domain"] = true
		for imported := range pkg.Imports {
			if stdlib(imported) || allowed[imported] {
				continue
			}
			t.Errorf("%s imports %s; the application layer may import its own "+
				"domain, platform/messaging and the standard library — no driver, "+
				"no SQL, no HTTP, no Kafka, no logger", pkg.PkgPath, imported)
		}
	}
}

func TestPresentationAndInfrastructureAreSiblings(t *testing.T) {
	for _, pkg := range load(t) {
		for imported := range pkg.Imports {
			if strings.Contains(pkg.PkgPath, "/presentation") &&
				strings.Contains(imported, "/infrastructure") {
				t.Errorf("%s imports %s; presentation and infrastructure share the "+
					"outer ring and neither may import the other", pkg.PkgPath, imported)
			}
			if strings.Contains(pkg.PkgPath, "/infrastructure") &&
				strings.Contains(imported, "/presentation") {
				t.Errorf("%s imports %s; same rule, other direction", pkg.PkgPath, imported)
			}
		}
	}
}

func TestBoundedContextsDoNotKnowEachOther(t *testing.T) {
	for _, pkg := range load(t) {
		mine, ok := contextOf(pkg.PkgPath)
		if !ok {
			continue
		}
		for imported := range pkg.Imports {
			theirs, ok := contextOf(imported)
			if !ok || theirs == mine {
				continue
			}
			t.Errorf("%s imports %s; the two contexts communicate through a topic, "+
				"not through Go types", pkg.PkgPath, imported)
		}
	}
}

func TestNoProcessWiresBothContexts(t *testing.T) {
	for _, pkg := range load(t) {
		if !strings.HasPrefix(pkg.PkgPath, module+"/cmd/") {
			continue
		}
		if strings.HasPrefix(pkg.PkgPath, module+"/cmd/migrate") {
			continue // the migrator owns both schemas by definition
		}
		seen := map[string]bool{}
		for imported := range pkg.Imports {
			if c, ok := contextOf(imported); ok {
				seen[c] = true
			}
		}
		if len(seen) > 1 {
			t.Errorf("%s wires more than one bounded context (%v); a process that "+
				"holds both pools can write a cross-context transaction", pkg.PkgPath, seen)
		}
	}
}

// contextOf returns the bounded context a package belongs to, if any.
func contextOf(path string) (string, bool) {
	for _, c := range []string{"accounts", "notifications"} {
		if strings.HasPrefix(path, module+"/internal/"+c+"/") ||
			path == module+"/internal/"+c {
			return c, true
		}
	}
	return "", false
}
```

- [ ] **Step 3: Run it**

Run: `go test ./internal/arch/ -v`
Expected: PASS. If it fails, the layering is wrong — fix the import, never the test. `packages.Load` takes a few seconds, paid once for the whole file.

- [ ] **Step 4: Prove the test can fail**

Temporarily add `import _ "net/http"` to `internal/accounts/domain/user.go`, then:

Run: `go test ./internal/arch/ -run TestDomainImportsOnlyStdlibAndUUID -v`
Expected: still PASS — `net/http` is standard library, so this is the wrong probe. Instead add `import _ "github.com/jackc/pgx/v5"` to the domain and run it again.
Expected: FAIL, naming `internal/accounts/domain` and `github.com/jackc/pgx/v5`.

Then remove the import and confirm the test passes again. A guard nobody has watched fail is a guard nobody knows works.

- [ ] **Step 5: Run the whole suite**

Run: `make lint && make test && make test-integration`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/arch
git commit -m "test(arch): enforce the dependency rule and the context boundary by walking imports"
```

---

## Done means

Plan 1 is complete when all of the following are true:

1. `make lint && make test && make test-integration` is green from a clean checkout with Docker running.
2. `POST /users` returns 201 and leaves exactly one `accounts.users` row and one `pending` `accounts.outbox` row whose `payload->>'id'` equals its `event_id` column.
3. A failure anywhere in the transaction leaves neither row — proven by `TestUnitOfWork_NeitherRowSurvivesAFailure`, not by inspection.
4. No `application` package imports anything concrete — proven by `internal/arch`. That no *use case* mentions the outbox is a separate property, held structurally rather than by that test: `application.Work` has no outbox field, so `RegisterUser`'s closure has nothing to reach for. An import walk could not see it either way, because it would be an intra-package reference.
5. `make migrate` is the only thing that touches the schema; no process migrates at startup.
6. The envelope on the wire matches spec §9.1 byte for byte before it reaches JSONB — proven by `TestCloudEventFactory_ProducesTheExactWireFormat`.

## Notes for Plan 2

Things this plan deliberately left for the relay, so that Plan 2 does not have to guess:

- `platform/messaging` exists and holds the CloudEvents encoder every context shares. `messaging.Message` — the transport envelope, the only type crossing the context boundary (spec §6.5) — is what the Kafka publisher adds to it.
- `platformpg.Queryer` has no `Query` method yet; the claiming repository's `SELECT … FOR UPDATE SKIP LOCKED` is what adds it.
- `platformpg.WithTx` owns the begin/rollback/commit rules, including returning `fn`'s error unwrapped so domain sentinels survive. The relay's claim-publish-mark transaction uses it rather than repeating them.
- `application.OutboxAppender` is named for a role, not a table. Declare a separate claiming port — claim, mark published, mark failed — rather than widening it, and declare the relay's own transaction boundary rather than adding an outbox to `application.Work`: the moment `Work` exposes one, `RegisterUser` can append by hand and Plan 1's headline invariant quietly retires with no failing test.
- There is no `wakeup.Discard`. `RELAY_USE_NOTIFY` is a relay variable, so the no-op belongs to the process that reads the flag.
- `accounts.outbox.status`, `attempts`, `available_at`, `last_error` and `published_at` are written by nothing so far. The relay owns all five.
- Headers stored on the row are `correlation_id` and `traceparent` only. The relay composes `ce_id`, `ce_type`, `ce_specversion`, `schema_version` and `content-type` from the row's columns at publish time (spec §9.2, §11.2).
- `wakeup.ChannelOutboxNew` is `outbox_new`. The relay's `LISTEN` must match, and `TestNotifier_DeliversToAListener` is where that agreement is checked.
- `platformpg.Queryer` is satisfied by both `*pgxpool.Pool` and `pgx.Tx`, which is what lets the relay's claiming repository run inside the claiming transaction (spec D6).
- `pgtest` starts one container per test binary holding both databases, and hands out truncated pools. Add `pgtest.TruncateNotifications` alongside the existing helpers rather than starting more containers.
- There is no metrics package and no `/metrics` endpoint, by design (spec §13.3). The relay's observability is one structured `slog` line per pass; its `backlog`, `oldest_pending_age_ms`, `failed_rows` and `stuck_rows` fields come from a single stats query issued on the transaction the pass has already opened, and its `claimed`/`published`/`*_failures` fields come straight off `PublishResult`. Do not add a registry.
- JSONB normalises stored JSON: it sorts object keys and drops insignificant whitespace, so the bytes the relay reads back are semantically identical to but not byte-identical with what the factory produced. This is inherent in the spec's choice of `JSONB` over `BYTEA` (§4, D10), which was made for incident readability. Consumers are tolerant readers and key order is not semantic in JSON, so nothing downstream depends on it — but do not write a relay test that asserts byte equality with the factory's output.
