# Compose Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the API against Postgres under `docker compose`, migrated and reachable.

**Architecture:** One image serves both roles — a one-shot `migrate` service runs `alembic upgrade head` and exits, and the `api` service starts only once that has succeeded, so the schema is never a race. The image is a two-stage `uv sync --locked --no-dev --no-editable` build: the runtime layer takes the resolved venv alone, and carries no source tree and none of the tooling that produced it.

The file is `docker-compose.yml` rather than the Compose spec's current canonical `compose.yaml`, because spec §10 names it — the design document is the authority here, and a rename is a spec change, not a plan change.

### Changed during execution

The plan as approved published both ports on loopback. Both collided with services already running on the development machine, which turned a design assumption into a visible bug:

- **Postgres is no longer published.** Its only clients — `migrate`, `api`, and later the relay and notifier — are on the compose network already, so a host port bought nothing and guaranteed a collision with any other Postgres. `docker compose exec postgres psql -U outbox -d outbox` replaces it.
- **The API's host port is a variable**, `${OUTBOX_HOST_PORT:-8000}`, surfaced as `make up HOST_PORT=8080`. The container port stays 8000, so nothing inside the network changes.

**Tech Stack:** Docker 29 · Compose v5 · `python:3.14-slim-bookworm` · `uv` 0.10 · `postgres:18` · GNU Make

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§10 layout, §13 M1)

## Global Constraints

- **Postgres only.** No Kafka, no kafka-ui, no Mailpit service — M1 exists to show the API's guarantee holds with no broker in the project (spec §13).
- **Nothing reaches Postgres before it is migrated** — the `api` waits on a completed `migrate`, which waits on a healthy `postgres`.
- **Settings come from the environment, never from a baked `.env`** — `OUTBOX_`-prefixed, matching `shared/config.py`.
- **The suite stays at 36 passed** and `ruff check .` / `pyright` stay clean. This plan adds no Python.

### On TDD

This plan is packaging and configuration — the exception `superpowers:test-driven-development` names, and a pytest that drove `docker compose` would be slow, flaky, and a second, worse copy of the testcontainers suite. The assertions are still executable and still written before the thing they check: `scripts/smoke.sh` (Task 3) proves healthz, byte-for-byte idempotent replay, read-back, and 404 against the **real running stack**. Every step below states its expected output, and a step whose output differs is a failure to stop on, not to reinterpret.

## Out of Scope

- Kafka, the relay, the notifier, and the `chaos-*` targets (M2 and M3)
- A production image — no TLS, no secret management, no multi-worker uvicorn tuning
- CI configuration; the stack is run by hand and by `make`
- Publishing the image anywhere

## Plan Queue

This work is split across small plans. **Only plan 5 is written.**

1. ✅ **Shipped** — shared foundations: settings, logging, models, migration, event contract
2. ✅ **Shipped** — write an order and its event atomically — `2026-08-19-atomic-write-path.md`
3. ✅ **Shipped** — serve the write path as idempotent `POST /orders` — `2026-08-19-http-write-endpoint.md`
4. ✅ **Shipped** — read an order back over HTTP — `2026-08-19-read-order-endpoint.md`
5. **[this plan]** Run the Postgres-only stack under compose — `2026-08-19-compose-stack.md`

This closes **M1**. M2 (relay and Kafka) opens its own queue, written against the code that exists once this ships.

---

### Task 1: The API image

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Consumes: `pyproject.toml` (the `outbox-api` console script), `uv.lock`, `alembic.ini`, `migrations/`, `src/`
- Produces: an image whose `PATH` leads with `/app/.venv/bin`, so both `outbox-api` and `alembic` are callable as bare commands. Task 2 relies on exactly that.

- [x] **Step 1: Write `.dockerignore`**

The build context decides what can accidentally end up in the image. `.venv/`
matters most: it is a host-built virtualenv with absolute paths into
`/home/...`, and copying it over the one `uv` builds would produce an image
that works on this machine and nowhere else.

```
.git/
.venv/
.pytest_cache/
.ruff_cache/
__pycache__/
*.py[cod]
.superpowers/
.vscode/
docs/
tests/
.env
```

- [x] **Step 2: Write the `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1

# --- build: resolve the locked dependency set into a self-contained venv ---
FROM python:3.14-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.10.0 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependencies before source: they change far less often than a route does, so
# editing the API does not re-resolve the lock file. The lock and manifest are
# bind-mounted rather than copied so they leave no layer behind.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY pyproject.toml uv.lock ./
COPY src ./src

# --no-editable installs the package into the venv rather than linking back to
# /app/src, so the runtime stage can take the venv alone and owns no source tree.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# --- run: the interpreter and the venv, without the tooling that produced them ---
FROM python:3.14-slim-bookworm

# The container writes nothing, so it has no reason to own the filesystem it runs on.
RUN useradd --create-home --uid 1000 outbox

# `.venv/bin` first, so `outbox-api` and `alembic` are bare commands -- no `uv run`
# in the runtime image, which has no uv.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --from=builder --chown=outbox:outbox /app/.venv /app/.venv

# Alembic reads these off disk at runtime. They are data the venv cannot carry,
# which is why they are copied here and not in the builder.
COPY --chown=outbox:outbox alembic.ini ./
COPY --chown=outbox:outbox migrations ./migrations

USER outbox
EXPOSE 8000
CMD ["outbox-api"]
```

- [x] **Step 3: Build the image**

Run: `docker build -t outboxexpress-api .`
Expected: build succeeds. Every wheel resolves — the lock file pins the same
platform this host resolved it on.

- [x] **Step 4: Verify both entrypoints and the user**

Run:

```bash
docker run --rm outboxexpress-api alembic --version
docker run --rm outboxexpress-api python -c "import outboxexpress.api.main; print('api ok')"
docker run --rm outboxexpress-api id -un
```

Expected: an Alembic version, `api ok`, and `outbox`. If the third prints
`root`, the `USER` line is missing or misplaced.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "build: package the api and its migrations as one image"
```

---

### Task 2: The compose stack

**Files:**
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: the image from Task 1 (both the default `CMD` and an `alembic upgrade head` override)
- Produces: services `postgres`, `migrate`, `api`; the API published on `127.0.0.1:${OUTBOX_HOST_PORT:-8000}`. Postgres is not published at all. Task 3 drives exactly these.

- [x] **Step 1: Write `docker-compose.yml`**

Three details below are the ones that bite, and all three are commented in the
file rather than in this plan, because the file is where the next reader meets
them.

```yaml
name: outboxexpress

services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: outbox
      POSTGRES_PASSWORD: outbox
      POSTGRES_DB: outbox
    # Deliberately unpublished. Every client of this database -- migrate, api, and
    # later the relay and notifier -- is on this network already, so a host port
    # would add reachability nobody needs and collide with any other Postgres on
    # the machine. For a psql prompt: docker compose exec postgres psql -U outbox -d outbox
    volumes:
      # Postgres 18 moved PGDATA to /var/lib/postgresql/18/docker and declares its
      # volume one level up. The /var/lib/postgresql/data mount inherited from every
      # Postgres 16 example persists nothing here, and does so silently.
      - postgres-data:/var/lib/postgresql
    healthcheck:
      # -h forces TCP. During initdb the entrypoint runs a temporary server with
      # listen_addresses='' that a socket-based pg_isready reports as ready -- so the
      # bare form goes healthy mid-initialization and migrate races the real startup.
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U outbox -d outbox"]
      interval: 2s
      timeout: 3s
      retries: 15

  migrate:
    build: .
    command: ["alembic", "upgrade", "head"]
    environment:
      OUTBOX_DATABASE_URL: postgresql+psycopg://outbox:outbox@postgres:5432/outbox
    depends_on:
      postgres:
        condition: service_healthy

  api:
    build: .
    environment:
      OUTBOX_DATABASE_URL: postgresql+psycopg://outbox:outbox@postgres:5432/outbox
      # The 127.0.0.1 default binds the container's own loopback, which the published
      # port cannot reach. In a container the interface is the boundary, not the guard.
      OUTBOX_API_HOST: 0.0.0.0
    ports:
      # Loopback, not 0.0.0.0: a published port is a DNAT rule that sits in front of
      # the host firewall, so the short form would offer this to the whole network.
      # The host side is a variable because 8000 is a popular port to already own.
      - "127.0.0.1:${OUTBOX_HOST_PORT:-8000}:8000"
    depends_on:
      # Not "postgres is up" -- "the schema is present". Ordering the migration ahead
      # of the first request is the whole reason this service exists.
      migrate:
        condition: service_completed_successfully
    healthcheck:
      test:
        ["CMD", "python", "-c",
         "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s

volumes:
  postgres-data:
```

- [x] **Step 2: Validate the file before running it**

Run: `docker compose config --quiet`
Expected: no output. A non-empty result means a syntax or reference error, and
it is cheaper to see here than in a half-started stack.

- [x] **Step 3: Bring the stack up**

Run: `docker compose up --build --wait`
Expected: `postgres` becomes healthy, `migrate` runs and exits `0`, `api`
becomes healthy, and the command returns `0`. `--wait` implies detached mode.

(`up --wait` returning non-zero on a one-shot service that exits cleanly is
docker/compose#10596, and it bites only when nothing depends on that service.
Here `api` does. Probed against Compose v5.3.1 on this host before writing:
exit `0`.)

- [x] **Step 4: Verify the stack answers**

Run:

```bash
curl -fsS http://127.0.0.1:8000/healthz
docker compose logs migrate | tail -5
```

Expected: `{"status":"ok"}`, and the migration log ending in
`Running upgrade  -> 0001, initial schema`.

- [x] **Step 5: Verify the migration is idempotent across restarts**

Run: `docker compose up --build --wait` a second time.
Expected: `migrate` exits `0` again with no upgrade to run — the volume kept
the schema, and re-running the stack is not a special case.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml
git commit -m "build: run postgres and the migrated api under compose"
```

---

### Task 3: The dev loop

**Files:**
- Create: `Makefile`
- Create: `scripts/smoke.sh`
- Create: `README.md`

**Interfaces:**
- Consumes: the services from Task 2 by name; the endpoints `GET /healthz`, `POST /orders`, `GET /orders/{id}`
- Produces: `make up|down|logs|ps|smoke|test|lint|clean`, and `scripts/smoke.sh` runnable on its own against any `BASE_URL`

- [x] **Step 1: Write the smoke check**

This is the executable assertion for the whole plan. It is written before the
Makefile that calls it, and it exercises the three things M1 claims: the API is
healthy without a broker, a replayed key returns the identical bytes, and a
committed order reads back.

Create `scripts/smoke.sh`:

```bash
#!/usr/bin/env bash
# End-to-end proof against a running stack: one order in, the same order out.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
KEY="smoke-$(date +%s%N)"
BODY='{"customer_email":"a@b.com","item_sku":"SKU-1","quantity":2}'

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

post() {
  curl -fsS -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $KEY" \
    -d "$BODY"
}

health=$(curl -fsS "$BASE_URL/healthz")
[ "$health" = '{"status":"ok"}' ] || fail "healthz returned $health"
echo "ok   healthz"

created=$(post)
replayed=$(post)
# Byte-for-byte, not field-by-field: a replay hands back the stored response, and
# comparing parsed fields would not notice if it were rebuilt.
if [ "$created" != "$replayed" ]; then
  printf 'FAIL: replay differed\n  first:  %s\n  second: %s\n' "$created" "$replayed" >&2
  exit 1
fi
echo "ok   replayed key returns the stored response byte for byte"

order_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])' <<<"$created")
fetched=$(curl -fsS "$BASE_URL/orders/$order_id")
# Both values go in through the environment: a heredoc already owns stdin here.
ORDER_ID="$order_id" FETCHED="$fetched" python3 - <<'PY' || fail "read-back mismatch"
import json, os

body = json.loads(os.environ["FETCHED"])
assert body["order_id"] == os.environ["ORDER_ID"], body
assert body["status"] == "CREATED", body
PY
echo "ok   order reads back as CREATED"

missing="00000000-0000-7000-8000-000000000000"
code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/orders/$missing")
[ "$code" = "404" ] || fail "unknown order returned $code, not 404"
echo "ok   unknown order is 404"

echo "smoke passed"
```

Make it executable: `chmod +x scripts/smoke.sh`

- [x] **Step 2: Run the smoke check against the stack from Task 2**

Run: `./scripts/smoke.sh`
Expected: four `ok` lines and `smoke passed`.

- [x] **Step 3: Write the `Makefile`**

Only the M1 loop. The `chaos-*` targets in spec §9.4 need a relay and a broker,
and belong to the plans that build them.

```makefile
.DEFAULT_GOAL := help
COMPOSE := docker compose

# The host port only. Inside the network the API is always on 8000. Override when
# something already owns 8000: `make up HOST_PORT=8080`.
HOST_PORT ?= 8000
export OUTBOX_HOST_PORT := $(HOST_PORT)

.PHONY: help up down logs ps smoke test lint clean

help:  ## List the targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk -F':.*?## ' '{printf "  \033[36m%-7s\033[0m %s\n", $$1, $$2}'

up:  ## Build and start Postgres and the migrated API
	$(COMPOSE) up --build --wait

down:  ## Stop the stack, keeping the database volume
	$(COMPOSE) down

logs:  ## Follow the API log
	$(COMPOSE) logs -f api

ps:  ## Show service state and health
	$(COMPOSE) ps

smoke:  ## Prove the running stack end to end
	BASE_URL=http://127.0.0.1:$(HOST_PORT) ./scripts/smoke.sh

test:  ## Run the suite (testcontainers, independent of the compose stack)
	uv run pytest

lint:  ## Lint and type-check
	uv run ruff check . && uv run pyright

clean:  ## Stop the stack and delete the database volume
	$(COMPOSE) down --volumes
```

- [x] **Step 4: Verify the targets, from nothing**

Run:

```bash
make clean
make up
make smoke
```

Expected: `clean` removes the volume, `up` rebuilds and waits, and `smoke`
passes against a database that was empty seconds earlier — which is the real
test of the `migrate` ordering in Task 2.

- [x] **Step 5: Write the `README.md`**

````markdown
# outboxexpress

A transactional outbox study: an order and the event announcing it commit in one
transaction, or neither does.

**Design:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md`

## Run it

```bash
make up      # Postgres + the migrated API on http://127.0.0.1:8000
make smoke   # prove it end to end
make down    # stop, keeping the data
```

`make` on its own lists every target.

If something already owns port 8000, move the host side — the API is always on
8000 inside the network, so only the published port changes:

```bash
make up HOST_PORT=8080
make smoke HOST_PORT=8080
```

Postgres is not published at all. Every client of it runs on the compose network,
so for an ad-hoc prompt use `docker compose exec postgres psql -U outbox -d outbox`.

## The API

| | |
| --- | --- |
| `POST /orders` | Creates an order and its outbox event in one transaction. Requires an `Idempotency-Key` header; replaying one returns the stored response byte for byte. |
| `GET /orders/{id}` | The committed order, or `404`. |
| `GET /healthz` | Postgres reachable. Deliberately says nothing about a broker. |

## Develop

```bash
uv sync          # Python 3.14
make test        # the suite; brings up its own Postgres via testcontainers
make lint        # ruff + pyright (strict)
```

The suite does not use the compose stack, and the compose stack does not use the
suite's database. Either runs without the other.

## Status

M1 — the write path, the read path, and the compose stack. No Kafka in the
project yet, which is the point: the API's guarantee does not depend on a broker.
Milestones M2 (relay) and M3 (consumer, chaos drills) are in §13 of the design.
````

- [x] **Step 6: Run the full suite and the linters**

Run: `uv run ruff check . && uv run pyright && uv run pytest`
Expected: clean, 36 passed. This plan adds no Python; a change here means
something was touched that should not have been.

- [ ] **Step 7: Commit**

```bash
git add Makefile scripts/smoke.sh README.md
git commit -m "build: add the dev loop, a smoke check, and a README"
```
