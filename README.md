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
