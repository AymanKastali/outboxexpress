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
