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
| 2 | The relay: claim, publish to Kafka, mark published | done |
| 3 | The consuming end: inbox, notifications, offsets | not started |
| 4 | The external side effect: consumer-side outbox and the email gateway | not started |
| 5 | Chaos endpoints, replay, the end-to-end invariant suite | not started |

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

## Running the relay

```bash
make up                                     # PostgreSQL 18.6, Kafka 4.3.1, topics
set -a && . ./.env.example && set +a
make migrate
make run-api                                # in one terminal
make run-relay                              # in another

curl -isS -X POST localhost:8080/users \
  -H 'Content-Type: application/json' \
  -d '{"email":"ada@example.com","display_name":"Ada Lovelace"}'
```

The relay logs one line per pass, and that line is the whole of this project's
observability — there is no `/metrics` endpoint and no metrics library, by design
(spec §13.3):

```json
{"level":"INFO","msg":"relay pass","claimed":1,"published":1,
 "transient_failures":0,"permanent_failures":0,"batch_ms":12,
 "backlog":0,"oldest_pending_age_ms":0,"failed_rows":0,"stuck_rows":0,
 "notify_queue_usage":0}
```

`oldest_pending_age_ms` is the one to watch. §13.3 calls it "the best single
signal — a stuck relay shows here first."

The four backlog fields are read on the pool after the pass commits, so they can
fail without taking the pass with them. When they do, the line carries
`stats_error` instead of the four numbers — a zero backlog and an unreadable
backlog look the same on a dashboard, and only one of them is good news.

Two lines are worth recognising because they are the only ones that mean
duplicates were created:

```json
{"level":"ERROR","msg":"relay pass rolled back; every row it claimed is still pending",
 "claimed":40,"republish_on_retry":12,"batch_ms":3104}
{"level":"WARN","msg":"relay loop stopped, but the drain grace expired with a pass still in flight; ..."}
```

`republish_on_retry` is how many messages the broker durably has whose marks were
undone. Everything else about a rolled-back pass is safe — the rows are still
pending and nothing is lost — but those will arrive twice, which is why the
consumer's inbox is not optional.

Then read the event off the topic:

```bash
docker compose -f deploy/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic accounts.user.v1 --from-beginning --property print.headers=true
```

### Getting a parked row moving again

A row reaches `status = 'failed'` only on a permanent error — an unmapped
aggregate type, a record the broker will never accept — so it means a human has to
decide something (§12.1, D8). `RELAY_MAX_ATTEMPTS` never parks a row; it only
raises the `stuck_rows` warning.

There is no replay endpoint yet: §12.3 groups the three replay paths and Plan 5
builds them together. Until then the state change is four lines of SQL, and it is
worth knowing them because this plan is the one that introduces the state:

```sql
-- Look first. last_error is why the row is here.
SELECT id, event_id, aggregate_id, attempts, last_error
  FROM accounts.outbox WHERE status = 'failed' ORDER BY id;

-- Then, once the cause is fixed (a topic created, a routing table corrected):
UPDATE accounts.outbox
   SET status = 'pending', attempts = 0, last_error = NULL, available_at = now()
 WHERE id = $1;
```

`event_id` is deliberately untouched: it was minted once at insert (§8.1) and the
consumer's inbox deduplicates on it, so a replayed row is recognised as the same
event rather than delivered twice. Regenerating it is, per §4, "the single most
common way to break the pattern while appearing to implement it correctly".

### Three things this demo cannot give you

**Durability.** One broker means replication factor 1, so `acks=all` is satisfied
by a single copy. §7's durability floor is `acks=all` *with*
`min.insync.replicas >= 2` and a replication factor of 3 or more, and a
single-broker demo cannot provide it. The producer is configured for the floor
(`RequiredAcks(AllISRAcks())`, idempotent writes) — the broker is what falls short.

**Ordering under more than one relay.** `SKIP LOCKED` makes a second relay
*safe*: no row is ever published twice concurrently, proven by
`TestClaimPending_TwoTransactionsClaimDisjointRows`. It does not make it
*ordered* — two relays can publish two events for the same user at the same
moment, and Kafka may accept them in either order. Since per-aggregate ordering
is a stated guarantee here, the relay runs single active with a hot standby
(§12.4, D7). The alternatives, and what they cost:

| Instead | Cost |
|---|---|
| Shard relays by `hash(aggregate_id)` | Real parallelism, but each relay needs a stable shard assignment and a rebalance protocol — a coordination problem the outbox does not otherwise have. |
| Accept version-stamped events with last-writer-wins consumers | Full parallelism, and consumers must carry `version` and discard stale events. §9's pragmatic alternative; it moves the guarantee to the consuming end. |

**A window you can close.** Between the broker's ack and the mark, a *crash*
leaves the message published and the row pending, and the next pass publishes it
again. That cannot be closed: closing it would need an atomic commit across Kafka
and PostgreSQL, which is the problem the outbox exists to avoid.

A restart is a different matter, and this demo does close that one. `SIGTERM` ends
the pass loop between passes but leaves the pass in flight running for
`RELAY_DRAIN_GRACE`, so it finishes its produce, marks its rows and commits —
`http.Server.Shutdown`'s rule, applied to a loop. Without it every rolling deploy
republished whatever the current batch had already acked, once per pod. Try it:
`make run-relay`, register a few users, and Ctrl-C mid-pass.

**A wakeup you can rely on.** `NOTIFY` is an optimisation and never the delivery
path (§13.1). Notifications are not durable and are not delivered to a listener
that was disconnected when they were sent; the poll loop with its timeout is the
source of truth. It can also *hurt*: PostgreSQL's notify queue is shared and
finite, and a session holding `LISTEN` inside a long transaction stops it being
trimmed — when it fills, transactions calling `NOTIFY` fail at commit, so a
wedged relay can start failing registrations. This is why the listening
connection never opens a transaction, why `notify_queue_usage` is on every pass
line and warned above 25%, and why `RELAY_USE_NOTIFY=false` exists. Setting it
false and accepting `RELAY_IDLE_MAX` of publish latency is a legitimate
production choice.

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
