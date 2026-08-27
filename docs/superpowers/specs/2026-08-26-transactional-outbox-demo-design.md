# Transactional Outbox — Reference Implementation Design

| | |
|---|---|
| **Status** | Approved for planning |
| **Date** | 2026-08-26 |
| **Reference** | [`docs/transactional-outbox.md`](../../transactional-outbox.md) |
| **Citations** | `§n` (whole number) cites the reference. `§n.m` (with a decimal) cites a section of *this* document. |
| **Stack** | Go 1.27.0 · PostgreSQL 18.6 · Apache Kafka 4.3.1 (KRaft) |
| **Module** | `github.com/kptac/outboxexpress` |
| **Goal** | A textbook-faithful reference implementation. Where fidelity and convenience conflict, fidelity wins. |

---

## 1. Purpose

A user registers. That registration and the intent to announce it commit in one
database transaction. A relay publishes the event to a broker. A notification
service consumes it exactly once in effect, and sends a welcome email.

The domain is deliberately trivial so that nothing distracts from the mechanism.
Everything else is implemented as the literature prescribes, with no shortcut
that would make a guarantee untrue.

**The governing rule of this design.** The pattern exists because *"the database
and the message broker are two independent systems. There is no transaction that
spans them"* (§1). Any implementation that removes that fact — an in-process
queue, a broker living in the producer's database, a relay embedded in the API —
makes the premise false and the guarantees undemonstrable. This design therefore
keeps the two systems genuinely separate and pays the infrastructure cost.

## 2. Textbook conformance map

Every normative section of the reference and where this design satisfies it.

| § | Requirement | Where |
|---|---|---|
| §4 | Outbox table schema, partial index, `BIGSERIAL` ordering authority | §8.1 |
| §4 | `event_id` generated once at insert, never regenerated | §8.1, §11.1 |
| §4 | Claim by state, never by cursor | §11.2 |
| §4 | One outbox per service database | Bounded contexts (§7.0) |
| §4 | Mark-and-purge, not delete-on-publish | §13.2 |
| §5 | Outbox insert in the business transaction, same connection | §11.1 |
| §5 | Nothing external inside the transaction | §11.1 |
| §5 | No after-commit hook as the delivery path | §11.1, §13.1 |
| §5 | Unit of work drains aggregate events during commit | §6.4, §11.1 |
| §6 | Event-carried state transfer, aggregate version, CloudEvents envelope | Message contract (§9.1) |
| §6 | Tolerant reader, additive-only evolution | §9.3 |
| §7 | Relay as its own process | Topology (§3.0) |
| §7 | `FOR UPDATE SKIP LOCKED` claim, publish in `id` order | §11.2 |
| §7 | Await durable acknowledgement before marking | §10.1, §11.2 |
| §7 | Transient vs permanent classification; a broker outage is not the message's fault | §12.1 |
| §7 | Exponential backoff with jitter via `attempts` / `available_at` | §12.1 |
| §7 | `LISTEN`/`NOTIFY` as optimisation only, with the notify-queue hazard | §13.1 |
| §8 | Single active relay for ordering; `SKIP LOCKED` makes a second instance safe | Topology (§3.0), §12.4 |
| §9 | Partition key = `aggregate_id` for per-aggregate ordering | §10.1, §9.2 |
| §10 | Producer-side dead letter (`status='failed'`) plus replay | §12.1, §13.5 |
| §12 | Idempotent consumer; three mechanisms, strongest available used | §11.3 |
| §13 | Inbox row and business work in one transaction | §11.3 |
| §13 | Acknowledge only after commit | §11.3, §10.2 |
| §13 | External side effect: consumer's own outbox **and** an idempotency key | §11.4 |
| §13 | Inbox retention window exceeds redelivery gap | §13.2 |
| §14 | Manual offset commits, `read_committed`, per-partition ordering | §10.2 |
| §15 | Retry ladder, dead-letter topic, replay | §12.2, §12.3 |
| §16 | Exactly-once stated honestly | Guarantees (§18.0) |
| §18 | Observability, purging, backpressure, deployment notes | Operations (§13.1–§13.6) |
| §21 | Unit, integration, fault-injection, end-to-end invariant tests | Testing (§15.0) |
| §22 | Anti-patterns explicitly avoided | Anti-patterns (§19.0) |

## 3. Topology

Four processes, one Go module, one `docker compose up`. Each is small; each has
one reason to exist.

```
                    ┌────────────────────────────────────────────────────────────┐
                    │  PostgreSQL 18.6 (one container, two databases)            │
                    │                                                            │
   POST /users       │   oe_accounts                    oe_notifications         │
        │            │   ┌──────────────────┐           ┌────────────────────┐   │
        ▼            │   │ accounts.users   │           │ notifications.inbox│   │
  ┌──────────┐  ONE  │   │ accounts.outbox  │           │ …notifications     │   │
  │   api    │──tx──▶│   └──────────────────┘           │ …outbox            │   │
  └──────────┘       │            ▲                     └────────────────────┘   │
                    │            │ claim/mark                   ▲    ▲          │
                    └────────────┼──────────────────────────────┼────┼──────────┘
                                 │                              │    │
                          ┌──────────────┐              ┌──────────────┐  ┌──────────────┐
                          │    relay     │              │   notifier   │  │    sender    │
                          └──────────────┘              └──────────────┘  └──────────────┘
                                 │ produce                     ▲ consume         │ POST
                                 ▼                             │                 ▼
                    ┌────────────────────────────────────────────────┐   ┌───────────────┐
                    │  Apache Kafka 4.3.1   accounts.user.v1 (3 part) │   │ email gateway │
                    │                       accounts.user.v1.DLT     │   │    (stub)     │
                    └────────────────────────────────────────────────┘   └───────────────┘
```

| Process | Responsibility | Why separate |
|---|---|---|
| `api` | Accounts HTTP API. Writes `users` + `outbox` in one transaction. | The write path must not depend on the broker at all. |
| `relay` | Polling publisher: claims `accounts.outbox`, produces to Kafka, marks published. | §7: "Deploy it as its own process. Embedding it in the API service … couples publishing throughput to request-handling." Also what makes the crash-window demonstration real — you can kill it. |
| `notifier` | Kafka consumer group member. Inbox row + notification + send-intent in one transaction, then commits the offset. | §13: the consuming end is a separate service with its own database. |
| `sender` | Polling publisher for `notifications.outbox`: performs the email call with `Idempotency-Key`, marks published. | §13 remedy 2: the consumer's own outbox, so an external side effect can never be lost or duplicated. |

The pattern therefore appears **twice** — once producing to a broker, once
driving an external API — which is exactly the composition §13 describes.

## 4. Scope

### In scope

The complete producing end (§4–§6), the polling publisher (§7) with backoff,
error classification and producer-side dead letters (§10), per-aggregate ordering
via partition keys (§9), the complete consuming end (§12–§15) with an inbox,
manual offset commits, a retry ladder and a dead-letter topic, a consumer-side
outbox for the external call (§13), operations (§18) — purge jobs,
health, replay — and the full test pyramid of §21 including fault injection.

### Out of scope

| Not included | What it would change |
|---|---|
| CDC / logical decoding (§11) | Replaces the relay with Debezium plus the outbox event router; the outbox would move to insert-then-delete in one transaction. The polling publisher is the reference's default, so it is what this implements. |
| Multi-region, sharded databases (§4, §8) | One outbox and one relay per shard; no cross-shard ordering. |
| Sagas (§23) | A coordinator for multi-step workflows. The outbox is a delivery mechanism, not an orchestrator. |
| Schema registry with Avro/Protobuf (§6) | `payload` would become `BYTEA` holding the exact wire bytes including the schema-id prefix. JSON keeps the table readable during incidents, which §4 recommends for exactly that reason. |
| Authentication, passwords, a UI | Not part of the pattern. |
| Metrics, Prometheus, dashboards (§18) | A telemetry stack around the pattern rather than the pattern. Every signal §18 asks for is emitted as a structured log field instead (§13.3), which keeps the reader's attention on the outbox. |
| Kafka transactions / exactly-once semantics | §16: they do not solve this problem, and believing they do is the anti-pattern. The relay's crash window is unclosable by design. |

## 5. Decisions

| # | Decision | Textbook basis |
|---|---|---|
| D1 | **Apache Kafka is the broker**, not an in-process queue | §1's premise requires two independent systems. Kafka also makes §7's ack table, §9's partition-key ordering, §14's offset semantics and §12's rebalance duplicates real rather than described. KRaft means one container. |
| D2 | **Four processes**: `api`, `relay`, `notifier`, `sender` | §7 requires the relay to be its own process; §13 requires the consumer to be its own service; §13 remedy 2 requires the consumer's own relay. |
| D3 | **Database per service**: `oe_accounts`, `oe_notifications` | §4: "Two services sharing one outbox table is two services sharing a database — the coupling the architecture exists to avoid." Two databases in one container make a cross-context join or shared transaction impossible to express. |
| D4 | **Structural event collection**: a unit of work drains tracked aggregates' events inside the commit | §5: this is "the only way 'every state change emits its event' becomes structural rather than a rule people must remember at each call site", and it puts the outbox insert in the persistence layer where the domain never sees it. Fowler's Unit of Work; Evans' aggregate events. |
| D5 | **Aggregate repository interfaces in `domain`**; technical ports in `application` | The four layers named in the brief are the Onion/DDD vocabulary, in which a Repository is a domain concept. An outbox is not: no domain expert has heard of one. |
| D6 | Relay **publishes inside the claiming transaction** | §7's recommended default: rows stay locked while publishing, so a crash duplicates one row rather than a batch. |
| D7 | **Single active relay**; a second instance is safe but unordered | §8: `SKIP LOCKED` makes concurrency safe, not ordered. Ordering is a stated guarantee here, so the relay scales by hot standby, not by parallelism. |
| D8 | `attempts` drives backoff; **only a permanent error reaches `failed`** | §7's table forbids burning attempts on transient errors, and warns that treating a broker outage as a per-message failure dead-letters the whole backlog. Its pseudocode contradicts its table; this design follows the table. |
| D9 | **Consumer-side outbox *and* an idempotency key** for the email | §13: remedy 1 alone loses the side effect if the send fails after the inbox row commits — the inbox then deduplicates the retry away. Remedy 2 makes the intent durable; the key makes the call safe to repeat. The reference's own final diagram shows both. |
| D10 | **Mark-and-purge**, `UUIDv7`, `payload` holds the complete serialised envelope | §4 on all three: auditability, index locality, and a relay that is a dumb pipe. |
| D11 | **`LISTEN`/`NOTIFY` wakeup**, poll loop authoritative, hazard monitored | §7: an optimisation, never the delivery path, and its queue can fail business commits. |
| D12 | **Stdlib `net/http`, no ORM, no framework**; `franz-go` for Kafka | Fewer concepts between reader and pattern. `franz-go` is pure Go, supports the idempotent producer, consumer groups and manual commits without cgo. |

## 6. Clean architecture

### 6.1 The dependency rule

```
                  ┌────────────────────────────────────┐
                  │             domain                 │
                  │ entities · events · invariants ·   │
                  │ aggregate repository interfaces    │
                  └────────────────────────────────────┘
                                   ▲
                  ┌────────────────┴───────────────────┐
                  │           application              │
                  │  use cases · technical ports       │
                  └────────────────────────────────────┘
                       ▲                        ▲
      ┌────────────────┴──────┐   ┌─────────────┴──────────────┐
      │     presentation      │   │       infrastructure       │
      │ http handlers·workers │   │ pgx·kafka·gateway·uow      │
      └───────────────────────┘   └────────────────────────────┘
                       ▲                        ▲
                       └───────────┬────────────┘
                            cmd/*/main.go wires
```

Arrows point in the direction of dependency. **`presentation` and
`infrastructure` are siblings, not a stack** — neither imports the other. This is
the shape Martin describes: interface adapters and drivers share the outer ring.
Nesting infrastructure inside presentation would license a handler to hold a
concrete repository, which the architecture test forbids.

| Layer | May import | Contains | Must not contain |
|---|---|---|---|
| **domain** | stdlib, `uuid` | Entities, value objects, domain events, domain errors, invariants, aggregate repository interfaces | SQL, struct tags, any I/O |
| **application** | domain, `platform/messaging` | Use cases, technical ports, commands and results, the transaction boundary | Any concrete driver, SQL, HTTP or Kafka types, logging |
| **infrastructure** | application, domain, `platform/*` | pgx repositories, unit of work, Kafka producer and consumer, email gateway, migrations | Business rules, use-case orchestration |
| **presentation** | application, domain, `platform/*` | HTTP handlers and DTOs, worker loops, logging of use-case results | Business rules, SQL |

**Where `platform` sits.** Technical shared code, not a fifth layer. Only
`platform/messaging` is visible to the application layer, because a consumer use
case's input *is* the wire envelope. `config`, `postgres`, `kafka`,
`logging`, `ids`, `clock` are reachable from infrastructure, presentation and
`main` only. `domain` imports none of it.

**Observation never appears in a use case.** Use cases return result structs —
`PublishResult{Claimed, Published, TransientFailures, PermanentFailures}`,
`ProcessResult{Duplicate, Recorded}` — and the presentation worker turns them
into log lines (§13.3). No logger reaches into the middle of business logic,
which is what makes every counted event assertable in a unit test.

### 6.2 Enforcement

An architecture test walks the import graph with `go/packages` and fails on:

- any `domain` package importing beyond stdlib and `uuid`;
- any `application` package importing `infrastructure`, `presentation`, or a
  `platform` package other than `platform/messaging`;
- any `accounts/*` package importing `notifications/*` or the reverse, except
  `platform/messaging`;
- any construction of one context's repository from the other context's pool.

A boundary that is not checked is a comment.

### 6.3 Where the relay lives

| Concern | Layer | Why |
|---|---|---|
| Claim, publish in `id` order, await ack, mark, classify errors, compute backoff | `accounts/application/publish_pending_batch.go` | This is the pattern. It must be testable with fakes, no database, no clock, no Kafka. |
| Ticker, jitter, `LISTEN` wakeup, shutdown, logging | `accounts/presentation/worker/relay.go` | A driver that triggers a use case, structurally identical to an HTTP handler. It picks the next sleep from `PublishResult`. |
| `SELECT … FOR UPDATE SKIP LOCKED`, `UPDATE … SET status` | `accounts/infrastructure/postgres/outbox_repo.go` | Dialect-specific persistence behind a port. |
| `kgo.Client.ProduceSync`, header mapping, error classification | `accounts/infrastructure/kafka/publisher.go` | Broker-specific; translates Kafka errors into `ErrTransient` / `ErrPermanent`. |

The notifier and the sender are split the same way.

### 6.4 Event collection: the unit of work

| Piece | Layer | Responsibility |
|---|---|---|
| `domain.User.PullEvents() []Event` | domain | Aggregates record events on themselves, knowing nothing of tables or brokers. `domain.Event` exposes routing only — `EventType`, `AggregateType`, `AggregateID`, `OccurredAt`; the schema version belongs to the message contract and is named by `mapData`. The draining interface is declared at its consumer, in `infrastructure/postgres`, not exported by the domain. |
| `application.UnitOfWork` — `Do(ctx, Metadata, func(Work) error) error` | application | The transaction boundary a use case sees. `Work` hands out repositories bound to that transaction. |
| `application.EnvelopeFactory` | application | Maps domain events to CloudEvents envelopes. The message contract is an application concern. |
| `infrastructure/postgres.UnitOfWork` | infrastructure | Opens the transaction, tracks aggregates the repositories touch, drains their events, appends outbox rows, commits. §5: "The outbox insert lives in the persistence layer. The domain never imports it." |

The explicit alternative — a use case calling `outbox.Append(user.PullEvents())`
itself — is more legible in one file but makes "every state change emits its
event" a rule each future call site must remember. §5 says the structural form
"is the only way" it stops being that, so the structural form is what a reference
implementation must show. The cost is that the two inserts are no longer adjacent,
so §11.1 shows the unit of work's body, and a test pins the invariant: register a
user through the use case, assert exactly one pending outbox row.

### 6.5 The one shared type

`platform/messaging.Message` — `EventID`, `EventType`, `SchemaVersion`, `Key`,
`Payload []byte`, `Headers map[string]string`, `Topic`, `Partition`, `Offset`,
`Attempt int`. The transport envelope, and the only type crossing the context
boundary.

`Attempt` is **process-local**, because Kafka has no delivery counter: a record
carries no "how many times have I been handed out" field, and there is nowhere to
put one without rewriting the record. The notifier keeps an in-memory count keyed
by `(topic, partition, offset)`, and that count resets on a rebalance or a
restart. It is therefore a heuristic for "stop retrying this and dead-letter it",
never a correctness input — the inbox is what makes reprocessing safe. §12.2 says
what follows from that. It is deliberately not a shared domain model: it is the wire contract,
and each context maps it to and from its own types at the edge.

## 7. Bounded contexts

| | accounts | notifications |
|---|---|---|
| **Role** | Producer | Consumer, and producer to an external API |
| **Process** | `api` + `relay` | `notifier` + `sender` |
| **Database** | `oe_accounts` | `oe_notifications` |
| **Schema within it** | `accounts` | `notifications` |
| **Tables** | `users`, `outbox` | `inbox`, `notifications`, `outbox` |
| **Domain** | `User` aggregate, `Email` / `DisplayName` value objects, `UserRegistered` event | `Notification` aggregate, `Recipient` / `UserRef` / `NotificationState` value objects, `WelcomeEmailRequested` event |
| **Domain repository** | `domain.UserRepository` | `domain.NotificationRepository` |
| **Application ports** | `OutboxRepository`, `UnitOfWork`, `EnvelopeFactory`, `EventPublisher`, `Wakeup`, `Chaos`, `Clock`, `IDGen` | `InboxRepository`, `OutboxRepository`, `UnitOfWork`, `EnvelopeFactory`, `EmailGateway`, `DeadLetterPublisher`, `Chaos`, `Clock`, `IDGen` |
| **Knows about the other** | Nothing. Produces to a topic. | Nothing. Consumes a topic. Never calls back into accounts. |

The schema name is kept inside each database so every qualified table name reads
identically whether you are looking at one database or two. Each context has its
own `pgxpool`; there is no connection on which a cross-context transaction could
be written even by mistake.

## 8. Data model

### 8.1 `oe_accounts`

```sql
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

CREATE INDEX outbox_pending_idx
    ON accounts.outbox (available_at, id)
    WHERE status = 'pending';
```

The reference schema of §4, unchanged. What each column is load-bearing for:

- **`id BIGSERIAL`** is the ordering authority and the relay's sort key. Never
  order by `occurred_at`: clocks skew, NTP steps backwards, and two rows written
  in the same millisecond have no defined order. Sequence gaps from rolled-back
  transactions are normal and are not evidence of a lost event.
- **`event_id UUID UNIQUE`** is minted once, at insert, by the producer, and is
  the consumer's deduplication key. The relay never generates one. Regenerating
  it per publish attempt is, per §4, "the single most common way to break the
  pattern while appearing to implement it correctly".
- **`aggregate_type` / `aggregate_id`** are columns, not payload fields, because
  the relay routes on them and must never parse business content.
  `aggregate_type` selects the topic; `aggregate_id` becomes the partition key.
- **`schema_version`** is a column for a different reason: nothing routes on it,
  but the relay composes it into a header (§9.2) and must do so without opening
  the payload. It is supplied by the envelope factory, not by the domain event —
  a wire schema version is message contract, and one domain event type returning
  one fixed version would make §9.3's dual-publish migration inexpressible.
- **`payload JSONB`** holds the message already serialised into final published
  form. `JSONB` over `BYTEA` deliberately: §4 recommends it wherever you may need
  to read the table during an incident.
- **`available_at`** implements retry backoff without a separate delay queue.

`users.version` is the aggregate version the payload carries so consumers can
detect staleness (§6). It is a column rather than a constant in the mapper
because hard-coding `1` would become a lie the first time a user is updated — the
kind of lie discovered by a consumer acting on stale data.

### 8.2 `oe_notifications`

```sql
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

-- The consumer's own outbox: durable intents for the external call (§13).
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
```

`consumer` is part of the inbox key because several logical consumers may share a
database and each must process an event independently (§13). This design has one,
`notifications.welcome-email`, and keeps the column so a second needs no
migration.

`notifications.source_event_id UNIQUE` is §12's mechanism 2 sitting behind §13's
mechanism 3 — redundant with the inbox by design. It demonstrates that the
mechanisms compose and guarantees that a bug in the inbox path still cannot
produce two notification rows.

`idempotency_key` on the consumer's outbox is the inbound `event_id`. It is a
column rather than a payload field for the same reason as the accounts outbox:
the sender relay is a dumb pipe and routes on columns.

**`state` is a domain type, not a string.** The `CHECK` constraint is the
database's backstop, not the definition of the state machine. `Notification`
owns a `NotificationState` value object and the transitions are methods on the
aggregate — `MarkSent(at)`, `MarkFailed(reason)` — each refusing an illegal move
(`sent` is terminal; `pending` is the only legal predecessor of either). A bare
`state string` field would leave the rules in three places at once: the CHECK,
whatever the sender does before its `UPDATE`, and the reader's memory. The rule
is the same one §5 applies to the outbox insert — an invariant that lives in a
call site is an invariant someone can forget.

### 8.3 Migrations

Plain SQL under `migrations/accounts/` and `migrations/notifications/`, embedded
with `//go:embed`, applied by goose's library API. Each database has its own
migration directory and its own `goose_db_version` table, so the two contexts'
schema histories are independent.

Schema change is a **separate step**: `outboxexpress migrate --context=accounts`,
called by `make migrate` and by an init job in compose. No service applies
migrations at startup — with four processes and any replica count, startup
migration is a race. Readiness stays false until the expected version is present.

## 9. Message contract

### 9.1 The envelope

CloudEvents 1.0 (§6). `payload` holds the complete serialised envelope; the relay
sends those bytes verbatim.

```json
{
  "specversion": "1.0",
  "id": "018f4c2a-6b1e-7c3d-9a44-2f8e1d5b7c90",
  "type": "com.outboxexpress.accounts.user.registered",
  "source": "/services/accounts",
  "subject": "9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1",
  "time": "2026-08-26T10:15:30.123Z",
  "dataschema": "https://schemas.outboxexpress.dev/accounts/user.registered/1.json",
  "datacontenttype": "application/json",
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  "data": {
    "user_id": "9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1",
    "email": "ada@example.com",
    "display_name": "Ada Lovelace",
    "version": 1,
    "registered_at": "2026-08-26T10:15:30.100Z"
  }
}
```

`id` is the `event_id` and the consumer's deduplication key. `subject` is the
aggregate id. Business fields live under `data` and nowhere else.

**Event-carried state transfer** (§6): the payload carries the fields consumers
need as of the moment the event occurred, plus `version`. The notifier never calls
back into accounts — it works while accounts is down, and it acts on data
consistent with the event rather than on newer state that never coexisted with it.

### 9.2 Kafka mapping

| Kafka field | Value | Why |
|---|---|---|
| Topic | `accounts.user.v1`, from `aggregate_type` via a routing table | §4: route by `aggregate_type` at publish time. An unmapped type is a *permanent* error, so the branch is reachable. |
| Key | `aggregate_id` | §9: this is what buys per-entity ordering. All events for one user land on one partition. |
| Value | `payload` bytes, verbatim | §4: the relay is a dumb pipe. |
| Headers | `ce_id`, `ce_type`, `ce_specversion`, `schema_version`, `content-type`, `correlation_id`, `traceparent` | §4, §6: a consumer can route and deduplicate without deserialising, and the trace survives the asynchronous hop. |

Topic configuration: 3 partitions to prove keying does the work,
`cleanup.policy=delete`, `retention.ms=604800000` (7 days), `min.insync.replicas`
and replication factor 1 **in the demo only** — §7 requires `acks=all` *with*
`min.insync.replicas >= 2` and RF ≥ 3 for durability, and the README states that
a single-broker demo cannot provide it. Dead-letter topic
`accounts.user.v1.DLT`, 1 partition, 30-day retention.

### 9.3 Evolution

Additive changes only within a `schema_version`. Never repurpose, retype or
remove a field consumers may read. Consumers are tolerant readers and ignore
unknown fields — which is what makes additive producer changes safe. A breaking
change means a new `schema_version` or a new `event_type`, with both published
during the migration window as two outbox rows in the same transaction, which the
outbox makes trivial.

## 10. Broker configuration

### 10.1 Producer (relay)

`franz-go` options, with the classic Kafka setting each corresponds to:

```go
kgo.RequiredAcks(kgo.AllISRAcks())            // acks=all — §7's durability floor
// idempotent writes are franz-go's default; we never call DisableIdempotentWrite
kgo.MaxProduceRequestsInflightPerBroker(5)    // safe while idempotent
kgo.RecordRetries(0)                          // the outbox is the retry — see below
kgo.ProducerBatchCompression(kgo.ZstdCompression())
kgo.RequestTimeoutOverhead(10 * time.Second)  // bound the open transaction
```

Note that franz-go requires idempotency unless it is explicitly disabled, and an
idempotent producer implies `acks=all` — the two settings §7 asks you to set
rather than assume are, in this client, the defaults you have to work to break.

`RecordRetries(0)` looks wrong and is deliberate. The outbox *is* the retry
mechanism: a failed produce leaves the row `pending` with a backoff, and the relay
tries again with the same `event_id`. Client-side retries would add a second,
invisible retry loop with different semantics, and — because D6 publishes inside
the claiming transaction — they would hold that transaction open across them.

The relay uses **`ProduceSync`** and inspects the returned record's error before
marking the row published. §7: "Fire-and-forget publishing plus 'mark published'
loses messages: the relay records success, the broker never durably had the
message, and the row is gone from the pending set forever."

Kafka's idempotent producer deduplicates retries *within a producer session*
using a producer id and sequence numbers. It does not survive a restart, and it
says nothing about the relay's crash window. It reduces duplicates; it does not
remove the need for the inbox.

### 10.2 Consumer (notifier)

```go
kgo.ConsumerGroup("notifications.welcome-email")
kgo.ConsumeTopics("accounts.user.v1")
kgo.DisableAutoCommit()                             // §13: commit after the DB commit
kgo.FetchIsolationLevel(kgo.ReadCommitted())
kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())   // auto.offset.reset=earliest
kgo.OnPartitionsRevoked(stopAndForget)              // see rebalances, below
```

Offsets are committed with `CommitRecords` for the single record just processed —
never with an automatic ticker.

**Manual commits, always after the database commit.** The order is: process the
record in a transaction, commit the transaction, then commit the offset. A crash
between them causes redelivery, the inbox insert conflicts, the duplicate is
skipped, and the offset is committed. That is the design working. Committing the
offset first would turn a crash into permanent loss.

**One record per transaction.** This is a property of the processing loop, not a
client setting: the notifier iterates fetched records and opens one database
transaction per record. Batching would be faster and is the wrong default for a
reference implementation — a single poison record would fail its whole batch, and
partial progress could not be committed. Per-key ordering survives because one
partition is processed in offset order by one member.

**Rebalances produce duplicates** (§12). On partition revocation the notifier
stops processing, does not commit offsets for records it has not finished, and
lets the new owner redeliver them. The inbox absorbs the overlap.

### 10.3 The sender's "broker" is an HTTP API

The `sender` relay is the same polling publisher against
`notifications.outbox`, but its destination is the email gateway rather than
Kafka. It passes `Idempotency-Key: <idempotency_key>` — the original inbound
`event_id` — so a repeated call after a crash window is deduplicated by the
provider. This is §13 remedy 1 applied by the relay of remedy 2, which is exactly
the composition the reference's final diagram shows.

The stub gateway persists keys it has seen to disk for the container's lifetime
and returns the original response for a repeat, so the demo behaves like a real
provider rather than a forgetful map.

## 11. Core flows

### 11.1 Registration — the producing transaction

```go
// accounts/application/register_user.go
func (uc *RegisterUser) Execute(ctx context.Context, cmd RegisterUserCommand) (Result, error) {
    id, err := uc.ids.New()                       // UUIDv7; generation can fail
    if err != nil {
        return Result{}, err
    }
    user, err := domain.Register(id, cmd.Email, cmd.DisplayName, uc.clock.Now())
    if err != nil {
        return Result{}, err                      // pure domain, no I/O
    }

    if err := uc.uow.Do(ctx, cmd.Meta, func(w Work) error {
        return w.Users.Insert(ctx, user)          // ErrEmailTaken surfaces here
    }); err != nil {                              // <- the atomic moment
        return Result{}, err
    }

    uc.wakeup.Notify(ctx)                         // best effort; failure is fine
    return Result{UserID: user.ID}, nil
}
```

The outbox insert is not missing — it is structural. `Insert` marks the aggregate
as tracked, and the unit of work drains it inside the same transaction:

```go
// accounts/infrastructure/postgres/uow.go  — the persistence layer, per §5
func (u *UnitOfWork) Do(ctx context.Context, meta application.Metadata,
    fn func(application.Work) error) error {

    tx, err := u.pool.Begin(ctx)
    if err != nil { return err }
    defer tx.Rollback(ctx)                        // no-op after commit

    w := newWork(tx)                              // repositories bound to tx
    if err := fn(w); err != nil { return err }

    if events := w.drainTrackedEvents(); len(events) > 0 {
        envelopes, err := u.envelopes.From(events, meta)
        if err != nil { return err }
        if err := newOutboxRepo(tx).Append(ctx, envelopes); err != nil { return err }
    }
    return tx.Commit(ctx)                          // business rows + outbox rows
}
```

Rules this obeys (§5):

1. **Same transaction, same connection.** One `tx`, both writes, committed
   together. The pool is not reachable from the application layer, so there is no
   path to an untransacted repository.
2. **Nothing external inside the transaction.** No HTTP, no produce, no email.
   Transactions hold locks; holding them across a call to a system you do not
   control converts its latency into your lock contention.
3. **No after-commit hook as the delivery path.** `wakeup.Notify` sits outside
   the transaction and its error is discarded. It only shortens latency.
4. **Short transactions.** Two statements, no network calls. The outbox is a hot
   table, and in PostgreSQL a long transaction there holds back the `xmin`
   horizon for the whole database.

What this closes:

| Moment of failure | Outcome |
|---|---|
| Crash before commit | Nothing happened. No user, no event. The client retries. |
| Crash during commit | PostgreSQL resolves it atomically. Both rows or neither. |
| Crash after commit, before the relay runs | Both rows exist. The relay publishes on a later pass, possibly minutes later after a restart, but it publishes. |
| Kafka unavailable for an hour | Rows accumulate. `POST /users` keeps returning 201. |

That last row is the pattern's quiet superpower: a broker outage stops being an
outage of the write path.

### 11.2 A relay pass

```
begin transaction
    batch = SELECT * FROM accounts.outbox
             WHERE status = 'pending' AND available_at <= now()
             ORDER BY id
             LIMIT :batch_size
             FOR UPDATE SKIP LOCKED

    if empty: commit; wait(notification or jittered idle backoff); continue

    for row in batch:                              -- strictly in id order
        res = ProduceSync(topic_for(row.aggregate_type),
                          key = row.aggregate_id,
                          value = row.payload,
                          headers = row.headers + ce_* + schema_version)
        if res.err is transient:  attempts++, available_at = now()+backoff, break
        if res.err is permanent:  status = 'failed', alert, continue
        chaos.MaybeCrashAfterPublish()             -- no-op unless armed
        UPDATE … SET status='published', published_at=now() WHERE id = row.id
commit
```

**Claim by state, never by cursor.** A `WHERE id > :last_seen` relay loses
events: sequence values are assigned at `INSERT` but rows become visible at
`COMMIT`, and those orders differ. A row that commits late is already behind the
cursor and is never published, silently. Claiming by status has no such flaw —
uncommitted rows are simply invisible to the relay's snapshot.

**`FOR UPDATE SKIP LOCKED`** gives mutual exclusion per row, no blocking between
relays, and automatic lease release: a crashed relay's locks are dropped by the
database on disconnect. There is no stuck claim and no reaper to write — a real
advantage over a `locked_by` / `locked_at` column, which needs a lease longer
than the worst-case publish time or it becomes a duplicate factory.

**The crash window.** Between the broker's durable ack and the `UPDATE`, a crash
leaves the message published and the row pending, so the next pass publishes it
again. This window cannot be closed — closing it would require an atomic commit
across Kafka and PostgreSQL, which is where §1 started. It is why the pattern is
at-least-once, why `event_id` must be stable, and why §11.3 is not optional.

### 11.3 Consumption — the consuming transaction

```go
// notifications/application/process_user_registered.go
func (uc *ProcessUserRegistered) Execute(ctx context.Context, msg messaging.Message) (ProcessResult, error) {
    // The anti-corruption layer. accounts' published language is translated into
    // this context's own vocabulary here, and nowhere else; no domain code below
    // this line has ever seen what accounts calls its fields. A decoded envelope
    // handed straight to a notifications constructor would make this context a
    // Conformist — its model defined by a schema another team owns.
    req, err := uc.translate(msg)
    if err != nil {
        return ProcessResult{}, fmt.Errorf("%w: %v", ErrPermanent, err)   // -> DLT
    }

    notif, err := notification.RequestWelcome(
        uc.ids.New(), req.Recipient, req.User, req.SourceEventID, uc.clock.Now())
    if err != nil {
        return ProcessResult{}, fmt.Errorf("%w: %v", ErrPermanent, err)   // -> DLT
    }

    var duplicate bool
    err = uc.uow.Do(ctx, msg.Metadata(), func(w Work) error {
        first, err := w.Inbox.Record(ctx, uc.consumer, msg.EventID, msg.EventType)
        if err != nil {
            return err
        }
        if !first {
            duplicate = true
            return nil                            // already processed; no work
        }
        return w.Notifications.Insert(ctx, notif) // emits WelcomeEmailRequested
    })
    if err != nil {
        return ProcessResult{}, err               // no offset commit -> redelivery
    }
    if duplicate {
        return ProcessResult{Duplicate: true}, nil
    }
    return ProcessResult{Recorded: true}, nil
}
```

One transaction, three writes: the inbox row, the notification row, and — drained
structurally by the same unit of work — the send-intent row in
`notifications.outbox`. Then, and only then, the worker commits the offset.

**The translator is the boundary.** `uc.translate` lives in
`notifications/application/translate.go` and returns notifications-owned types:

```go
type WelcomeRequest struct {
    Recipient     notification.Recipient    // this context's email value object
    User          notification.UserRef      // a foreign aggregate, referenced by id
    SourceEventID uuid.UUID
}
```

It is the only code in the context that knows accounts' field names, so an
additive change to `com.outboxexpress.accounts.user.registered` is absorbed in
one file rather than reaching the `Notification` aggregate. `UserRef` is an id
and nothing else: accounts' `User` is a different aggregate in a different
context, and holding a copy of it here would be one model with a network in the
middle. A missing or malformed field is `ErrPermanent` — the message will never
become valid, so it belongs on the dead-letter topic, not in a retry loop.

This is the inbound half of what §9.1 already does outbound. The CloudEvents
factory is an Open Host Service publishing a stable language; the translator is
the Anti-Corruption Layer that consumes one. A context with only the first half
exports its model and imports someone else's.

Why atomicity here is not optional (§13):

| Wrong shape | What breaks |
|---|---|
| Mark processed, then do the work | A crash between them loses the work permanently: redelivery deduplicates it away. Silent data loss. |
| Do the work, then mark processed | A crash between them reprocesses the work — the problem the inbox was added to solve. |
| Mark processed in Redis, work in Postgres | Two systems, no shared transaction. Back to §1. |

**The external send is not here.** It is a durable intent in the consumer's own
outbox, performed by `sender`. This is the decision that most repays explanation:
if the notifier called the gateway after committing the inbox row, a transient
gateway failure would be unrecoverable — the retry arrives, the inbox says
"already processed", and the email is never sent. With an intent row, the send
retries independently, forever if need be, and the `Idempotency-Key` keeps a
repeat harmless.

### 11.4 The sender pass

Identical machinery to §11.2 against `notifications.outbox`: claim with
`SKIP LOCKED`, `POST` to the gateway with `Idempotency-Key`, mark published, back
off on transient failures, dead-letter on permanent ones. On success it also sets
`notifications.state = 'sent'` — in the same transaction as the mark, because both
tables are in `oe_notifications` and there is no reason to accept a dual write
that a single transaction can absorb.

### 11.5 The duplicate path

```mermaid
sequenceDiagram
    autonumber
    participant R as relay
    participant K as Kafka
    participant DBa as oe_accounts
    participant N as notifier
    participant DBn as oe_notifications
    participant S as sender
    participant G as gateway

    R->>DBa: claim row 42 (SKIP LOCKED)
    R->>K: ProduceSync(event_id=E1, key=user-9f3c)
    K-->>R: acks=all — durable
    Note over R: chaos hook fires: crash before marking
    Note over DBa: lock released, row 42 still 'pending'

    K->>N: deliver E1
    N->>DBn: inbox(new) + notification + send-intent; COMMIT
    N->>K: commit offset
    S->>G: POST Idempotency-Key: E1
    G-->>S: 202 accepted
    S->>DBn: mark intent published, notification sent

    R->>DBa: (restart) claim row 42 again
    R->>K: ProduceSync(event_id=E1) — SECOND TIME
    K->>N: deliver E1 again
    N->>DBn: inbox insert — CONFLICT, 0 rows
    Note over N: duplicate absorbed, offset committed
    Note over G: one email. Not two.
```

Every duplicate source of §12 is present and absorbed: the relay's crash window,
a notifier that dies before committing an offset, a consumer-group rebalance, and
a deliberate replay.

## 12. Failure handling

### 12.1 Relay error classification

The most common relay bug in production (§7), so it is modelled explicitly: the
`EventPublisher` port returns errors wrapping `ErrTransient` or `ErrPermanent`,
and the use case branches on them.

| Class | Kafka examples | Action |
|---|---|---|
| **Transient** | `NOT_ENOUGH_REPLICAS`, `LEADER_NOT_AVAILABLE`, `REQUEST_TIMED_OUT`, connection refused, coordinator loading | Not this message's fault. `attempts++` — which drives backoff and nothing else — `available_at = now() + backoff(attempts)`, record `last_error`, **break the batch**, commit, back off. |
| **Permanent** | `RECORD_TOO_LARGE`, `UNKNOWN_TOPIC_OR_PARTITION` with auto-creation disabled, `TOPIC_AUTHORIZATION_FAILED`, unmapped `aggregate_type`, serialisation failure | `status = 'failed'`, record `last_error`, log at alert level, continue the batch. |

`backoff(n) = min(5m, 1s · 2^n) · random(0.5, 1.5)`. Jitter stops restarts
synchronising into a thundering herd.

**The reference contradicts itself here, and the resolution matters.** §7's table
says a transient error must not burn `attempts`; §7's pseudocode increments them.
Taken together they would let an hour-long Kafka outage push a whole backlog to
`failed` — the exact anti-pattern that section warns about two paragraphs later.
This design resolves it:

> **`attempts` computes backoff. Only a permanent error moves a row to `failed`.**

`RELAY_MAX_ATTEMPTS` is therefore an **alert threshold**, not a state transition:
a row crossing it is counted in `outbox_stuck_rows` and logged, and it keeps
retrying. Whether a row is genuinely poison is a judgement a human makes with
`last_error` in front of them.

### 12.2 The consumer's retry ladder

§15's ladder, in order:

1. **In-process retry** for an obviously transient database error: 3 attempts,
   50 ms · 2^n with jitter, offset uncommitted.
2. **Redelivery** if those fail: return without committing the offset. Kafka
   redelivers on the next poll cycle or after a rebalance. `Deliveries` is tracked
   in a header the notifier stamps on the DLT record, since Kafka has no native
   delivery counter.
3. **Dead-letter topic** on a permanent error, or after
   `CONSUMER_MAX_DELIVERIES` (5) attempts: produce the original record —
   value and headers verbatim, plus `dlt_reason`, `dlt_error`, `dlt_deliveries`,
   `dlt_original_topic`, `dlt_original_partition`, `dlt_original_offset` — await
   the ack, then commit the offset.

Producing to the DLT before committing the offset is the same ordering rule as
before: the record is durable somewhere else before we forget where it was.

### 12.3 Replay

Three replay paths, each with a documented consequence:

| Path | Mechanism | Consequence |
|---|---|---|
| Producer-side | `POST /admin/outbox/{id}/replay` sets `failed` → `pending`, `attempts = 0` | Republished with the same `event_id`, so consumers deduplicate. Safe. |
| Consumer-side, from the DLT | A `replay-dlt` command reproduces DLT records onto the source topic | Safe *if* the inbox row is still within retention. |
| Consumer-side, offset reset | Reset the group to earliest | **Not safe by default.** Events whose inbox rows were purged are processed again. §13: retention is the boundary of the deduplication guarantee, and this is the incident it causes. |

### 12.4 Running more than one relay

`SKIP LOCKED` makes a second relay *safe* — no row is ever published twice
concurrently — but not *ordered*: two relays can publish two events for the same
user at the same time and Kafka may accept them in either order. Since
per-aggregate ordering is a stated guarantee here, the relay runs **single active
with hot standby**: two instances, both polling, and ordering preserved in
practice because one of them finds nothing to do. The README says what the
alternatives cost — sharded relays by `aggregate_id` hash, or accepting §9's
pragmatic alternative of version-stamped events with last-writer-wins consumers.

### 12.5 Fault injection

A `Chaos` port, no-op unless `CHAOS_ENABLED=true`, with three arming endpoints on
the admin listener:

| Endpoint | Injects | Proves |
|---|---|---|
| `POST /admin/chaos/crash-after-publish` | Relay returns before marking the row | The crash window. Same `event_id` published twice; one email. |
| `POST /admin/chaos/crash-before-offset-commit` | Notifier returns after the DB commit, before committing the offset | Redelivery absorbed by the inbox. |
| `POST /admin/chaos/gateway-fail-once` | Gateway returns 503 once | The consumer-side outbox retrying an external call that the inbox would otherwise have swallowed. |

These are the tests of §21 exposed as buttons, because a reference implementation
should let a reader watch the guarantees hold rather than trust an assertion.

## 13. Operations

### 13.1 Relay wakeup

The `api` issues `NOTIFY outbox_new` after commit, outside the transaction, with
its error discarded. The relay holds a dedicated connection with
`LISTEN outbox_new` and waits with a timeout equal to its current idle backoff.

**An optimisation, never the delivery path.** Notifications are not durable and
are not delivered to a listener that was disconnected when they were sent. The
poll loop with its timeout remains the source of truth.

**And it can fail your producers.** PostgreSQL's `NOTIFY` queue is shared and
finite, trimmable only past the slowest listener's position. A session that has
executed `LISTEN` and then sits in a long transaction prevents cleanup; when the
queue fills, transactions calling `NOTIFY` **fail at commit** — a wedged relay
can start failing registrations, the exact opposite of the outbox's purpose. So:
the listening connection never enters a transaction, `pg_notification_queue_usage()`
is read and logged on every relay pass and warned on above 25%, and
`RELAY_USE_NOTIFY=false` drops the mechanism entirely. The README says that dropping it and accepting poll latency is
a legitimate production choice.

### 13.2 Purging

| Job | Deletes | Demo | Production guidance |
|---|---|---|---|
| Accounts outbox purge | `status='published' AND published_at < now() - retention` | 24 h | Days — long enough to answer "did we send it?" during an incident |
| Notifications outbox purge | same, on the consumer's outbox | 24 h | as above |
| Inbox purge | `processed_at < now() - retention` | 7 d | `> broker_retention + max_replay_lag + max_outage_duration`; 7 d floor, 30 d comfortable |

PostgreSQL has no `DELETE … LIMIT`, so the bound goes in a subquery:

```sql
DELETE FROM accounts.outbox
 WHERE id IN (
    SELECT id FROM accounts.outbox
     WHERE status = 'published'
       AND published_at < now() - $1::interval
     ORDER BY id
     LIMIT $2
 );
```

The loop repeats until fewer than `$2` rows are removed, so no purge takes a long
lock or a long transaction. `failed` rows are never purged automatically.

Inbox retention is not storage hygiene — it is the boundary of the deduplication
guarantee, and §12.3 records the incident that follows from forgetting it.

### 13.3 Observability

There is no metrics system, no registry and no `/metrics` endpoint. §18's
"metrics that matter" are all still watched — as fields on structured `slog`
lines, because the subject of this project is the outbox, not the telemetry
stack around it.

The relay logs one line per pass, at `INFO` when it did work and `DEBUG` when it
found none:

| Field | Why |
|---|---|
| `claimed`, `published` | Throughput, and the gap between them when a batch is stalling. |
| `transient_failures`, `permanent_failures` | Transient versus permanent; a spike in permanent is a poison batch. |
| `backlog` | Pending rows. Rising means the relay is losing. |
| `oldest_pending_age_ms` | The best single signal — a stuck relay shows here first. |
| `failed_rows` | Rows parked in `status='failed'`. Above zero needs a human. |
| `stuck_rows` | Pending past `RELAY_MAX_ATTEMPTS`. Still retrying; look anyway. |
| `batch_ms` | Transaction duration — the signal for moving to claim-commit-publish. |
| `notify_queue_usage` | The §13.1 hazard, watched. |

The notifier logs `duplicate` and `recorded` per record, which is what makes the
inbox's work visible — a non-zero duplicate count is healthy, not an error. The
sender logs each gateway outcome and the `occurred_at`-to-send latency, the
number a product owner feels. Consumer lag is read with
`kafka-consumer-groups.sh`, which `make lag` wraps.

The backlog fields come from one stats query per pass, issued on the transaction
that is already open, so observation costs a single cheap round trip rather than
a background collector holding its own connection. Every number in those lines
comes out of a use case's result struct (§6.1) — none is a counter incremented in
the middle of business logic — which is exactly why each one is assertable in a
unit test.

### 13.4 Health

`/healthz` is liveness: the process is up. `/readyz` is readiness: pools
reachable, expected migration version present, and for the notifier, group
membership established. Kafka reachability is deliberately *not* part of the
`api`'s readiness — the whole point is that the write path survives a broker
outage, and a readiness probe that fails on it would take the API down with the
broker.

### 13.5 Endpoints

Two listeners per process. An unauthenticated replay-and-chaos surface has no
business sharing a port with public traffic.

| Listener | Process | Route |
|---|---|---|
| public `:8080` | api | `POST /users` · `GET /healthz` |
| admin `127.0.0.1:8081` | api | `GET /readyz` · `POST /admin/chaos/*` |
| admin `127.0.0.1:8082` | relay | the above · `POST /admin/outbox/{id}/replay` |
| admin `127.0.0.1:8083` | notifier | the above · `POST /admin/chaos/crash-before-offset-commit` |
| admin `127.0.0.1:8084` | sender | the above · `POST /admin/outbox/{id}/replay` (notifications) |

Each process gets its own admin port because four processes cannot share one, and
`make demo` prints the four URLs. The admin listener binds loopback, not a
wildcard. There is no authentication,
and the README says plainly that the bind address is the only reason that is
acceptable.

### 13.6 Deployment notes

Migrations run as their own job before any service starts. The relay is deployed
with `maxSurge: 0` semantics — a brief gap is harmless because the outbox is
durable, whereas two relays overlapping during a rollout forfeit ordering (§12.4).
The notifier scales to at most the partition count; beyond that, members sit
idle. `terminationGracePeriod` must exceed one poll-and-commit cycle so a rolling
restart does not manufacture redeliveries.

## 14. Configuration

Environment variables, parsed once into a struct per process, validated at
startup, refusing to start on an invalid value. No configuration library.

| Variable | Default | Processes |
|---|---|---|
| `ACCOUNTS_DATABASE_URL` | — | api, relay |
| `NOTIFICATIONS_DATABASE_URL` | — | notifier, sender |
| `KAFKA_BROKERS` | `localhost:9092` | relay, notifier |
| `KAFKA_TOPIC` | `accounts.user.v1` | relay, notifier |
| `KAFKA_DLT_TOPIC` | `accounts.user.v1.DLT` | notifier |
| `KAFKA_GROUP_ID` | `notifications.welcome-email` | notifier |
| `HTTP_ADDR` | `:8080` | api |
| `ADMIN_ADDR` | `127.0.0.1:8081` api · `:8082` relay · `:8083` notifier · `:8084` sender | all |
| `LOG_LEVEL` | `info` | all |
| `RELAY_BATCH_SIZE` | `100` | relay, sender |
| `RELAY_IDLE_MIN` / `RELAY_IDLE_MAX` | `50ms` / `2s` | relay, sender |
| `RELAY_BACKOFF_BASE` / `RELAY_BACKOFF_CAP` | `1s` / `5m` | relay, sender |
| `RELAY_MAX_ATTEMPTS` | `10` | relay, sender — an alert threshold, not a transition |
| `RELAY_USE_NOTIFY` | `true` | relay |
| `CONSUMER_MAX_DELIVERIES` | `5` | notifier |
| `CONSUMER_RETRY_ATTEMPTS` | `3` | notifier |
| `EMAIL_GATEWAY_URL` | — | sender |
| `PURGE_INTERVAL` | `1m` | relay, sender |
| `OUTBOX_RETENTION` | `24h` | relay, sender |
| `INBOX_RETENTION` | `168h` | notifier |
| `PURGE_BATCH` | `1000` | relay, sender |
| `CHAOS_ENABLED` | `false` | all |

## 15. Testing

| Level | Tooling | What it proves |
|---|---|---|
| **Domain** | stdlib `testing`, table-driven, no I/O | Email normalisation and validation; registration emits exactly one `UserRegistered`; `PullEvents` drains once and is empty after; invariants reject bad input. |
| **Application** | Fakes for every port, injected clock and ids | Backoff arithmetic; a transient error breaks the batch and a permanent one does not; a transient error never reaches `failed`; duplicates short-circuit before any work; publish order follows `id`. |
| **Integration — Postgres** | `testcontainers-go`, PostgreSQL 18.6 | The unit of work drains events without the use case asking (register, assert one pending row); two concurrent relays claim disjoint rows under `SKIP LOCKED`; a killed relay's locks release and its rows are reclaimed; `ON CONFLICT` inbox returns "not first"; abort mid-transaction and assert *neither* row exists. |
| **Integration — Kafka** | `testcontainers-go` Kafka module, real broker | `acks=all` is awaited before marking; key routing puts one user's events on one partition; offsets are committed only after the database commit; a DLT record carries the original value and headers. |
| **Fault injection** | The §12.5 hooks against real infrastructure | Crash after publish → same `event_id` twice → one notification, one send, and the notifier logs a `duplicate=true` pass (§13.3). Crash before offset commit → redelivery absorbed. Gateway 503 → intent retried and eventually sent. Broker stopped mid-run → rows accumulate, `POST /users` keeps returning 201, everything drains on restart with nothing lost. |
| **End-to-end invariant** | 500 registrations while the relay and notifier are killed at random | `count(users) == count(notifications) == count(distinct inbox.event_id)`; every notification reaches `sent`; no row left `pending`; and the gateway *delivered* at most one email per `Idempotency-Key` — it may well have been *called* more than once with the same key, which is precisely what the key is for. This is the test that would catch a broken implementation. |
| **Architecture** | `go/packages` import walk | The layer and context rules of §6.2, plus per-context pool wiring. |

**No sleeps for synchronisation.** Unit tests inject a controllable clock;
integration tests use `waitFor(cond, timeout)` which polls and fails with the last
observed state. Sleeps are how outbox test suites become flaky, and a flaky suite
cannot prove an invariant.

## 16. Repository layout

```
cmd/
  api/main.go        relay/main.go        notifier/main.go        sender/main.go
  migrate/main.go

internal/
  accounts/
    domain/           user.go  events.go  errors.go  repository.go
    application/      register_user.go  publish_pending_batch.go  ports.go  envelope.go
    infrastructure/   postgres/{uow,user_repo,outbox_repo}.go
                      kafka/publisher.go  wakeup/notify.go  chaos/chaos.go
    presentation/     http/{router,handler,dto}.go  worker/{relay,purger}.go
  notifications/
    domain/           notification.go  events.go  errors.go  repository.go
    application/      process_user_registered.go  send_pending_batch.go  ports.go  envelope.go
    infrastructure/   postgres/{uow,inbox_repo,notification_repo,outbox_repo}.go
                      kafka/{consumer,dlt}.go  email/gateway.go
    presentation/     worker/{notifier,sender,purger}.go
  platform/
    config/  postgres/  kafka/  messaging/  logging/  ids/  clock/  admin/

migrations/
  accounts/0001_init.sql        notifications/0001_init.sql
deploy/
  docker-compose.yml            postgres:18.6 · apache/kafka:4.3.1 · the four services
  kafka-init.sh                 creates topics with partitions and retention
  postgres-init.sh              creates both databases
Makefile                        run · test · test-integration · migrate · lint · lag · demo
README.md                       how to run it, and the three guided failure demos
```

Both contexts have their own `postgres/uow.go` because each defines its own
`Work` set. Sharing one would mean one context's transaction handing out the
other's repositories — the coupling the database split exists to prevent. Shared
pgx and Kafka plumbing lives in `platform/`.

## 17. Dependencies

| Dependency | Purpose |
|---|---|
| Go 1.27.0 | `go.mod` declares `go 1.27`; `GOTOOLCHAIN=auto` fetches it if the local toolchain is older. |
| PostgreSQL 18.6 | `SKIP LOCKED`, `JSONB`, `LISTEN`/`NOTIFY`. One container, two databases. |
| Apache Kafka 4.3.1 | KRaft — no ZooKeeper since 4.0. One container. |
| `github.com/jackc/pgx/v5` | Driver and pool. Direct SQL, no ORM. |
| `github.com/twmb/franz-go` | Kafka client: pure Go, idempotent producer, consumer groups, manual commits, no cgo. |
| `github.com/google/uuid` | UUIDv7 via `uuid.NewV7`. |
| `github.com/pressly/goose/v3` | Migrations as a library over `embed.FS`. |
| `golang.org/x/sync/errgroup` | Goroutine lifecycle. |
| `github.com/testcontainers/testcontainers-go` | Test-only: Postgres and Kafka modules. |

Standard library for HTTP (`net/http`, Go 1.22+ method patterns), logging
(`log/slog`) and JSON.

## 18. Guarantees, and their limits

**Guaranteed**

- **Nothing is lost.** Every committed registration produces its event
  eventually, across process crashes, restarts, and broker outages of any length.
- **No phantom events.** No event is ever published for a rolled-back
  transaction.
- **At-least-once delivery**, with duplicates absorbed by the inbox.
- **At-most-once external effect.** The send intent is durable, and the
  `Idempotency-Key` makes a repeated call harmless.
- **Per-aggregate ordering.** All events for a user share a partition key, one
  partition is owned by one group member, and the relay is single-active.

**Not guaranteed — and this is the part people get wrong**

- **Not exactly-once.** Duplicates are inherent, not a bug to be fixed. §16: the
  relay's crash window cannot be closed, Kafka's idempotent producer covers only
  retries within a producer session, and Kafka transactions do not span
  PostgreSQL. What you get is at-least-once delivery plus an idempotent consumer,
  which produces exactly-once *effect*. That is the honest formulation, and it is
  the one this implementation makes true.
- **Not synchronous.** The event lands after the commit plus relay latency — tens
  to hundreds of milliseconds when polling. Consumers are eventually consistent
  with the producer.
- **Not globally ordered.** Only per key. Two different users' events have no
  defined relative order, and asking for global ordering means asking for one
  partition and no parallelism.
- **Ordering does not survive scaling the relay.** §12.4 states what it costs and
  what the alternatives are.
- **Demo durability is not production durability.** A single-broker Kafka with
  RF=1 cannot honour `min.insync.replicas >= 2`; §7 requires RF ≥ 3 and
  `min.insync.replicas = 2` for the durability `acks=all` is supposed to buy. The
  compose file says so in a comment, and the README says so in prose.

## 19. Anti-patterns explicitly avoided

§22's list, and where this design refuses each one.

| Anti-pattern | Refused by |
|---|---|
| Publishing inside the transaction | §11.1 rule 2 |
| After-commit hook as the delivery path | §11.1 rule 3 |
| Regenerating `event_id` per attempt | §8.1; the id is minted once at insert |
| Cursor-based claiming | §11.2 |
| Marking published without awaiting the ack | §10.1, `ProduceSync` |
| Treating a broker outage as a per-message failure | §12.1 / D8 |
| A non-idempotent consumer | §11.3 |
| Splitting the inbox write from the work | §11.3 |
| Acknowledging before committing | §10.2, §11.3 |
| Deduplicating in Redis or a Bloom filter | §11.3; the inbox is in the same transaction |
| An external call the inbox can swallow | §11.3, §13 remedy 2 |
| Business logic in the relay | §6.3; the relay never parses `payload` |
| Ordering by `occurred_at` | §8.1 |
| Two services sharing an outbox | D3, separate databases |
| Trusting broker deduplication for correctness | §18 |

## 20. Future work

In descending order of understanding gained per line of code:

1. Replace the polling publisher with Debezium's outbox event router (§11) and
   measure the latency difference. The ports are already drawn so that only
   `infrastructure/kafka` and the relay process change.
2. Add a second aggregate that emits several events in one transaction, so
   per-aggregate ordering becomes observable rather than merely designed for.
3. Shard the relay by `aggregate_id` hash and show ordering preserved under
   parallelism (§8).
4. Swap JSON for Avro against a schema registry with `BACKWARD` compatibility
   (§6), turning `payload` into `BYTEA`.
