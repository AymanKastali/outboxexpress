# Plan 3 — The Notifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `notifier` process joins the consumer group `notifications.welcome-email` on `accounts.user.v1`, and for each record opens one `oe_notifications` transaction that writes the inbox row, the `Notification` aggregate and — structurally, without the use case mentioning it — the send-intent row in `notifications.outbox`; commits; and only then commits the Kafka offset. Duplicates are absorbed by the inbox and reported, not hidden. A permanent error or a fifth delivery goes to the dead-letter topic before the offset is committed. An inbox purge job enforces the retention that bounds the deduplication guarantee.

**Architecture:** The consuming transaction is a use case (`notifications/application/process_user_registered.go`) that knows nothing about Kafka or SQL. In front of it sits the piece this plan exists to demonstrate: an **anti-corruption layer** (`translate.go`) that is the only code in the context that has ever seen accounts' field names. Behind it, a second unit of work — this context's own, over its own pool — drains the aggregate's events into this context's own outbox, exactly as Plan 1's does for accounts. The consumer group member, the offset commit and the retry ladder are a presentation worker, because deciding *when* to run a use case is a presentation concern (spec §6.3). Nothing in this plan touches `internal/accounts`.

**Tech Stack:** Go 1.27.0, PostgreSQL 18.6, Apache Kafka 4.3.1, `pgx/v5`, `github.com/twmb/franz-go`, `google/uuid`, `golang.org/x/sync/errgroup`, `testcontainers-go` (test-only), stdlib `net/http`, `log/slog`, `encoding/json`.

**Spec:** `docs/superpowers/specs/2026-08-26-transactional-outbox-demo-design.md` — read §6.5, §7, §8.2, §10.2, §11.3, §11.5, §12.2, §12.3, §13.2–§13.5 and §15 before starting. The plan argues from the spec; where the two disagree the spec wins and the plan is wrong, **except** for the four places recorded under "Divergences" below, each of which is a spec sketch that does not compile or does not respect a rule the spec states elsewhere.

---

## What Plans 1 and 2 left here

Read these before Task 1. Each is a decision already made and already committed, which this plan honours rather than revisits.

- **`platform/messaging` is the only shared type, and it is already half-built.** `messaging.Message` carries `EventID`, `EventType`, `SchemaVersion`, `Key`, `Payload`, `Headers`, `Topic`. Its doc comment says the consumer-side fields "arrive with the consumer that reads them, in Plan 3". Task 1 is that sentence coming due.
- **`platform/backoff` was written for this plan.** Its package doc says so: "§12.1's relay ladder and §12.2's consumer ladder are the same arithmetic with a different base… Each context declares its own one-method port over this." `internal/accounts/application/publishing.go` repeats it: the formula lives in platform "so that §12.2's consumer ladder can reuse it rather than hand-write the same overflow-prone doubling a second time." Task 10 declares the port and wires `backoff.Exponential`; it does not compute `50ms · 2^n` anywhere.
- **The header names are constants in `platform/messaging`, not string literals.** `HeaderEventID`, `HeaderEventType`, `HeaderSpecVersion`, `HeaderSchemaVersion`, `HeaderContentType`, `HeaderCorrelationID`, `HeaderTraceparent`. The relay writes them; this plan reads them. Two contexts agreeing on a spelling by each holding their own literal is how a contract breaks quietly. **The six `dlt_*` names of §12.2 join them there**, for the same reason and one more: §12.3's `replay-dlt` tool is specified with no notifier dependency, so a name held privately in `notifications/infrastructure` would be re-declared by its only other reader.
- **`platformpg.WithTx` owns begin/rollback/commit and returns `fn`'s error unwrapped**, so domain sentinels survive the trip out. This context's unit of work uses it. Do not write a second one.
- **`platformpg.Queryer` is the "runs on a transaction or a pool" interface** with `Exec` and `QueryRow` and deliberately no `Query`. The inbox's `ON CONFLICT` needs only `QueryRow`; do not widen the interface.
- **`internal/platform/kafka` is where franz-go meets the shared transport type.** `internal/accounts/infrastructure/kafka/publisher.go`'s `recordHeaders` says of itself: "Composing which headers exist is not done here — that is message contract and lives in the application layer. This function's entire job is the type change." A conversion whose signature names only `messaging.Message` and `kgo.Record` has no context in it, so both directions belong in platform — and `presentation` may import platform, which it may not do for `infrastructure`.
- **Each context has its own `pgxpool` and its own `uow.go`** (§16). There is no connection on which a cross-context transaction could be written, and `internal/arch`'s `TestNoProcessWiresBothContexts` already fails any `main` that opens both.
- **Ports live beside what they are about, not in a file called `ports.go`.** This is `internal/accounts/application/doc.go`'s stated convention as of commit `3055f06`, written down precisely so this plan would inherit the considered layout: the repository port sits beside the type it returns, the envelope factory beside the envelope it produces and its only implementation, a single-consumer port beside its consumer, and `machine.go` holds only the residue that genuinely belongs nowhere (`Clock`, `IDGen`).
- **The domain's `eventRecorder` is unexported and per-context, by design.** `internal/accounts/domain/recorder.go` says so: "the notifications context is a separate model and gets its own recorder if it needs one. Fifteen duplicated lines cost less than a type two bounded contexts must agree on." Copy it. Do not extract it into a shared kernel, and do not import the accounts one — `internal/arch` forbids it and it would be wrong even if it did not.
- **`domain.Event` exposes routing only** — `EventType`, `AggregateType`, `AggregateID`, `OccurredAt` — and deliberately has no `SchemaVersion`, because a wire version is message contract, not domain vocabulary. This context's `Event` interface is the same four methods for the same reasons, in its own package.
- **A use case never logs and never takes a logger.** It returns a result struct; the presentation layer logs it (§6.1, §13.3). Every number on a log line must be a field on a result struct, which is what makes it assertable in a unit test.
- **The lesson of `ce144d0`: `SIGTERM` is not a crash.** The relay drains the pass in flight for `RELAY_DRAIN_GRACE` because a produce that is acked but not yet marked is a duplicate the next pass will create. The notifier has the identical window — a database commit that has landed but whose offset is not yet committed — and Task 11 drains it for the identical reason. Do not implement the loop as `for { select { case <-ctx.Done(): return } }`.
- **The lesson of `f6f88ea`: observation is not the delivery path.** Any count-the-table query is pool-bound, read after the commit, and its failure is recorded on the result rather than returned. PostgreSQL aborts a transaction on any statement error, so an observability read inside one can undo a durable commit.
- **JSONB normalises stored JSON.** It sorts object keys and drops insignificant whitespace, so a payload read back out of `notifications.outbox` is semantically identical to but not byte-identical with what the envelope factory produced. Do not assert byte equality between the two.
- **`internal/platform/kafkatest` and `internal/platform/pgtest` are the only non-`_test.go` files allowed to contain test-only code**, because a container helper is imported by other packages' tests. `kafkatest` already gives you `Broker(t)`, `Producer(t)`, `Topic(t, partitions)` and — the one this plan keeps reaching for — `Records(t, topic, want)`, a deadline-bounded read-back that fails with the count it actually saw. Use it for every "assert the record arrived" step, including the dead-letter ones. The only addition this plan makes is a unique group name per test.
- **`pgtest.Notifications(t)` already truncates all three tables** before it returns, exactly as `pgtest.Accounts(t)` does. No integration test in this plan calls `Truncate` after it; a test that did would teach the next reader that it does not, which is how one gets truncated and two are left dirty.
- **The id generator is `ids.UUIDv7{}` and the clock is `clock.System{}`.** Two different packages; the plan names both explicitly wherever a test or a `main` needs one.
- **`internal/arch` already generalises over both contexts.** `TestBoundedContextsDoNotKnowEachOther` and `TestNoProcessWiresBothContexts` loop over `[]string{"accounts", "notifications"}`. They pass today because `internal/notifications` is empty; from Task 2 they are load-bearing. This plan adds no new arch rule and needs none.

---

## Global Constraints

Copied from the spec and from Plans 1 and 2. Every task's requirements implicitly include this section.

- **Go 1.27.0.** `go.mod` declares `go 1.27.0`; `GOTOOLCHAIN=auto` fetches it when the local toolchain is older.
- **PostgreSQL 18.6** (`postgres:18.6`). **Apache Kafka 4.3.1** (`apache/kafka:4.3.1`, KRaft, one container).
- **Module path:** `github.com/AymanKastali/outboxexpress`.
- **The dependency rule (§6.1).** `domain` imports stdlib and `github.com/google/uuid` only. `application` imports its own `domain` and `internal/platform/messaging` only. `presentation` and `infrastructure` are **siblings**: neither imports the other. `internal/arch` enforces all of it; obey it without waiting for the failing test.
- **The two contexts never import each other.** Not the domain, not the application, not a constant, not an error. They share `platform/messaging` and a topic name and nothing else (§7).
- **Nothing external inside a transaction (§11.1 rule 2).** No HTTP call, no produce, no email inside the consuming transaction. The Kafka offset commit happens *after* the database commit, from the worker, never from inside `Do`. D6's exception is the *relay's* pass and does not extend here — and the reason it does not is the whole argument of §11.3: the external send is a durable intent, not a call.
- **One record per transaction (§10.2).** A property of the loop, not a client setting. No batching, no multi-record transaction, even where it would be faster.
- **Offsets are committed with `CommitRecords` for the record just processed.** `kgo.DisableAutoCommit()` is set and there is no commit ticker anywhere in this plan.
- **`state` is a domain type, not a string (§8.2).** `NotificationState` is a value object; `MarkSent` and `MarkFailed` are methods that refuse an illegal transition. The `CHECK` constraint is the database's backstop, not the definition.
- **`idempotency_key` on `notifications.outbox` is the inbound `event_id`** (§8.2). It is never regenerated, and it is a column rather than a payload field because the sender is a dumb pipe that routes on columns.
- **`ErrPermanent` means the message will never become valid.** A malformed or unmapped record goes to the dead-letter topic; it does not enter a retry loop. `ErrTransient` means try again. Every adapter error wraps exactly one, with `%w`.
- **No metrics system, no registry, no `/metrics` (§13.3).** Observability is structured `slog` lines whose every field comes from a result struct.
- **No ORM, no web framework, no CLI framework, no configuration library** (§5 D12, §14).
- **No sleeps for synchronisation in tests (§15).** Unit tests inject a controllable clock; integration tests poll with a deadline and fail printing the last observed state.
- **No test-only code in non-test files**, except `internal/platform/pgtest` and `internal/platform/kafkatest`.
- **Files read top-down: the stepdown rule.** Named constants first; then the headline type the file is named for; its constructor; its exported methods in the order a caller meets them; the private helpers each calls, in call order; and last the low-level details — SQL text, lookup tables, sentinel errors, boundary structs. A `var _ Iface = (*T)(nil)` assertion goes at the very bottom. Test files put tests first, fakes and helpers below.
- **Every commit message explains why, not what.** The diff already says what. End each with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- **Commit directly to `main`.** No branches, no pull requests. Each task's commit is the review unit.

---

## Divergences from the spec found while planning

The spec's §11.3 listing is a sketch that illustrates the shape of the consuming transaction. Four details in it do not survive contact with the rules the spec states elsewhere, or with the compiler. Each is recorded here rather than silently "fixed", so that a reader who has just read §11.3 is not left wondering which of the two is wrong.

**1. `msg.Metadata()` cannot exist.** The sketch calls a method on `messaging.Message` that returns this context's `application.Metadata`. `platform/messaging` is shared by both contexts (§6.5) and cannot import either one's application package, and a `Metadata` type promoted into `platform` would be the shared domain model §6.5 explicitly refuses to be. **This plan** reads the two ambient headers in the notifications application layer, in `metadataFrom(msg.Headers)` beside the `Metadata` type itself (Task 6), using the `messaging.Header*` constants. Same result, no shared type.

**2. `uc.ids.New()` is shown without an error return.** `application.IDGen` returns `(uuid.UUID, error)` — `uuid.NewV7` reads entropy and can fail, and Plan 1 chose to surface that rather than swallow it. The sketch's single-value call would not compile. **This plan** handles the error, and classifies it as **transient**: a failed read of the entropy pool is a machine problem, not a bad message, and dead-lettering a perfectly valid record because `/dev/urandom` hiccuped would be the wrong permanent decision.

**3. The package qualifier is `domain`, not `notification`.** The sketch writes `notification.RequestWelcome(...)`; §16's layout puts the file at `internal/notifications/domain/notification.go`, which makes the package `domain` and the call `domain.RequestWelcome(...)`. The sketch's qualifier is a naming aspiration from an earlier draft.

**4. The inbox insert is shown returning `(first bool, err error)`, and that is right, but the sketch's ordering hides a subtlety worth stating.** `INSERT … ON CONFLICT DO NOTHING` reports "no rows" for a duplicate, which is what `first` carries. It must be the *first* statement in the transaction, before any other write, so that a duplicate costs one round trip and touches nothing — and, more importantly, so that the notification insert and the outbox drain are unreachable for a record already processed. Task 7's integration test asserts that a duplicate leaves `notifications.outbox` untouched, because "already processed" that still emits a second send-intent would be a duplicate email with an inbox that thinks it did its job.

**5. `messaging.Message` does not get an `Attempt` field, though §6.5 lists one.** §6.5 is right that the count is process-local and right that Kafka has no delivery counter. It is wrong that the number belongs on the type both contexts share. Trace the field: the worker would write it, and the worker would read it two lines later — one writer and one reader, in one function, in one context. The dead-letter adapter does not read it; it reads `DeadLetterReason.Deliveries`, which already carries the number to the only other place that wants it. What the field buys is nothing; what it costs is a paragraph on the wire contract explaining that this one member is not part of the wire contract, must never be persisted and must never be published — three prohibitions that a local variable makes unrepresentable. §6.5's own argument against a shared domain model is the argument against it. **This plan adds `Partition` and `Offset`** — those are facts about a fetched record, and the dead-letter headers need them — **and keeps the count as a local in the worker.**

**6. The inbox claim moves from the use case into the unit of work.** §11.3's sketch has the use case call `w.Inbox.Record(...)` as the first statement inside `Do`, and the plan's first draft followed it — which put the design's headline invariant in a call site defended by a comment and a test whose failure mode is silent: with the two writes swapped, every assertion about a duplicate still passes, because a duplicate is still reported as a duplicate. It has just inserted a notification first.

That is precisely the situation D4 exists to refuse, and the plan was arguing both sides of it two paragraphs apart — reproducing accounts' "an invariant that lives in a call site is an invariant someone can forget" for the outbox, and then making an exception for the inbox. So the boundary takes it: `Do(ctx, meta, claim, fn)` issues the `ON CONFLICT DO NOTHING` as the transaction's first statement and, when the row already exists, **never calls `fn` at all**. `Work` holds only the notification repository. "Inbox first" stops being a rule and becomes the only thing the type system permits, the ordering test and its silent failure mode both disappear, and the boundary ends up owning both side-tables symmetrically: the deduplication write before the work, the event drain after it.

A seventh, smaller note: §11.3's table of wrong shapes is worth copying into the use case's doc comment nearly verbatim. It is the clearest statement in the spec of why this transaction is not optional, and it belongs where someone editing the transaction will read it.

---

## File Structure

**Created:**

```
internal/notifications/
  domain/
    recipient.go            Recipient — this context's email value object
    user_ref.go             UserRef — a foreign aggregate, referenced by id only
    state.go                NotificationState + its legal transitions
    errors.go               ErrInvalidRecipient, ErrInvalidUserRef, ErrIllegalTransition
    notification.go         the Notification aggregate: RequestWelcome, MarkSent, MarkFailed
    events.go               Event interface, WelcomeEmailRequested
    recorder.go             eventRecorder — copied per §6.3, not shared
    repository.go           NotificationRepository (domain port, per D5)
  application/
    doc.go                  package doc + the port-placement convention
    machine.go              Clock, IDGen
    unit_of_work.go         Metadata, metadataFrom, Work, UnitOfWork
    inbox.go                InboxClaim — what the boundary deduplicates on
    envelope.go             Envelope (with IdempotencyKey), EnvelopeFactory, CloudEventFactory
    translate.go            the anti-corruption layer: WelcomeRequest, translate
    consuming.go            ErrTransient, ErrPermanent, Schedule, DeadLetterPublisher
    process_user_registered.go   the consuming transaction
    purge_inbox.go          the retention job (§13.2)
  infrastructure/
    postgres/
      uow.go                the boundary: inbox claim, then fn, then the event drain
      tracker.go            records which aggregates were persisted
      inbox_repo.go         Record: ON CONFLICT DO NOTHING -> first bool
      notification_repo.go  Insert
      outbox_repo.go        Append, including idempotency_key
      inbox_purge.go        the pool-bound delete loop
    kafka/
      dlt.go                DeadLetterPublisher: original record + dlt_* headers
  presentation/
    worker/
      notifier.go           the poll loop: retry ladder, DLT, offset commit, drain
      purger.go             the inbox purge ticker
cmd/notifier/main.go
```

**Modified:**

```
internal/platform/messaging/message.go        + Partition, Offset (§6.5, minus Attempt)
                                              + the six dlt_* header names (§12.2)
                                              + SchemaBase, promoted from two copies
internal/platform/kafka/client.go             + NewConsumer, + RecordToMessage
                                              + recordHeaders, moved from accounts
internal/accounts/infrastructure/kafka/publisher.go   calls the moved recordHeaders
internal/accounts/application/envelope.go     reads messaging.SchemaBase
internal/platform/kafkatest/kafkatest.go      + ConsumerGroup(t), a unique group per test
internal/platform/config/config.go            + LoadNotifier
deploy/docker-compose.yml                     + the notifier service
deploy/kafka-init.sh                          + the dead-letter topic
Makefile                                      + notifier targets
README.md                                     the consuming half, and what to watch
.env.example                                  the notifier's variables
docs/superpowers/specs/...-design.md          §11.3's sketch annotated with the four divergences
```

Three notes on the layout, all following from the convention `doc.go` records.

`inbox.go` holds `InboxClaim` and the argument for the mechanism. The *repository* interface is not here: its only caller is the unit of work, which lives in `infrastructure/postgres`, so it is declared there beside `outboxAppender` — the precedent accounts already set for a port whose consumer is the boundary rather than a use case. What stays in the application layer is the value the use case passes in and the paragraph explaining why the boundary deduplicates before it does anything else.

`consuming.go` holds the error classes, the retry schedule and the dead-letter port together because they are one idea from three directions: `ErrTransient` is the classification that earns a retry, `Schedule` is how long to wait, and `DeadLetterPublisher` is what happens when retrying is no longer the answer. Splitting them would put the question and its answers in different files.

There is no `consumer.go` under `infrastructure/kafka`. The record-to-message conversion names only `kgo.Record` and `messaging.Message`, so it has no context in it and belongs in `platform/kafka` — which is also the only way the worker can call it, since `presentation` may not import `infrastructure`.

---

## Task 1: The consumer-side fields of the shared envelope

**Files:**
- Modify: `internal/platform/messaging/message.go`
- Test: `internal/platform/messaging/message_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `messaging.Message` gains `Partition int32` and `Offset int64`. It does **not** gain `Attempt`; see divergence 5.

The whole task is two fields and one paragraph. The paragraph matters because the paragraph is where a field that does not belong on a shared contract type gets refused.

- [ ] **Step 1: Write the failing test**

Add to `internal/platform/messaging/message_test.go`:

```go
// The coordinates exist so a worker can identify a record — and so the
// dead-letter headers of §12.2 can say where it came from — without keeping a
// parallel map keyed by something it invented.
func TestMessage_CarriesTheRecordCoordinates(t *testing.T) {
	msg := Message{
		EventID:   "0199a4f0-0000-7000-8000-000000000001",
		Topic:     "accounts.user.v1",
		Partition: 3,
		Offset:    4711,
	}
	if msg.Partition != 3 || msg.Offset != 4711 {
		t.Errorf("coordinates = %d/%d, want 3/4711", msg.Partition, msg.Offset)
	}
}

// A produced message has no coordinates, and the zero values are not a sentinel:
// partition 0 offset 0 is a real record. The field that says whether these mean
// anything is Topic — a message this process did not fetch has none. This test
// pins the absence of a -1-means-unset convention; nothing should add one.
func TestMessage_AProducedMessageHasNoCoordinates(t *testing.T) {
	msg := Message{EventID: "x", Topic: "accounts.user.v1", Payload: []byte("{}")}
	if msg.Partition != 0 || msg.Offset != 0 {
		t.Errorf("produced message has coordinates: %d/%d", msg.Partition, msg.Offset)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/platform/messaging/ -run TestMessage_Carries -v`
Expected: FAIL — `unknown field Partition in struct literal`.

- [ ] **Step 3: Add the fields and rewrite the paragraph that promised them**

In `internal/platform/messaging/message.go`, replace the final paragraph of the `Message` doc comment — the one beginning "The consumer-side fields of §6.5 … arrive with the consumer that reads them, in Plan 3" — with the text below, and add the two fields after `Topic`:

```go
// Partition and Offset are a fetched record's coordinates. They are set by the
// consumer adapter, they are empty on a produced message, and that is not a
// sentinel convention: partition 0 offset 0 is a real record, and the field that
// tells you whether these mean anything is Topic — a message the process did not
// fetch has none.
//
// §6.5 also lists an Attempt field, and this type deliberately does not have one.
// §6.5 is right that Kafka has no delivery counter and right that a retry count is
// process-local; it is wrong that the number belongs here. Its only writer and its
// only reader are two lines apart inside one worker in one context, so what the
// field would buy is nothing, and what it would cost is a paragraph on the wire
// contract explaining that this one member is not part of the wire contract, must
// never be persisted, and must never be published. A local variable makes all
// three unrepresentable. The count travels to the one other place that wants it —
// the dead-letter headers — as a field on the reason the notifier hands its
// dead-letter port (§12.2).
type Message struct {
	EventID       string
	EventType     string
	SchemaVersion int
	Key           string
	Payload       []byte
	Headers       map[string]string
	Topic         string
	Partition     int32
	Offset        int64
}
```

- [ ] **Step 4: Add the dead-letter header names and promote `SchemaBase`**

Both belong in this package for the reason its existing header block states: the producer and the consumer must not each hold their own spelling.

```go
// The dead-letter header names of §12.2. They are here rather than in the
// notifier's adapter because a DLT record has two readers — the notifier that
// writes it and §12.3's replay tool, which is specified to have no notifier
// dependency and would therefore re-declare all six.
const (
	HeaderDLTReason          = "dlt_reason"
	HeaderDLTError           = "dlt_error"
	HeaderDLTDeliveries      = "dlt_deliveries"
	HeaderDLTOriginalTopic   = "dlt_original_topic"
	HeaderDLTOriginalPart    = "dlt_original_partition"
	HeaderDLTOriginalOffset  = "dlt_original_offset"
)

// SchemaBase is the namespace of the dataschema URI EncodeCloudEvent formats. It
// is deployment-wide, like SpecVersion and TimeFormat beside it — two copies in
// two contexts would drift into two published schema namespaces with nothing
// failing. What is legitimately per-context is `source`, and that stays there.
const SchemaBase = "https://schemas.outboxexpress.dev"
```

Then change `internal/accounts/application/envelope.go` to read `messaging.SchemaBase` instead of its own `schemaBase` constant, keeping its `source` where it is.

- [ ] **Step 5: Run the tests**

Run: `go test ./internal/platform/messaging/ ./internal/accounts/... -v`
Expected: PASS. Accounts' envelope tests assert the full `dataschema` URI, so a mistake in Step 4 fails there.

- [ ] **Step 6: Prove nothing else moved**

Run: `make lint && go test ./...`
Expected: green. A positional `Message{...}` literal anywhere would fail here; there should be none.

- [ ] **Step 7: Commit**

```bash
git add internal/platform/messaging internal/accounts/application
git commit -m "feat(messaging): the record coordinates, and the one field 6.5 asks for that does not belong here"
```

---

## Task 2: The notifications domain — value objects

**Files:**
- Create: `internal/notifications/domain/recipient.go`, `user_ref.go`, `state.go`, `errors.go`
- Test: `internal/notifications/domain/recipient_test.go`, `user_ref_test.go`, `state_test.go`

**Interfaces:**
- Consumes: nothing. This package imports stdlib and `github.com/google/uuid` and nothing else, forever.
- Produces: `NewRecipient(string) (Recipient, error)`, `Recipient.String() string`; `NewUserRef(uuid.UUID) (UserRef, error)`, `UserRef.ID() uuid.UUID`; `NotificationState` with `StatePending`/`StateSent`/`StateFailed`, `NotificationState.String() string`, `NotificationState.canTransitionTo(NotificationState) bool`; the sentinels in `errors.go`.

There is no `ParseState`. This plan has no read path — the repository is `Insert`-only (Task 3), nothing scans a `state` column, and a round-trip parser with no caller is a branch whose own test has to concede it is unreachable. It belongs in the commit that adds the sender's claiming query, where a round-trip test has something real to protect. Same for `UserRef.String()`: `insertNotification` passes a `uuid.UUID`, so the "column form" accessor has no column.

Three value objects, and the interesting one is `UserRef`. It is the shape of a bounded-context boundary expressed in the type system: accounts' `User` is a different aggregate in a different context, and the only honest thing this context can hold is its identity. A copy of accounts' user here would be one model with a network in the middle (§11.3).

- [ ] **Step 1: Write `errors.go`**

```go
package domain

import "errors"

// The domain's vocabulary of refusal. Sentinels, so that an outer layer can map
// them — the application layer to ErrPermanent, a test to an assertion — without
// matching on message text.
//
// The prefix is "notifications:" and it is not decoration. These errors travel
// out of this context on a dead-letter record's dlt_error header, where the only
// clue about which of four processes refused a message is the text of the error.
var (
	ErrInvalidRecipient = errors.New("notifications: recipient is not a valid address")
	ErrInvalidUserRef   = errors.New("notifications: user reference must not be the nil UUID")
	ErrInvalidID        = errors.New("notifications: notification id must not be the nil UUID")

	// ErrIllegalTransition is the state machine refusing, and it exists as a
	// sentinel because the sender needs to tell "this notification was already
	// sent" apart from "this notification could not be sent". The first is a
	// duplicate delivery absorbed; the second is an incident.
	ErrIllegalTransition = errors.New("notifications: illegal notification state transition")
)
```

- [ ] **Step 2: Write the failing tests for `Recipient`**

`internal/notifications/domain/recipient_test.go`:

```go
package domain

import (
	"errors"
	"testing"
)

func TestNewRecipient_NormalisesAndValidates(t *testing.T) {
	tests := []struct {
		name, raw, want string
		wantErr         error
	}{
		{name: "plain", raw: "ada@example.com", want: "ada@example.com"},
		{name: "folds case and space", raw: "  Ada@Example.COM ", want: "ada@example.com"},
		{name: "empty", raw: "", wantErr: ErrInvalidRecipient},
		{name: "no domain dot", raw: "ada@localhost", wantErr: ErrInvalidRecipient},
		{name: "display-name form", raw: "Ada <ada@example.com>", wantErr: ErrInvalidRecipient},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewRecipient(tc.raw)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && got.String() != tc.want {
				t.Errorf("Recipient = %q, want %q", got.String(), tc.want)
			}
		})
	}
}

// Comparable by value is the property that makes the type worth having: a
// recipient differing only in case is the same recipient, and no call site has to
// remember to fold before comparing.
func TestRecipient_IsComparableByValue(t *testing.T) {
	a, _ := NewRecipient("ada@example.com")
	b, _ := NewRecipient("ADA@example.com ")
	if a != b {
		t.Errorf("%v != %v; normalisation is what makes == mean what the domain means", a, b)
	}
}
```

- [ ] **Step 3: Run it and watch it fail**

Run: `go test ./internal/notifications/domain/ -run TestNewRecipient -v`
Expected: FAIL — `undefined: NewRecipient`.

- [ ] **Step 4: Write `recipient.go`**

```go
package domain

import (
	"net/mail"
	"strings"
)

// maxRecipientLength is RFC 3696's local part plus domain.
const maxRecipientLength = 320

// Recipient is a validated destination address, and it is *this context's* email
// type rather than a copy of accounts' Email.
//
// The duplication is the point. Accounts validates an address because a user is
// registering one; notifications validates an address because it is about to send
// mail to it, and the day the two rules differ — a suppression list, a
// role-account rule, an internal-domain restriction — belongs to this file and
// affects nothing upstream. A shared Email type would make that day a
// negotiation between two contexts (spec §6.3, §7).
//
// It normalises on the way in, which makes the type comparable by value.
type Recipient struct {
	value string
}

// NewRecipient normalises and validates one address, rejecting anything that is
// not a bare addr-spec.
func NewRecipient(raw string) (Recipient, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" || len(s) > maxRecipientLength {
		return Recipient{}, ErrInvalidRecipient
	}
	parsed, err := mail.ParseAddress(s)
	if err != nil {
		return Recipient{}, ErrInvalidRecipient
	}
	// Requiring the parse to be a no-op rejects every valid address *expression*
	// that is not a bare address — the display-name and angle-bracket forms —
	// because storing either would make the recipient column mean two things.
	if parsed.Address != s {
		return Recipient{}, ErrInvalidRecipient
	}
	_, domainPart, ok := strings.Cut(s, "@")
	if !ok || !strings.Contains(domainPart, ".") {
		return Recipient{}, ErrInvalidRecipient
	}
	return Recipient{value: s}, nil
}

// String returns the normalised address, which is what an adapter writes to a
// column or hands to a gateway.
func (r Recipient) String() string { return r.value }
```

- [ ] **Step 5: Run it and watch it pass**

Run: `go test ./internal/notifications/domain/ -run TestNewRecipient -v && go test ./internal/notifications/domain/ -run TestRecipient_Is -v`
Expected: PASS.

- [ ] **Step 6: Write the failing tests for `UserRef`**

`internal/notifications/domain/user_ref_test.go`:

```go
package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNewUserRef_RejectsTheNilUUID(t *testing.T) {
	if _, err := NewUserRef(uuid.Nil); !errors.Is(err, ErrInvalidUserRef) {
		t.Fatalf("err = %v, want ErrInvalidUserRef", err)
	}
	id := uuid.MustParse("0199a4f0-0000-7000-8000-000000000001")
	ref, err := NewUserRef(id)
	if err != nil {
		t.Fatalf("NewUserRef: %v", err)
	}
	if ref.ID() != id {
		t.Errorf("ID() = %v, want %v", ref.ID(), id)
	}
}

// The test that documents the design: UserRef holds an id and has nowhere to put
// anything else. If this stops compiling because someone added a field, the
// bounded-context boundary moved and this comment is why it should not have.
func TestUserRef_HoldsNothingButAnID(t *testing.T) {
	id := uuid.MustParse("0199a4f0-0000-7000-8000-000000000001")
	a, _ := NewUserRef(id)
	b, _ := NewUserRef(id)
	if a != b {
		t.Error("two references to the same user are not equal; UserRef has gained state")
	}
}
```

- [ ] **Step 7: Run it, watch it fail, write `user_ref.go`**

Run: `go test ./internal/notifications/domain/ -run TestUserRef -v` → FAIL, `undefined: NewUserRef`. Then:

```go
package domain

import "github.com/google/uuid"

// UserRef is a foreign aggregate referenced by identity, and nothing else.
//
// Accounts' User lives in another bounded context, in another database, behind a
// topic. This context cannot load it, cannot validate it, and cannot know when it
// changes — so the only truthful thing to hold is its id. A struct here with an
// email and a display name copied off an event would look convenient and would be
// a second, stale, unmaintained model of someone else's aggregate: one model with
// a network in the middle (spec §11.3).
//
// Everything this context needs *about* the user at send time — the address, the
// name in the greeting — arrives on the event that requested the notification and
// is stored on the Notification, as of the moment it occurred. That is
// event-carried state transfer (§9.1), and it is why this type can afford to be
// this small.
type UserRef struct {
	id uuid.UUID
}

// NewUserRef refuses the nil UUID, which is the only invalid id this context can
// detect without calling accounts — and calling accounts is the thing it must
// never do.
func NewUserRef(id uuid.UUID) (UserRef, error) {
	if id == uuid.Nil {
		return UserRef{}, ErrInvalidUserRef
	}
	return UserRef{id: id}, nil
}

func (u UserRef) ID() uuid.UUID { return u.id }
```

Run again: PASS.

- [ ] **Step 8: Write the failing tests for `NotificationState`**

`internal/notifications/domain/state_test.go`:

```go
package domain

import "testing"

// The state machine, asserted as a table. This is the definition; the database's
// CHECK constraint is the backstop (spec §8.2).
func TestNotificationState_LegalTransitions(t *testing.T) {
	tests := []struct {
		from, to NotificationState
		want     bool
	}{
		{StatePending, StateSent, true},
		{StatePending, StateFailed, true},
		{StateSent, StateFailed, false},   // sent is terminal
		{StateSent, StateSent, false},     // and not idempotent: a second send is an event
		{StateFailed, StateSent, false},   // a retry re-requests; it does not resurrect
		{StateFailed, StateFailed, false},
		{StatePending, StatePending, false},
	}
	for _, tc := range tests {
		if got := tc.from.canTransitionTo(tc.to); got != tc.want {
			t.Errorf("%v -> %v = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}
```

- [ ] **Step 9: Run it, watch it fail, write `state.go`**

Run: `go test ./internal/notifications/domain/ -run TestNotificationState -v` → FAIL, `undefined: NotificationState`. Then:

```go
package domain

// The three legal states of a notification (spec §8.2). A defined type rather
// than string constants, so that a function taking a NotificationState cannot be
// handed "snet", and so the transition rules have somewhere to live that is not a
// call site.
type NotificationState string

const (
	StatePending NotificationState = "pending"
	StateSent    NotificationState = "sent"
	StateFailed  NotificationState = "failed"
)

func (s NotificationState) String() string { return string(s) }

// canTransitionTo is the state machine. `sent` is terminal and `pending` is the
// only legal predecessor of either outcome (spec §8.2).
//
// Note what is absent: pending -> pending and sent -> sent are both illegal. A
// self-transition looks harmless and would make MarkSent idempotent, which is
// precisely the wrong shape here — the sender calling MarkSent twice means the
// gateway was called twice for one intent, and the aggregate refusing is how that
// surfaces instead of being absorbed. Idempotency in this design lives in the
// inbox and the Idempotency-Key, both checked *before* any work happens; an
// aggregate that also shrugged at repeats would hide the failure of the
// mechanisms that are supposed to prevent them.
func (s NotificationState) canTransitionTo(to NotificationState) bool {
	return s == StatePending && (to == StateSent || to == StateFailed)
}
```

Run: PASS.

- [ ] **Step 10: Verify the dependency rule already holds**

Run: `go test ./internal/arch/ -v`
Expected: PASS. `TestDomainImportsOnlyStdlibAndUUID` now has a second context to walk; `net/mail`, `strings` and `uuid` are all it will find.

- [ ] **Step 11: Commit**

```bash
git add internal/notifications/domain
git commit -m "feat(notifications): the value objects, and a foreign aggregate that is only an id"
```

---

## Task 3: The `Notification` aggregate

**Files:**
- Create: `internal/notifications/domain/notification.go`, `events.go`, `recorder.go`, `repository.go`
- Test: `internal/notifications/domain/notification_test.go`, `recorder_test.go`

**Interfaces:**
- Consumes: Task 2's `Recipient`, `UserRef`, `NotificationState` and the sentinels.
- Produces: `RequestWelcome(id uuid.UUID, to Recipient, user UserRef, sourceEventID uuid.UUID, now time.Time) (*Notification, error)`; accessors `ID()`, `UserID()`, `Recipient()`, `Kind()`, `State()`, `SourceEventID()`, `CreatedAt()`, `SentAt()`; transitions `MarkSent(at time.Time) error`, `MarkFailed() error`; `PullEvents() []Event`; the `Event` interface; `WelcomeEmailRequested`; `NotificationRepository`.

`RequestWelcome` is a request, not a send. The name is the whole design in one verb: this context records that an email *should* be sent and emits an event saying so; the send is a durable intent for another process (§11.3). If this constructor were called `SendWelcome`, every reader would look for the SMTP call and every future maintainer would eventually add one.

- [ ] **Step 1: Write `recorder.go`**

The accounts recorder, copied, with the comment updated to say which direction the copy went — a reader who finds two near-identical files deserves to know it was a decision.

```go
package domain

// eventRecorder is the recording half of an aggregate root, kept in one type so
// that no aggregate carries a bare slice it could append to, read twice, or
// forget to drain.
//
// Every aggregate in this context embeds it. Its zero value is ready to use, and
// the slice stays unexported — the only ways to move an event in or out are
// record and PullEvents.
//
// This is a deliberate copy of accounts' recorder, not an oversight and not a
// candidate for extraction. That file says why: "the notifications context is a
// separate model and gets its own recorder if it needs one. Fifteen duplicated
// lines cost less than a type two bounded contexts must agree on" (spec §6.3).
// The two Event interfaces are already different types; a shared recorder would
// need a type parameter, and a shared kernel between two contexts is a coupling
// that outlives whoever introduced it.
type eventRecorder struct {
	events []Event
}

// record appends one event. Aggregates call this only after their invariants have
// accepted the change — an event is a record of something that happened, so
// recording one that was refused would be a lie the outbox then publishes.
func (r *eventRecorder) record(e Event) { r.events = append(r.events, e) }

// PullEvents returns the recorded events and forgets them. Draining is what makes
// a second commit of the same aggregate emit nothing: an aggregate that re-emitted
// would turn one state change into two send-intents, and two send-intents are two
// emails unless the gateway's idempotency key saves us. It should not have to.
func (r *eventRecorder) PullEvents() []Event {
	events := r.events
	r.events = nil
	return events
}
```

`recorder_test.go` is the accounts test with the notifications event type substituted: record two, `PullEvents` returns both in order, a second `PullEvents` returns nothing.

- [ ] **Step 2: Write `events.go`**

```go
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Event is what an aggregate emits when its state changes. Routing fields are
// methods rather than payload data because the sender routes on them and must
// never parse business content: AggregateType selects the destination,
// AggregateID becomes the key.
//
// It is the same four methods as accounts' Event, in a different package, and
// they are not the same type. That is not duplication to be cleaned up — it is
// what "separate model" means. A shared Event interface would make every future
// change to either context's event vocabulary a cross-context change.
//
// There is deliberately no SchemaVersion, for the reason accounts states: a wire
// schema version is message contract, not domain vocabulary. The application
// layer names the version alongside the schema it belongs to.
type Event interface {
	EventType() string
	AggregateType() string
	AggregateID() string
	OccurredAt() time.Time
}

const (
	// AggregateTypeNotification selects the destination at send time.
	AggregateTypeNotification = "Notification"

	// KindWelcomeEmail is the one kind of notification this design sends. It is a
	// column value, so it is a constant here rather than a literal in the
	// repository — the domain names its own kinds.
	KindWelcomeEmail = "welcome_email"

	// EventTypeWelcomeEmailRequested is reverse-DNS and versionless, like
	// accounts' (spec §9.3). Note the tense: requested, not sent. The event
	// records that this context decided an email should go out. Whether it went
	// out is the sender's story, and it ends in a state transition rather than a
	// second event, because nothing subscribes to it.
	EventTypeWelcomeEmailRequested = "com.outboxexpress.notifications.welcome_email.requested"
)

// WelcomeEmailRequested is the send-intent. Everything the sender needs to make
// the call is here, so that performing it requires no join and no second read:
// the address, the greeting, the notification's own id, and the source event id
// that becomes the Idempotency-Key (§8.2, §11.4).
type WelcomeEmailRequested struct {
	NotificationID uuid.UUID
	UserID         uuid.UUID
	Recipient      Recipient
	SourceEventID  uuid.UUID
	RequestedAt    time.Time
}

func (e WelcomeEmailRequested) EventType() string     { return EventTypeWelcomeEmailRequested }
func (e WelcomeEmailRequested) AggregateType() string { return AggregateTypeNotification }
func (e WelcomeEmailRequested) AggregateID() string   { return e.NotificationID.String() }
func (e WelcomeEmailRequested) OccurredAt() time.Time { return e.RequestedAt }
```

- [ ] **Step 3: Write the failing tests for the aggregate**

`internal/notifications/domain/notification_test.go`:

```go
package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testID     = uuid.MustParse("0199a4f0-0000-7000-8000-00000000000a")
	testUserID = uuid.MustParse("0199a4f0-0000-7000-8000-00000000000b")
	testSource = uuid.MustParse("0199a4f0-0000-7000-8000-00000000000c")
	testNow    = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
)

func newRequest(t *testing.T) *Notification {
	t.Helper()
	to, err := NewRecipient("ada@example.com")
	if err != nil {
		t.Fatalf("NewRecipient: %v", err)
	}
	user, err := NewUserRef(testUserID)
	if err != nil {
		t.Fatalf("NewUserRef: %v", err)
	}
	n, err := RequestWelcome(testID, to, user, testSource, testNow)
	if err != nil {
		t.Fatalf("RequestWelcome: %v", err)
	}
	return n
}

func TestRequestWelcome_StartsPendingAndRecordsExactlyOneEvent(t *testing.T) {
	n := newRequest(t)

	if n.State() != StatePending {
		t.Errorf("State() = %v, want pending", n.State())
	}
	if n.Kind() != KindWelcomeEmail {
		t.Errorf("Kind() = %q, want %q", n.Kind(), KindWelcomeEmail)
	}
	if !n.SentAt().IsZero() {
		t.Errorf("SentAt() = %v, want the zero time on a pending notification", n.SentAt())
	}

	events := n.PullEvents()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want exactly 1", len(events))
	}
	req, ok := events[0].(WelcomeEmailRequested)
	if !ok {
		t.Fatalf("event is %T, want WelcomeEmailRequested", events[0])
	}
	// The source event id must survive into the event, because it becomes the
	// Idempotency-Key on the gateway call (§8.2). Losing it here would leave the
	// sender generating a key of its own, which would deduplicate nothing across
	// a retry.
	if req.SourceEventID != testSource {
		t.Errorf("SourceEventID = %v, want %v", req.SourceEventID, testSource)
	}
	if req.AggregateID() != testID.String() {
		t.Errorf("AggregateID() = %q, want the notification id", req.AggregateID())
	}
}

func TestRequestWelcome_RejectsTheNilID(t *testing.T) {
	to, _ := NewRecipient("ada@example.com")
	user, _ := NewUserRef(testUserID)
	if _, err := RequestWelcome(uuid.Nil, to, user, testSource, testNow); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("err = %v, want ErrInvalidID", err)
	}
}

// The source event id is what notifications.source_event_id UNIQUE enforces, and
// §8.2 calls that constraint "redundant with the inbox by design". A nil value
// would make the constraint useless for the second and every later notification,
// so the aggregate refuses it here rather than discovering it as a unique
// violation on the second welcome email ever sent.
func TestRequestWelcome_RejectsANilSourceEventID(t *testing.T) {
	to, _ := NewRecipient("ada@example.com")
	user, _ := NewUserRef(testUserID)
	if _, err := RequestWelcome(testID, to, user, uuid.Nil, testNow); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("err = %v, want ErrInvalidID", err)
	}
}

func TestNotification_MarkSent(t *testing.T) {
	n := newRequest(t)
	sentAt := testNow.Add(2 * time.Second)

	if err := n.MarkSent(sentAt); err != nil {
		t.Fatalf("MarkSent: %v", err)
	}
	if n.State() != StateSent {
		t.Errorf("State() = %v, want sent", n.State())
	}
	if !n.SentAt().Equal(sentAt) {
		t.Errorf("SentAt() = %v, want %v", n.SentAt(), sentAt)
	}
}

// The transition table, from the aggregate's side. A second MarkSent is the case
// that matters: it means the gateway was called twice for one intent, and the
// aggregate refusing is how that reaches a log line instead of being absorbed.
func TestNotification_RefusesIllegalTransitions(t *testing.T) {
	sent := newRequest(t)
	if err := sent.MarkSent(testNow); err != nil {
		t.Fatalf("MarkSent: %v", err)
	}
	if err := sent.MarkSent(testNow); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("second MarkSent err = %v, want ErrIllegalTransition", err)
	}
	if err := sent.MarkFailed(); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("MarkFailed after sent err = %v, want ErrIllegalTransition", err)
	}

	failed := newRequest(t)
	if err := failed.MarkFailed(); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if err := failed.MarkSent(testNow); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("MarkSent after failed err = %v, want ErrIllegalTransition", err)
	}
}

// A refused transition must change nothing. A state machine that rejects the move
// but keeps the timestamp is worse than one with no rules, because the row then
// disagrees with itself.
func TestNotification_ARefusedTransitionLeavesTheAggregateUntouched(t *testing.T) {
	n := newRequest(t)
	if err := n.MarkFailed(); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	before := n.SentAt()

	_ = n.MarkSent(testNow.Add(time.Hour))

	if n.State() != StateFailed {
		t.Errorf("State() = %v, want failed", n.State())
	}
	if !n.SentAt().Equal(before) {
		t.Errorf("SentAt() = %v, want it unchanged at %v", n.SentAt(), before)
	}
}

// Transitions record no events. Nothing subscribes to "the email went out" — the
// state column is the record — and an event with no consumer is an outbox row
// that is written, published, purged and never read.
func TestNotification_TransitionsRecordNoEvents(t *testing.T) {
	n := newRequest(t)
	n.PullEvents() // drain the request event

	if err := n.MarkSent(testNow); err != nil {
		t.Fatalf("MarkSent: %v", err)
	}
	if events := n.PullEvents(); len(events) != 0 {
		t.Errorf("MarkSent recorded %d events, want 0", len(events))
	}
}
```

- [ ] **Step 4: Run them and watch them fail**

Run: `go test ./internal/notifications/domain/ -run TestRequestWelcome -v`
Expected: FAIL — `undefined: RequestWelcome`.

- [ ] **Step 5: Write `notification.go`**

```go
// Package domain is the notifications context's business model: the Notification
// aggregate, its state machine, and the event it records when an email is
// requested.
//
// It imports the standard library and github.com/google/uuid, and nothing else
// (spec §6.1), enforced by internal/arch. It also imports nothing from accounts —
// not the User, not the Email, not a constant. The two contexts communicate
// through a topic and share one transport type; everything accounts publishes is
// translated into this vocabulary at the application boundary, before any code in
// this package sees it (spec §7, §11.3).
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Notification is the aggregate: one email this context has decided to send.
//
// Its state machine is small and its rules are absolute — pending is the only
// state from which anything happens, sent is terminal — and they live here rather
// than in the sender's UPDATE, in the CHECK constraint, and in a reader's memory
// (spec §8.2). The CHECK constraint is a backstop against the value; this type is
// the definition of the transitions.
//
// Fields are unexported and read through accessors, for the reason accounts'
// User states: exported fields would make the invariants true of construction and
// false of the lifetime.
type Notification struct {
	id            uuid.UUID
	user          UserRef
	recipient     Recipient
	kind          string
	state         NotificationState
	sourceEventID uuid.UUID
	createdAt     time.Time
	sentAt        time.Time

	// Embedded, not a field: the aggregate records events, it does not own a
	// collection of them. Promotion is also what gives Notification its
	// PullEvents.
	eventRecorder
}

func (n *Notification) ID() uuid.UUID            { return n.id }
func (n *Notification) UserID() uuid.UUID        { return n.user.ID() }
func (n *Notification) Recipient() Recipient     { return n.recipient }
func (n *Notification) Kind() string             { return n.kind }
func (n *Notification) State() NotificationState { return n.state }
func (n *Notification) SourceEventID() uuid.UUID { return n.sourceEventID }
func (n *Notification) CreatedAt() time.Time     { return n.createdAt }

// SentAt is the zero time until the send succeeds, which is what the nullable
// column means. It is not an error to ask before then.
func (n *Notification) SentAt() time.Time { return n.sentAt }

// RequestWelcome records that a welcome email should be sent. It does not send
// one, and the verb is the design: this context's job is to make the intent
// durable, and the call to the gateway belongs to the sender, reading the
// send-intent this event becomes (spec §11.3, §11.4).
//
// It performs no I/O. The identifier and the current time arrive as values, so the
// function is deterministic and the layer above owns both sources of
// nondeterminism.
//
// sourceEventID is the inbound event's id, carried all the way to the gateway's
// Idempotency-Key. It is required, because notifications.source_event_id is
// UNIQUE — §12's mechanism 2 sitting behind §13's mechanism 3, deliberately
// redundant with the inbox so that a bug in the inbox path still cannot produce
// two notification rows.
func RequestWelcome(
	id uuid.UUID,
	to Recipient,
	user UserRef,
	sourceEventID uuid.UUID,
	now time.Time,
) (*Notification, error) {
	if id == uuid.Nil || sourceEventID == uuid.Nil {
		return nil, ErrInvalidID
	}

	n := &Notification{
		id:            id,
		user:          user,
		recipient:     to,
		kind:          KindWelcomeEmail,
		state:         StatePending,
		sourceEventID: sourceEventID,
		createdAt:     now,
	}
	n.record(WelcomeEmailRequested{
		NotificationID: n.id,
		UserID:         n.user.ID(),
		Recipient:      n.recipient,
		SourceEventID:  n.sourceEventID,
		RequestedAt:    n.createdAt,
	})
	return n, nil
}

// MarkSent moves a pending notification to sent. It refuses every other move,
// including a second MarkSent — see NotificationState.canTransitionTo for why a
// self-transition would be the wrong kind of forgiving.
func (n *Notification) MarkSent(at time.Time) error {
	if !n.state.canTransitionTo(StateSent) {
		return ErrIllegalTransition
	}
	n.state = StateSent
	n.sentAt = at
	return nil
}

// MarkFailed parks a notification a human has to look at.
//
// It takes no reason, and the absence is deliberate. There is no column for one:
// the failure detail belongs to the send-intent row, whose last_error the sender
// writes with the full error text and an attempt count beside it. A copy here
// would give one incident two accounts of itself, which diverge the first time one
// of the two writes fails. A parameter that is accepted and dropped would be worse
// than no parameter — a signature promising a record nothing keeps — and the day a
// failure_reason column arrives, adding it back is a smaller change than explaining
// why it was there and unused.
func (n *Notification) MarkFailed() error {
	if !n.state.canTransitionTo(StateFailed) {
		return ErrIllegalTransition
	}
	n.state = StateFailed
	return nil
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/notifications/domain/ -v`
Expected: PASS, and no `staticcheck` complaint — there is no unused parameter to complain about. Do include the state in the refusal, because "illegal transition" without saying from what is an error a reader has to reproduce: `return fmt.Errorf("%w: cannot send a %s notification", ErrIllegalTransition, n.state)`.

- [ ] **Step 7: Write `repository.go`**

```go
package domain

import "context"

// NotificationRepository is declared in the domain because a Repository is a
// domain concept in the DDD vocabulary: it is the collection of notifications,
// spoken about the way a domain expert would (spec D5). An inbox is not a domain
// concept and an outbox is not either, which is why both of those ports live in
// the application layer.
//
// The method is Insert rather than Save, and the difference from accounts'
// UserRepository.Save is deliberate. Save promises "persist whatever state this
// aggregate is in", which is the right promise for an aggregate with mutating
// operations. The consuming transaction only ever creates.
//
// **This interface is unfinished, and Plan 4 must finish it here rather than
// around it.** The sender transitions a notification to sent, and §11.4 describes
// that as setting `notifications.state = 'sent'` — which, taken literally, is a
// SQL UPDATE that goes around MarkSent and leaves the state machine guarding
// nothing. That is §8.2's own complaint arriving by the back door: "a bare state
// string field would leave the rules in three places at once: the CHECK, whatever
// the sender does before its UPDATE, and the reader's memory." A behaviour-rich
// aggregate whose only writer bypasses its behaviour is an anemic model with extra
// steps.
//
// So the transition belongs on this interface, added by the plan that needs it:
//
//	MarkSent(ctx context.Context, n *Notification) error
//
// implemented as a conditional UPDATE ... WHERE id = $1 AND state = 'pending'
// that returns ErrIllegalTransition on zero rows affected — the same rule as
// canTransitionTo, enforced where concurrency actually lives. The aggregate method
// stays the definition; the repository makes it the only way through.
type NotificationRepository interface {
	Insert(ctx context.Context, n *Notification) error
}
```

- [ ] **Step 8: Run everything**

Run: `make lint && go test ./internal/notifications/... ./internal/arch/ -v`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/notifications/domain
git commit -m "feat(notifications): the Notification aggregate, where the state machine actually lives"
```

---

## Task 4: The anti-corruption layer

**Files:**
- Create: `internal/notifications/application/doc.go`, `machine.go`, `consuming.go`, `translate.go`
- Test: `internal/notifications/application/translate_test.go`

**Interfaces:**
- Consumes: `internal/notifications/domain` (Task 2, 3), `internal/platform/messaging` (Task 1).
- Produces: `ErrTransient`, `ErrPermanent`; `Schedule`; `DeadLetterPublisher`, `DeadLetterReason`; `Clock`, `IDGen`; `WelcomeRequest{Recipient, User, SourceEventID}`; `handles(msg messaging.Message) bool`; `translate(msg messaging.Message) (WelcomeRequest, error)`.

This is the task that earns the plan. Everything else in it is machinery that Plan 2 already demonstrated in the other direction; this file is the half of the pattern accounts does not have.

§11.3 states the argument: "A decoded envelope handed straight to a notifications constructor would make this context a Conformist — its model defined by a schema another team owns." The CloudEvents factory in accounts is an Open Host Service publishing a stable language; this is the Anti-Corruption Layer that consumes one. A context with only the first half exports its model and imports someone else's.

The concrete promise: `translate` is the only code in `internal/notifications` that contains the strings `user_id`, `email`, `display_name` or `com.outboxexpress.accounts.user.registered`. Step 8 asserts that with a grep, because it is the kind of property that is true on the day it is written and false a year later.

- [ ] **Step 1: Write `doc.go`**

```go
// Package application holds the notifications use cases and the technical ports
// they need. It imports this context's domain and internal/platform/messaging,
// and nothing concrete: no SQL, no Kafka, no HTTP, no logger (spec §6.1).
//
// It imports nothing from accounts. What arrives from accounts arrives as bytes
// inside a messaging.Message and is translated in translate.go before any other
// file in this context sees it — that file is the anti-corruption layer, and its
// existence is the reason this context has a model of its own rather than a copy
// of someone else's (spec §11.3).
//
// Ports live beside what they are about, not in a file called ports. The inbox
// port is in inbox.go, the envelope factory beside the envelope it produces and
// its only implementation, the transaction boundary beside the work set it hands
// out, and machine.go holds only the residue that belongs nowhere: the clock and
// the id generator, which every layer needs and no layer owns. This is the
// convention accounts settled on; it is written down in both packages so that the
// next context inherits the considered layout rather than the first one.
package application
```

- [ ] **Step 2: Write `machine.go`**

Identical in shape to accounts', because the reasons are identical. Do not import accounts' version; `internal/arch` forbids it, and two contexts sharing a `Clock` would be a shared kernel nobody argued for.

```go
package application

import (
	"time"

	"github.com/google/uuid"
)

// Clock and IDGen exist so that a use case is deterministic under test. Reading
// the wall clock or generating a UUID is I/O against the machine.
//
// Now must return UTC. Normalising is this layer's job, not the domain's: an
// aggregate handed a timestamp stores that instant unchanged, so whatever the
// clock returns is what gets written.
type Clock interface {
	Now() time.Time
}

// IDGen returns an error because uuid.NewV7 reads entropy and can fail. The
// notifier classifies that failure as transient: an exhausted entropy pool is a
// machine problem, and dead-lettering a valid record because of one would be a
// permanent decision about a temporary condition (see consuming.go).
type IDGen interface {
	New() (uuid.UUID, error)
}
```

- [ ] **Step 3: Write `consuming.go`**

```go
package application

import (
	"context"
	"errors"

	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// The two error classes of the consuming side, and they mean exactly what the
// relay's mean (spec §12.1, §12.2) — which is why they are declared per context
// rather than shared: an error class is a decision about what this process does
// next, and the two processes do different things.
//
// ErrTransient means try again. The record is fine, the world is not: a database
// that is down, a pool that is exhausted, an entropy pool that hiccuped. The
// notifier retries in process, then declines to commit the offset and lets Kafka
// redeliver.
//
// ErrPermanent means this record will never succeed. It is malformed, unmapped,
// or violates an invariant the domain will refuse every time it is asked. Retrying
// is a way of never noticing, so the record goes to the dead-letter topic and the
// offset is committed.
//
// Unclassified errors exist and are treated as transient. That is the safe
// default in this direction — the opposite of the relay's, which abandons the pass
// — because the cost of retrying something unrecognised is latency and a log line,
// while the cost of dead-lettering it is a message a human has to replay.
var (
	ErrTransient = errors.New("notifications: transient failure")
	ErrPermanent = errors.New("notifications: permanent failure")
)

// Schedule is how long to wait before the next in-process attempt (§12.2 rung 1).
//
// A one-method port over internal/platform/backoff, declared here for the reason
// that package's doc gives: "§12.1's relay ladder and §12.2's consumer ladder are
// the same arithmetic with a different base… Each context declares its own
// one-method port over this." The formula — min(cap, base·2^n) · random(0.5, 1.5),
// with the saturate-before-multiply that stops an int64 overflow making a backoff
// get *shorter* the longer the outage lasts — is not rewritten here or anywhere
// else.
//
// It is a port rather than a duration because §15 forbids sleeps for
// synchronisation: a test that has to wait 350ms to assert three attempts is a
// test that eventually fails on a loaded machine.
type Schedule interface {
	After(attempts int) time.Duration
}

// DeadLetterPublisher hands a record that will never succeed to the dead-letter
// topic, and returns only once the broker has durably acknowledged it (spec
// §12.2).
//
// The synchronous shape is the contract, not an implementation detail, and it is
// the same argument as the relay's EventPublisher: the offset is committed after
// this returns, so a fire-and-forget DLT produce would forget where the record was
// before it was durable anywhere else. "Producing to the DLT before committing the
// offset is the same ordering rule as before: the record is durable somewhere else
// before we forget where it was."
//
// It takes the original message plus the reason, and the adapter composes the
// dlt_* headers. Every error it returns wraps ErrTransient — there is no permanent
// failure of dead-lettering that the notifier could act on, and if the DLT itself
// is unreachable the right behaviour is to stop making progress rather than to
// drop the record.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, msg messaging.Message, reason DeadLetterReason) error
}

// DeadLetterReason is what the notifier knows about why this record is here. It
// becomes the dlt_reason, dlt_error and dlt_deliveries headers; the adapter owns
// the spelling, this layer owns the content (spec §12.2).
type DeadLetterReason struct {
	// Reason is a short classification: "permanent" or "max_deliveries".
	Reason string
	// Err is the full error text, which is the only clue a human replaying from
	// the DLT will have about which of four processes refused the record and why.
	Err string
	// Deliveries is the process-local Attempt count. It resets on a rebalance or
	// a restart, so it is a hint about severity and never a fact (§6.5).
	Deliveries int
}
```

- [ ] **Step 4: Write the failing tests for the translator**

`internal/notifications/application/translate_test.go`:

```go
package application

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// validRecord is the exact wire form accounts produces: the CloudEvents envelope
// of §9.1 with a userRegisteredData in `data`. It is written out as a literal
// rather than built by calling accounts' factory, and that is the point — a test
// that imported the producer would pass on the day the producer changed the
// contract, which is the only day this test matters.
func validRecord(t *testing.T, mutate func(env map[string]any)) messaging.Message {
	t.Helper()
	env := map[string]any{
		"specversion":     "1.0",
		"id":              "0199a4f0-0000-7000-8000-00000000000c",
		"type":            "com.outboxexpress.accounts.user.registered",
		"source":          "/services/accounts",
		"subject":         "0199a4f0-0000-7000-8000-00000000000b",
		"time":            "2026-08-27T12:00:00.000Z",
		"dataschema":      "https://schemas.outboxexpress.dev/accounts/user.registered/1.json",
		"datacontenttype": "application/json",
		"data": map[string]any{
			"user_id":       "0199a4f0-0000-7000-8000-00000000000b",
			"email":         "ada@example.com",
			"display_name":  "Ada Lovelace",
			"version":       1,
			"registered_at": "2026-08-27T12:00:00.000Z",
		},
	}
	if mutate != nil {
		mutate(env)
	}
	payload, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return messaging.Message{
		EventID:       "0199a4f0-0000-7000-8000-00000000000c",
		EventType:     "com.outboxexpress.accounts.user.registered",
		SchemaVersion: 1,
		Key:           "0199a4f0-0000-7000-8000-00000000000b",
		Payload:       payload,
		Headers:       map[string]string{messaging.HeaderCorrelationID: "corr-1"},
		Topic:         "accounts.user.v1",
	}
}

func TestTranslate_ProducesThisContextsTypes(t *testing.T) {
	req, err := translate(validRecord(t, nil))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if got := req.Recipient.String(); got != "ada@example.com" {
		t.Errorf("Recipient = %q, want ada@example.com", got)
	}
	if got := req.User.ID().String(); got != "0199a4f0-0000-7000-8000-00000000000b" {
		t.Errorf("User = %q", got)
	}
	if req.SourceEventID.String() != "0199a4f0-0000-7000-8000-00000000000c" {
		t.Errorf("SourceEventID = %v", req.SourceEventID)
	}
}

// The deduplication key comes from the header-and-column EventID, not from the
// envelope's `id` field, and the two are the same value on every honest message.
// Reading the column is what lets a consumer deduplicate without deserialising
// (§9.2) — and it is what makes the inbox key match what the relay's row says,
// even if a producer bug ever made the two disagree.
func TestTranslate_TakesTheSourceEventIDFromTheEnvelopeIdentity(t *testing.T) {
	msg := validRecord(t, func(env map[string]any) {
		env["id"] = "0199a4f0-0000-7000-8000-0000000000ff" // payload disagrees
	})
	req, err := translate(msg)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if req.SourceEventID.String() != msg.EventID {
		t.Errorf("SourceEventID = %v, want the message's EventID %s", req.SourceEventID, msg.EventID)
	}
}

// Tolerant reader (§9.3): an added field is absorbed, because a producer must be
// able to add one without a coordinated deploy.
func TestTranslate_IgnoresUnknownFields(t *testing.T) {
	msg := validRecord(t, func(env map[string]any) {
		env["newattribute"] = "whatever"
		env["data"].(map[string]any)["locale"] = "en-GB"
	})
	if _, err := translate(msg); err != nil {
		t.Fatalf("translate rejected an additive change: %v", err)
	}
}

// Every one of these is ErrPermanent, because none of them will ever become
// valid: the record on the topic is immutable, so a retry is a way of never
// noticing (§11.3, §12.2).
func TestTranslate_MalformedInputIsPermanent(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
		msg    *messaging.Message
	}{
		{name: "empty email", mutate: func(e map[string]any) {
			e["data"].(map[string]any)["email"] = ""
		}},
		{name: "invalid email", mutate: func(e map[string]any) {
			e["data"].(map[string]any)["email"] = "not-an-address"
		}},
		{name: "missing user_id", mutate: func(e map[string]any) {
			delete(e["data"].(map[string]any), "user_id")
		}},
		{name: "user_id not a uuid", mutate: func(e map[string]any) {
			e["data"].(map[string]any)["user_id"] = "seventeen"
		}},
		{name: "nil user_id", mutate: func(e map[string]any) {
			e["data"].(map[string]any)["user_id"] = uuid.Nil.String()
		}},
		{name: "data is not an object", mutate: func(e map[string]any) {
			e["data"] = "surprise"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := translate(validRecord(t, tc.mutate)); !errors.Is(err, ErrPermanent) {
				t.Fatalf("err = %v, want ErrPermanent", err)
			}
		})
	}
}

func TestTranslate_UnparseablePayloadIsPermanent(t *testing.T) {
	msg := validRecord(t, nil)
	msg.Payload = []byte("{not json")
	if _, err := translate(msg); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}

// A bad event_id is permanent and needs its own case, because it is the one field
// whose failure would otherwise be invisible: without a source event id there is
// no inbox key and no Idempotency-Key, so processing the record would be
// processing it unsafely.
func TestTranslate_AnUnparseableEventIDIsPermanent(t *testing.T) {
	msg := validRecord(t, nil)
	msg.EventID = "not-a-uuid"
	if _, err := translate(msg); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}

// A different event type on the same topic is neither an error nor work, so it is
// answered by a predicate rather than by an error. Dead-lettering it would flood
// the DLT on a deployment that has nothing to do with notifications; retrying it
// would stall the partition forever. Neither is a failure mode worth a sentinel
// that gets wrapped and unwrapped three lines apart.
func TestHandles_AcceptsOnlyTheEventThisConsumerWants(t *testing.T) {
	msg := validRecord(t, nil)
	if !handles(msg) {
		t.Error("handles rejected user.registered")
	}
	msg.EventType = "com.outboxexpress.accounts.user.deactivated"
	if handles(msg) {
		t.Error("handles accepted an event type this consumer does not map")
	}
}

// The error text has to name the record. A dead-letter reason of "invalid
// recipient" with no event id is an incident nobody can act on.
func TestTranslate_ErrorsNameTheEventAndTheField(t *testing.T) {
	msg := validRecord(t, func(e map[string]any) {
		e["data"].(map[string]any)["email"] = "nope"
	})
	_, err := translate(msg)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{msg.EventID, "recipient"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
```

Imports for this file: `bytes`, `encoding/json`, `errors`, `io/fs`, `os`, `path/filepath`, `strings`, `testing`, `github.com/google/uuid`, `internal/platform/messaging`. The last four of those arrive with Step 8.

- [ ] **Step 5: Run them and watch them fail**

Run: `go test ./internal/notifications/application/ -run TestTranslate -v`
Expected: FAIL — `undefined: translate`.

- [ ] **Step 6: Write `translate.go`**

```go
package application

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// eventTypeUserRegistered is accounts' published event type, and this constant is
// the only place in internal/notifications where it appears. Same for the field
// names below: this file is the context's whole knowledge of accounts' schema,
// which is what makes an additive change upstream a change to one file here (spec
// §11.3).
const eventTypeUserRegistered = "com.outboxexpress.accounts.user.registered"

// handles reports whether this consumer maps the record's event type.
//
// A predicate, not an error class. A topic is a stream of a context's published
// language, not a queue for one consumer, so accounts.user.v1 will carry a second
// event type the day accounts grows a second use case — and a record this consumer
// does not want is not a failure of any kind. Making it one would put "not for me"
// into the same vocabulary as "the database is down", where the worker's
// classification switch would have to grow an arm to stop it being retried forever.
//
// A record this returns false for is skipped, its offset is committed, and —
// deliberately — no inbox row is written. An inbox row records that this consumer
// *processed* an event; writing one for an event it declined to understand would be
// a lie that outlives the deployment, hiding every such record from the version of
// the notifier that finally handles the type. Skipping is idempotent on its own.
func handles(msg messaging.Message) bool {
	return msg.EventType == eventTypeUserRegistered
}

// WelcomeRequest is what the translator returns: this context's own types, ready
// for its own constructor. Nothing downstream of here has seen accounts' field
// names, and that is the property the anti-corruption layer exists to provide.
type WelcomeRequest struct {
	// Recipient is this context's email value object, validated by this context's
	// rules. Accounts validated the address too; that it did is not a reason to
	// trust it, and the day the two rules differ this is where the difference
	// lives.
	Recipient domain.Recipient

	// User is a foreign aggregate referenced by id and nothing else.
	User domain.UserRef

	// SourceEventID is the inbound event's identity: the inbox key, and the
	// gateway's Idempotency-Key (§8.2). It comes from the message's EventID
	// column rather than from inside the payload, so that deduplicating never
	// requires deserialising (§9.2).
	SourceEventID uuid.UUID
}

// translate is the anti-corruption layer. It is unexported: an anti-corruption
// layer that other packages can call is a translation nobody controls, and the
// only legitimate caller is the use case in this package.
//
// Every failure here is ErrPermanent, with no exceptions. A record on this topic
// is immutable, so retrying a malformed one is a way of never noticing it, which
// is why §11.3 sends it to the dead-letter topic rather than into a retry loop.
// Callers establish that the record is one this consumer maps by calling handles
// first; translate assumes it.
func translate(msg messaging.Message) (WelcomeRequest, error) {
	// The source event id is read first, because it is the only field whose
	// absence makes the record unprocessable rather than merely invalid: without
	// it there is no inbox key, so there is no safe way to do the work at all.
	sourceEventID, err := uuid.Parse(msg.EventID)
	if err != nil {
		return WelcomeRequest{}, fmt.Errorf("%w: event id %q: %v", ErrPermanent, msg.EventID, err)
	}
	if sourceEventID == uuid.Nil {
		return WelcomeRequest{}, fmt.Errorf("%w: event id is the nil UUID", ErrPermanent)
	}

	var env userRegisteredEnvelope
	if err := json.Unmarshal(msg.Payload, &env); err != nil {
		return WelcomeRequest{}, fmt.Errorf("%w: event %s: payload: %v", ErrPermanent, sourceEventID, err)
	}

	recipient, err := domain.NewRecipient(env.Data.Email)
	if err != nil {
		// The address is not echoed into the error. A dead-letter header ends up
		// in logs and in a human's terminal, and a bad address is still someone's
		// address; the event id is enough to find the record.
		return WelcomeRequest{}, fmt.Errorf("%w: event %s: recipient: %v", ErrPermanent, sourceEventID, err)
	}

	userID, err := uuid.Parse(env.Data.UserID)
	if err != nil {
		return WelcomeRequest{}, fmt.Errorf("%w: event %s: user_id %q: %v",
			ErrPermanent, sourceEventID, env.Data.UserID, err)
	}
	user, err := domain.NewUserRef(userID)
	if err != nil {
		return WelcomeRequest{}, fmt.Errorf("%w: event %s: user: %v", ErrPermanent, sourceEventID, err)
	}

	return WelcomeRequest{
		Recipient:     recipient,
		User:          user,
		SourceEventID: sourceEventID,
	}, nil
}

// userRegisteredEnvelope is accounts' wire format as this context reads it, and
// it is deliberately a *different* struct from the one accounts marshals.
//
// It lists only the fields this context uses, which is what makes it a tolerant
// reader (§9.3): json.Unmarshal ignores everything else, so accounts can add an
// attribute or a data field without a coordinated deploy. Sharing accounts'
// struct instead would turn every additive change up there into a compile-time
// dependency down here, and the display name — which accounts sends and this
// context has no use for — would be a field two teams maintain for no reader.
//
// It is at the bottom of the file because it is the lowest level of detail in it.
type userRegisteredEnvelope struct {
	Data struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
	} `json:"data"`
}
```

- [ ] **Step 7: Run the tests**

Run: `go test ./internal/notifications/application/ -run TestTranslate -v`
Expected: PASS, all of them. If "data is not an object" fails, the reason is that `json.Unmarshal` into a struct field errors on a type mismatch, which is the behaviour the test wants — confirm the error wraps `ErrPermanent`.

- [ ] **Step 8: Assert the part of the boundary a test can actually express**

The property the file exists for is "accounts' vocabulary appears here and nowhere else in the context". A text search over the context's `.go` files cannot express it, and the first draft of this plan found that out the hard way: `envelope.go` legitimately publishes a `user_id` field of *this* context's own schema, so the forbidden list had to be narrowed until it was two strings, permitting `email`, `version`, `subject` and most of the rest of accounts' envelope. A test that cannot tell the two cases apart is worse than no test, because it reads like the boundary is guarded.

So assert the one string that is unambiguously accounts' and cannot occur innocently — its reverse-DNS event type — and say plainly in the comment that this is a tripwire, not a proof.

```go
// A tripwire, not a proof. The property this file exists for — that accounts'
// vocabulary lives here and nowhere else — is not expressible as a text search:
// envelope.go publishes a user_id field of this context's own schema, and no list
// of forbidden strings can tell that apart from accounts'. What *is* unambiguous is
// accounts' reverse-DNS event type namespace: nothing in this context has an
// innocent reason to name it, and a second file that did would be a second place
// deciding what an accounts event means.
//
// The rest of the property is held by the file's doc comment and by review. That
// is an honest answer; a green test that permits most of what it claims to forbid
// is not.
func TestTranslate_IsTheOnlyPlaceThatNamesAccountsEventTypes(t *testing.T) {
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".go" {
			return err
		}
		switch filepath.Base(path) {
		case "translate.go", "translate_test.go":
			return nil // this file owns the vocabulary; its test carries a sample
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(body, []byte("com.outboxexpress.accounts")) {
			t.Errorf("%s names an accounts event type; translate.go is the only "+
				"place in this context that may", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
```

The walk root is `".."` — the context's `application` directory's parent — so it covers domain, application, infrastructure and presentation as they arrive. Run it after every later task, not just this one.

- [ ] **Step 9: Verify the dependency rule**

Run: `make lint && go test ./internal/arch/ ./internal/notifications/... -v`
Expected: PASS. `TestApplicationImportsNothingConcrete` now walks this package; `encoding/json`, `fmt`, `uuid`, this context's domain and `platform/messaging` are all it finds.

- [ ] **Step 10: Commit**

```bash
git add internal/notifications/application
git commit -m "feat(notifications): the anti-corruption layer, which is the half accounts does not have"
```

---

## Task 5: This context's envelope, and the column that makes the sender dumb

**Files:**
- Create: `internal/notifications/application/envelope.go`
- Test: `internal/notifications/application/envelope_test.go`

**Interfaces:**
- Consumes: Task 4's `Metadata` shape (declared in Task 6 — write that type first if you are doing these in order, or declare it here and move it; the plan puts it in Task 6 because it belongs beside the unit of work), `IDGen`, this context's `domain`.
- Produces: `Envelope` (with `IdempotencyKey`), `EnvelopeFactory`, `CloudEventFactory`, `NewCloudEventFactory(ids IDGen) CloudEventFactory`.

Structurally the same as accounts' envelope factory, with one field's difference that carries the whole of §13 remedy 2: `Envelope.IdempotencyKey`. Everything else about this task is a mirror, and the mirror is worth having rather than sharing — the two contexts publish different languages to different destinations, and a shared factory would be one type deciding both.

- [ ] **Step 1: Write the failing test**

`internal/notifications/application/envelope_test.go`:

```go
package application

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
)

func TestCloudEventFactory_MapsTheSendIntent(t *testing.T) {
	notifID := uuid.MustParse("0199a4f0-0000-7000-8000-00000000000a")
	userID := uuid.MustParse("0199a4f0-0000-7000-8000-00000000000b")
	source := uuid.MustParse("0199a4f0-0000-7000-8000-00000000000c")
	minted := uuid.MustParse("0199a4f0-0000-7000-8000-0000000000ee")
	at := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

	to, err := domain.NewRecipient("ada@example.com")
	if err != nil {
		t.Fatalf("NewRecipient: %v", err)
	}
	event := domain.WelcomeEmailRequested{
		NotificationID: notifID,
		UserID:         userID,
		Recipient:      to,
		SourceEventID:  source,
		RequestedAt:    at,
	}

	f := NewCloudEventFactory(fixedIDs{next: minted})
	envelopes, err := f.From([]domain.Event{event}, Metadata{CorrelationID: "corr-1", Traceparent: "tp-1"})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("got %d envelopes, want 1", len(envelopes))
	}
	env := envelopes[0]

	// The event id is minted here, once, and is *not* the source event id. Two
	// different identities: this row's own, and the inbound event it answers.
	if env.EventID != minted {
		t.Errorf("EventID = %v, want the minted id %v", env.EventID, minted)
	}

	// The one field accounts' Envelope does not have, and the reason §13 remedy 2
	// composes with remedy 1: the gateway's Idempotency-Key is the *original
	// inbound* event id, so a repeated call after a crash window is deduplicated
	// by the provider (§8.2, §10.3).
	if env.IdempotencyKey != source.String() {
		t.Errorf("IdempotencyKey = %q, want the source event id %q", env.IdempotencyKey, source)
	}

	if env.AggregateType != domain.AggregateTypeNotification {
		t.Errorf("AggregateType = %q", env.AggregateType)
	}
	if env.AggregateID != notifID.String() {
		t.Errorf("AggregateID = %q, want the notification id", env.AggregateID)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", env.SchemaVersion)
	}
	if !env.OccurredAt.Equal(at) {
		t.Errorf("OccurredAt = %v, want %v", env.OccurredAt, at)
	}
}

// The payload carries what the sender needs to make the call without a join: the
// address and the notification id. It does not carry the idempotency key, which
// lives in its own column because the sender routes on columns and never parses
// business content (§8.2).
func TestCloudEventFactory_PayloadCarriesTheCallItself(t *testing.T) {
	env := oneEnvelope(t)

	var decoded struct {
		Type string `json:"type"`
		Data struct {
			NotificationID string `json:"notification_id"`
			Recipient      string `json:"recipient"`
		} `json:"data"`
	}
	if err := json.Unmarshal(env.Payload, &decoded); err != nil {
		t.Fatalf("payload is not a CloudEvent: %v", err)
	}
	if decoded.Type != domain.EventTypeWelcomeEmailRequested {
		t.Errorf("type = %q", decoded.Type)
	}
	if decoded.Data.Recipient != "ada@example.com" {
		t.Errorf("data.recipient = %q", decoded.Data.Recipient)
	}
	if decoded.Data.NotificationID == "" {
		t.Error("data.notification_id is empty; the sender would have nothing to mark")
	}
}

// Headers hold only what has no column of its own (§9.2) — the same rule accounts
// follows, because the stored headers are read back by a relay that composes the
// rest at publish time.
func TestCloudEventFactory_HeadersHoldOnlyTheAmbientContext(t *testing.T) {
	env := oneEnvelope(t)
	if len(env.Headers) != 2 {
		t.Fatalf("headers = %v, want exactly correlation_id and traceparent", env.Headers)
	}
	if env.Headers[messaging.HeaderCorrelationID] != "corr-1" {
		t.Errorf("correlation_id = %q", env.Headers[messaging.HeaderCorrelationID])
	}
}

func TestCloudEventFactory_NoEventsIsNoEnvelopes(t *testing.T) {
	f := NewCloudEventFactory(fixedIDs{next: uuid.New()})
	envelopes, err := f.From(nil, Metadata{})
	if err != nil || envelopes != nil {
		t.Fatalf("From(nil) = %v, %v; want nil, nil", envelopes, err)
	}
}

// An event type with no mapping must fail loudly rather than produce an envelope
// with an empty payload. The failure is the unit of work's, so the transaction
// rolls back and the notifier retries — which is right: an unmapped *outgoing*
// event is a bug in this context, not a bad message from another one.
func TestCloudEventFactory_AnUnmappedEventIsAnError(t *testing.T) {
	f := NewCloudEventFactory(fixedIDs{next: uuid.New()})
	if _, err := f.From([]domain.Event{unmappedEvent{}}, Metadata{}); !errors.Is(err, ErrUnmappedEvent) {
		t.Fatalf("err = %v, want ErrUnmappedEvent", err)
	}
}
```

Add to a new `internal/notifications/application/fakes_test.go`:

```go
package application

import (
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
)

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

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// unmappedEvent is a domain.Event the factory has no case for. It lives in the
// test because a type whose only purpose is to be unmapped has no business in the
// domain.
type unmappedEvent struct{}

func (unmappedEvent) EventType() string     { return "com.outboxexpress.notifications.nothing" }
func (unmappedEvent) AggregateType() string { return domain.AggregateTypeNotification }
func (unmappedEvent) AggregateID() string   { return "none" }
func (unmappedEvent) OccurredAt() time.Time { return time.Time{} }

// oneEnvelope is the happy path, for tests that assert one aspect of it.
func oneEnvelope(t *testing.T) Envelope {
	t.Helper()
	to, err := domain.NewRecipient("ada@example.com")
	if err != nil {
		t.Fatalf("NewRecipient: %v", err)
	}
	event := domain.WelcomeEmailRequested{
		NotificationID: uuid.MustParse("0199a4f0-0000-7000-8000-00000000000a"),
		UserID:         uuid.MustParse("0199a4f0-0000-7000-8000-00000000000b"),
		Recipient:      to,
		SourceEventID:  uuid.MustParse("0199a4f0-0000-7000-8000-00000000000c"),
		RequestedAt:    time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
	}
	f := NewCloudEventFactory(fixedIDs{next: uuid.MustParse("0199a4f0-0000-7000-8000-0000000000ee")})
	envelopes, err := f.From([]domain.Event{event}, Metadata{CorrelationID: "corr-1", Traceparent: "tp-1"})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	return envelopes[0]
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/notifications/application/ -run TestCloudEventFactory -v`
Expected: FAIL — `undefined: NewCloudEventFactory`.

- [ ] **Step 3: Write `envelope.go`**

```go
package application

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// This context's identity on the wire. A constant rather than a constructor
// parameter: there is one notifications service and no configuration path sets it.
// The schema *base* is not here — it is deployment-wide and lives in
// platform/messaging as SchemaBase, because two copies would drift into two
// published schema namespaces with nothing failing.
const source = "/services/notifications"

var ErrUnmappedEvent = errors.New("notifications: no message mapping for event type")

// Envelope is one outbox row's worth of content: the routing columns the sender
// reads, the already-serialised message it must not read, and the idempotency key
// it must put on the wire.
//
// IdempotencyKey is the field accounts' Envelope does not have, and it is the
// entire composition of §13's two remedies. Remedy 2 is this table: the intent is
// durable, so a gateway failure is retried rather than lost. Remedy 1 is this
// column: the value is the *original inbound* event id, so a repeated call after a
// crash window is deduplicated by the provider (§8.2, §10.3, §11.4).
//
// It is a column and not a payload field for the same reason the routing fields
// are: the sender is a dumb pipe. It reads columns and never parses the payload,
// so anything it needs in order to make the call has to be beside the payload
// rather than inside it.
type Envelope struct {
	EventID        uuid.UUID
	AggregateType  string
	AggregateID    string
	EventType      string
	SchemaVersion  int
	Payload        []byte
	Headers        map[string]string
	OccurredAt     time.Time
	IdempotencyKey string
}

// EnvelopeFactory maps this context's domain events onto its published contract.
// An application concern: the domain must not know what CloudEvents is, and the
// infrastructure must not decide what a message means.
type EnvelopeFactory interface {
	From(events []domain.Event, meta Metadata) ([]Envelope, error)
}

// CloudEventFactory maps this context's domain events onto the message contract.
//
// The mapping is an explicit type switch rather than reflection over the domain
// struct: the domain carries no struct tags (§6.1), so it has no opinion about
// JSON, and a wire contract that changes when someone renames a domain field is
// not a contract.
type CloudEventFactory struct {
	ids IDGen
}

func NewCloudEventFactory(ids IDGen) CloudEventFactory {
	return CloudEventFactory{ids: ids}
}

func (f CloudEventFactory) From(events []domain.Event, meta Metadata) ([]Envelope, error) {
	if len(events) == 0 {
		return nil, nil
	}

	// Loop-invariant: every envelope from one transaction carries the same
	// correlation and trace, which is how a registration in accounts and the email
	// it eventually causes end up on the same trace (§9.2).
	h := headers(meta)

	envelopes := make([]Envelope, 0, len(events))
	for _, event := range events {
		m, err := mapData(event)
		if err != nil {
			return nil, err
		}

		// One id per event, minted here at insert time and never again. It is not
		// the idempotency key and must not be reused as one: this identifies the
		// row, while the key identifies the inbound event whose effect must happen
		// once (§8.1, §8.2).
		eventID, err := f.ids.New()
		if err != nil {
			return nil, fmt.Errorf("notifications: event id: %w", err)
		}

		occurred := event.OccurredAt()
		aggregateID := event.AggregateID()
		eventType := event.EventType()

		payload, err := messaging.EncodeCloudEvent(messaging.Attributes{
			ID:          eventID.String(),
			Type:        eventType,
			Source:      source,
			Subject:     aggregateID,
			Time:        occurred,
			SchemaName:  m.schema.name,
			SchemaBase:  messaging.SchemaBase,
			Version:     m.schema.version,
			Traceparent: meta.Traceparent,
		}, m.data)
		if err != nil {
			return nil, err
		}

		envelopes = append(envelopes, Envelope{
			EventID:        eventID,
			AggregateType:  event.AggregateType(),
			AggregateID:    aggregateID,
			EventType:      eventType,
			SchemaVersion:  m.schema.version,
			Payload:        payload,
			Headers:        h,
			OccurredAt:     occurred,
			IdempotencyKey: m.idempotencyKey,
		})
	}
	return envelopes, nil
}

// messageSchema names one version of one message, so that emitting one state
// change as two schema versions (§9.3) is a change in mapData and nowhere else.
type messageSchema struct {
	name    string
	version int
}

// mapping is everything mapData decides about one event: the data shape, the
// schema that describes it, and the idempotency key for the external call it will
// cause. One value rather than a widening tuple, for the reason messageSchema
// exists at all — a per-event-type decision should be one thing a reader can hold,
// and the alternative is a return list that grows by one every time this context
// learns to publish something new.
type mapping struct {
	data           any
	schema         messageSchema
	idempotencyKey string
}

// mapData is the context-specific part of the envelope. The idempotency key is
// decided here rather than by the caller because it is a per-event-type question:
// WelcomeEmailRequested's key is the source event id, and a future event whose
// external effect is keyed differently says so beside its own payload mapping
// instead of in a switch somewhere else.
func mapData(event domain.Event) (mapping, error) {
	switch e := event.(type) {
	case domain.WelcomeEmailRequested:
		return mapping{
			data: welcomeEmailRequestedData{
				NotificationID: e.NotificationID.String(),
				UserID:         e.UserID.String(),
				Recipient:      e.Recipient.String(),
				SourceEventID:  e.SourceEventID.String(),
				RequestedAt:    e.RequestedAt.UTC().Format(messaging.TimeFormat),
			},
			schema:         messageSchema{name: "notifications/welcome_email.requested", version: 1},
			idempotencyKey: e.SourceEventID.String(),
		}, nil
	default:
		return mapping{}, fmt.Errorf("%w: %s", ErrUnmappedEvent, event.EventType())
	}
}

// headers holds only what has no column of its own. The sender composes ce_id,
// ce_type, ce_specversion, schema_version and content-type from the row itself at
// send time (§9.2).
func headers(meta Metadata) map[string]string {
	h := make(map[string]string, 2)
	if meta.CorrelationID != "" {
		h[messaging.HeaderCorrelationID] = meta.CorrelationID
	}
	if meta.Traceparent != "" {
		h[messaging.HeaderTraceparent] = meta.Traceparent
	}
	return h
}

// welcomeEmailRequestedData is the `data` shape. It carries everything the sender
// needs to make the call with no join and no second read — the address it goes to
// and the notification it belongs to — which is what lets the sender be a pipe.
type welcomeEmailRequestedData struct {
	NotificationID string `json:"notification_id"`
	UserID         string `json:"user_id"`
	Recipient      string `json:"recipient"`
	SourceEventID  string `json:"source_event_id"`
	RequestedAt    string `json:"requested_at"`
}

// Compile-time proof that the factory satisfies the port.
var _ EnvelopeFactory = (*CloudEventFactory)(nil)
```

Note: `user_id` appears in this file, and it is *this context's* `user_id` in *this context's* published schema — not a leak of accounts' vocabulary. This is the naming coincidence that decided the shape of Task 4's Step 8: a forbidden-strings test cannot tell the two apart, so it asserts only accounts' reverse-DNS namespace, which nothing here has an innocent reason to name. Nothing to do at this step beyond not being surprised.

- [ ] **Step 4: Run it and watch it pass**

Run: `go test ./internal/notifications/application/ -v`
Expected: PASS. The `messaging` import in `envelope_test.go` arrives with the header test.

- [ ] **Step 5: Commit**

```bash
git add internal/notifications/application
git commit -m "feat(notifications): the send-intent envelope, and the one column accounts does not need"
```

---

## Task 6: The consuming transaction

**Files:**
- Create: `internal/notifications/application/unit_of_work.go`, `inbox.go`, `process_user_registered.go`
- Test: `internal/notifications/application/process_user_registered_test.go`, and additions to `fakes_test.go`

**Interfaces:**
- Consumes: Tasks 2–5.
- Produces: `Metadata`, `metadataFrom(map[string]string) Metadata`, `Work{Notifications domain.NotificationRepository}`, `InboxClaim{Consumer, EventID, EventType}`, `UnitOfWork.Do(ctx, meta, claim, fn) (claimed bool, err error)`; `Outcome` with `OutcomeRecorded`/`OutcomeDuplicate`/`OutcomeIgnored`; `ProcessResult{Outcome, NotificationID}`; `NewProcessUserRegistered(uow, ids, clk, consumer string) *ProcessUserRegistered` and its `Execute(ctx, msg messaging.Message) (ProcessResult, error)`.

The use case is short and every line of it is load-bearing. §11.3's table of wrong shapes goes in its doc comment.

Read divergence 6 before starting: the inbox claim is the boundary's first statement, not the use case's. That is the one place this plan departs from §11.3's sketch structurally rather than cosmetically, and the reason is the plan's own D4 argument — an invariant in a call site is an invariant someone forgets, and "inbox first" is exactly that kind of invariant.

- [ ] **Step 1: Write `unit_of_work.go`**

```go
package application

import (
	"context"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// Metadata is the ambient context of one consumed record — which trace it belongs
// to, and which correlation it continues. It travels into this context's own
// envelope, so that a registration in accounts and the email it causes share a
// trace across two asynchronous hops and two databases (§9.2).
type Metadata struct {
	CorrelationID string
	Traceparent   string
}

// metadataFrom reads the two ambient headers off an inbound record.
//
// Spec §11.3 writes this as msg.Metadata(), a method on messaging.Message. It
// cannot be one: messaging is shared by both contexts (§6.5) and cannot import
// either one's application package, and promoting Metadata into platform would
// make it the shared domain model §6.5 exists to refuse. So the read happens here,
// in the layer that owns the type, using the header constants messaging does own.
//
// A record with neither header yields the zero Metadata, which is correct rather
// than tolerated: a message produced before tracing existed is still a message
// worth processing, and an absent trace is absent, not invalid.
func metadataFrom(headers map[string]string) Metadata {
	return Metadata{
		CorrelationID: headers[messaging.HeaderCorrelationID],
		Traceparent:   headers[messaging.HeaderTraceparent],
	}
}

// Work is the set of repositories bound to one transaction. The infrastructure
// constructs it; a use case can only reach persistence through it, which is what
// makes an untransacted write unexpressible.
//
// It holds one repository, and the two tables it does *not* hold are the point.
// The outbox is absent for the reason accounts' Work states: the moment a use case
// can append a send-intent by hand, "every state change emits its event" becomes a
// rule someone has to remember. The inbox is absent for the same reason one step
// earlier — see UnitOfWork.Do.
type Work struct {
	Notifications domain.NotificationRepository
}

// InboxClaim is what the boundary deduplicates on: this consumer's name and the
// inbound event's identity (§8.2, §12 mechanism 3).
//
// It is a value rather than three parameters because it travels as a unit and
// because a claim is a thing a reader can name. EventType is carried for the
// column, not for the key — the key is (consumer, event_id).
type InboxClaim struct {
	Consumer  string
	EventID   uuid.UUID
	EventType string
}

// UnitOfWork is this context's transaction boundary, over this context's own pool,
// and it owns the deduplication decision.
//
// Do opens a transaction, records the claim as its **first** statement, and calls
// fn only if the claim was new. It returns claimed=false for a record this consumer
// has already processed — having committed, because the transaction did real work
// (it tried the insert) and because rolling back would leave the offset uncommitted
// and the record redelivered forever.
//
// Everything fn does commits together with the claim and with the send-intent row
// in notifications.outbox, which fn never mentions (§5, §6.4, §11.3).
//
// The claim is a parameter rather than something fn does, and that is this plan's
// one structural departure from §11.3's sketch. The sketch has the use case call
// w.Inbox.Record(...) first, which makes "first" a property of a call site — and
// the plan's own §5/D4 argument is that an invariant living in a call site is an
// invariant someone forgets. With the two writes swapped, every assertion about a
// duplicate still passes; it has just inserted a notification first. Here there is
// nothing to swap: fn cannot run before the claim, and a duplicate cannot reach the
// notification insert or the event drain, because fn is not called at all. The
// boundary ends up owning both side-tables symmetrically — the deduplication write
// before the work, the event drain after it — which is what §11.3's "one
// transaction, three writes" describes, with the use case asking for one.
type UnitOfWork interface {
	Do(ctx context.Context, meta Metadata, claim InboxClaim, fn func(Work) error) (claimed bool, err error)
}
```

- [ ] **Step 2: Write `inbox.go`**

```go
package application

// The inbox is the idempotent consumer's memory (spec §12 mechanism 3, §11.3), and
// this file exists to hold the argument rather than a type.
//
// There is no InboxRepository interface here. Its only caller is the transaction
// boundary, which lives in infrastructure/postgres, so the port is declared there
// beside outboxAppender — the precedent accounts set for a port whose consumer is
// the boundary rather than a use case. A port exported from the application layer
// that nothing in the application layer calls is an invitation to call it, and the
// whole design of Do is that nobody may.
//
// What the mechanism is, for a reader who found this file looking for it:
//
// INSERT ... ON CONFLICT DO NOTHING reports one row affected when the event is new
// and zero when this consumer has seen it before. One round trip, atomic, and
// correct under concurrency — which a SELECT followed by an INSERT is not, and the
// difference shows up exactly during a consumer-group rebalance, when two members
// can briefly be processing the same record (§12).
//
// A duplicate is not a failure. §13.3: "a non-zero duplicate count is healthy, not
// an error." That is why Do reports it as a bool rather than an error, and why the
// worker logs it at INFO: putting the normal operation of an at-least-once
// transport on the same path as a broken database trains a reader to ignore both.
//
// The consumer name is part of the key because several logical consumers may share
// this database and each must process an event independently (§8.2). This design
// has one; the parameter is what makes a second one a wiring change rather than a
// migration.
//
// A duplicate still costs three round trips, not one: BEGIN, the conflicting
// insert, COMMIT. Nothing can know a record is a duplicate before BEGIN, so three
// is the floor for this architecture rather than a cost worth removing — but §11.5
// makes the duplicate path the demo's headline, so the number should be right.
```

- [ ] **Step 3: Write the failing tests**

`internal/notifications/application/process_user_registered_test.go`:

```go
package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

const testConsumer = "notifications.welcome-email"

func newProcess(t *testing.T) (*ProcessUserRegistered, *fakeUOW) {
	t.Helper()
	uow := &fakeUOW{claimed: true, notifications: &fakeNotifications{}}
	uc := NewProcessUserRegistered(
		uow,
		fixedIDs{next: uuid.MustParse("0199a4f0-0000-7000-8000-00000000000a")},
		fixedClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)},
		testConsumer,
	)
	return uc, uow
}

func TestProcessUserRegistered_ClaimsTheEventAndWritesTheNotification(t *testing.T) {
	uc, uow := newProcess(t)

	res, err := uc.Execute(context.Background(), validRecord(t, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Outcome != OutcomeRecorded {
		t.Fatalf("Outcome = %v, want recorded", res.Outcome)
	}
	if !uow.committed {
		t.Error("the transaction did not commit")
	}

	// The claim carries this consumer's name and the inbound event's identity —
	// the same id that becomes the gateway's Idempotency-Key. One identity, three
	// mechanisms.
	if uow.lastClaim.Consumer != testConsumer {
		t.Errorf("claim consumer = %q, want %q", uow.lastClaim.Consumer, testConsumer)
	}
	if uow.lastClaim.EventID.String() != "0199a4f0-0000-7000-8000-00000000000c" {
		t.Errorf("claim event id = %v", uow.lastClaim.EventID)
	}
	if uow.lastClaim.EventType != validRecord(t, nil).EventType {
		t.Errorf("claim event type = %q", uow.lastClaim.EventType)
	}
}

// The notification the transaction inserts carries an event, and the boundary is
// what turns that event into the send-intent row. The use case never mentions an
// outbox; this asserts that the *aggregate* is what carries the intent out, which
// is what makes the guarantee structural.
func TestProcessUserRegistered_TheNotificationCarriesTheSendIntent(t *testing.T) {
	uc, uow := newProcess(t)

	if _, err := uc.Execute(context.Background(), validRecord(t, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	inserted := uow.notifications.inserted
	if len(inserted) != 1 {
		t.Fatalf("inserted %d notifications, want 1", len(inserted))
	}
	events := inserted[0].PullEvents()
	if len(events) != 1 {
		t.Fatalf("the notification carries %d events, want 1", len(events))
	}
	if _, ok := events[0].(domain.WelcomeEmailRequested); !ok {
		t.Errorf("event is %T, want WelcomeEmailRequested", events[0])
	}
}

// The headline case, and note what it can no longer get wrong: fn is never called
// for a duplicate, so there is no ordering to assert and no way to write the two
// statements in the wrong order. The old shape needed a test that asserted the
// *sequence* of writes and admitted its own failure mode was silent.
func TestProcessUserRegistered_ADuplicateDoesNoWork(t *testing.T) {
	uc, uow := newProcess(t)
	uow.claimed = false

	res, err := uc.Execute(context.Background(), validRecord(t, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Outcome != OutcomeDuplicate {
		t.Fatalf("Outcome = %v, want duplicate", res.Outcome)
	}
	// It still commits: the transaction did real work, and rolling back would
	// leave the offset uncommitted and the record redelivered forever.
	if !uow.committed {
		t.Error("a duplicate rolled back; it must commit so the offset can advance")
	}
	if uow.fnCalls != 0 {
		t.Errorf("fn ran %d times for a duplicate, want 0", uow.fnCalls)
	}
	if len(uow.notifications.inserted) != 0 {
		t.Errorf("a duplicate inserted %d notifications, want 0", len(uow.notifications.inserted))
	}
}

func TestProcessUserRegistered_AMalformedRecordIsPermanentAndOpensNoTransaction(t *testing.T) {
	uc, uow := newProcess(t)
	msg := validRecord(t, func(e map[string]any) {
		e["data"].(map[string]any)["email"] = "not-an-address"
	})

	_, err := uc.Execute(context.Background(), msg)
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
	// Translation happens before the transaction. A record that can never be
	// processed must not cost a BEGIN, and must not be able to leave a transaction
	// open on the way to the dead-letter topic.
	if uow.calls != 0 {
		t.Errorf("opened %d transactions for a record that will never be valid", uow.calls)
	}
}

func TestProcessUserRegistered_AnUnhandledEventTypeIsIgnored(t *testing.T) {
	uc, uow := newProcess(t)
	msg := validRecord(t, nil)
	msg.EventType = "com.outboxexpress.accounts.user.deactivated"

	res, err := uc.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("Execute returned an error for an unhandled type: %v", err)
	}
	if res.Outcome != OutcomeIgnored {
		t.Fatalf("Outcome = %v, want ignored", res.Outcome)
	}
	// No transaction, and specifically no inbox row: an inbox row records that
	// this consumer processed an event, and writing one for an event it declined
	// to understand would hide the record from the notifier that finally handles
	// the type.
	if uow.calls != 0 {
		t.Errorf("opened %d transactions for an event it does not handle", uow.calls)
	}
}

// A failed id generation is transient, not permanent. The record is fine; the
// machine is not. Dead-lettering a valid registration because the entropy pool
// hiccuped would be a permanent decision about a temporary condition.
func TestProcessUserRegistered_AFailedIDIsTransient(t *testing.T) {
	uow := &fakeUOW{claimed: true, notifications: &fakeNotifications{}}
	uc := NewProcessUserRegistered(uow, fixedIDs{err: errors.New("no entropy")},
		fixedClock{}, testConsumer)

	_, err := uc.Execute(context.Background(), validRecord(t, nil))
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("err = %v, want ErrTransient", err)
	}
	if uow.calls != 0 {
		t.Errorf("opened %d transactions", uow.calls)
	}
}

// A database failure returns unclassified and the transaction does not commit, so
// the worker declines to commit the offset and Kafka redelivers (§12.2 rung 2).
// The important half is what did *not* happen: no offset commit, which is the only
// thing standing between a transient failure and silent loss.
func TestProcessUserRegistered_ADatabaseFailureRollsBack(t *testing.T) {
	uc, uow := newProcess(t)
	uow.notifications.err = errors.New("connection reset")

	if _, err := uc.Execute(context.Background(), validRecord(t, nil)); err == nil {
		t.Fatal("expected an error")
	}
	if uow.committed {
		t.Error("the transaction committed despite a failed insert")
	}
}

// A unique violation on source_event_id — the §8.2 backstop — comes out as an
// ordinary unclassified error, and that is the decision, not an omission. See the
// note after Step 5.
func TestProcessUserRegistered_ADuplicateSourceIsNotSpeciallyAbsorbed(t *testing.T) {
	uc, uow := newProcess(t)
	uow.notifications.err = errors.New(`ERROR: duplicate key value violates unique ` +
		`constraint "notifications_source_event_id_key" (SQLSTATE 23505)`)

	res, err := uc.Execute(context.Background(), validRecord(t, nil))
	if err == nil {
		t.Fatal("expected the constraint violation to surface as an error")
	}
	if res.Outcome != OutcomeRecorded && res.Outcome != 0 {
		t.Errorf("Outcome = %v; a failed pass reports no outcome", res.Outcome)
	}
	if uow.committed {
		t.Error("committed after a constraint violation")
	}
}

// The trace survives the hop — the assertion that keeps §9.2's promise meaningful
// across two databases.
func TestProcessUserRegistered_CarriesTheTraceOntoTheTransaction(t *testing.T) {
	uc, uow := newProcess(t)
	msg := validRecord(t, nil)
	msg.Headers = map[string]string{
		messaging.HeaderCorrelationID: "corr-9",
		messaging.HeaderTraceparent:   "00-abc-def-01",
	}

	if _, err := uc.Execute(context.Background(), msg); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if uow.lastMeta.CorrelationID != "corr-9" || uow.lastMeta.Traceparent != "00-abc-def-01" {
		t.Errorf("meta = %+v, want the record's headers", uow.lastMeta)
	}
}
```

Add to `fakes_test.go`:

```go
// fakeUOW runs fn immediately, unless claimed is false — in which case it must
// not run it at all, which is the property the real boundary provides and the
// reason this fake models the claim rather than a repository call.
type fakeUOW struct {
	calls         int
	fnCalls       int
	committed     bool
	claimed       bool
	lastMeta      Metadata
	lastClaim     InboxClaim
	notifications *fakeNotifications
	commitErr     error
}

func (f *fakeUOW) Do(
	ctx context.Context, meta Metadata, claim InboxClaim, fn func(Work) error,
) (bool, error) {
	f.calls++
	f.lastMeta = meta
	f.lastClaim = claim
	if !f.claimed {
		f.committed = true
		return false, nil
	}
	f.fnCalls++
	if err := fn(Work{Notifications: f.notifications}); err != nil {
		return false, err
	}
	if f.commitErr != nil {
		return false, f.commitErr
	}
	f.committed = true
	return true, nil
}

type fakeNotifications struct {
	inserted []*domain.Notification
	err      error
}

func (f *fakeNotifications) Insert(_ context.Context, n *domain.Notification) error {
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, n)
	return nil
}
```

`validRecord` already exists in `translate_test.go`; reuse it rather than writing a second fixture — the two tests want the same wire format for the same reason, and two copies would drift.

- [ ] **Step 4: Run them and watch them fail**

Run: `go test ./internal/notifications/application/ -run TestProcessUserRegistered -v`
Expected: FAIL — `undefined: NewProcessUserRegistered`.

- [ ] **Step 5: Write `process_user_registered.go`**

```go
package application

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// Outcome is what happened to one record. Exactly one of these is true of any
// processed record, which is why it is an enum and not a set of booleans: four
// independent flags would admit sixteen states for three legal ones, and every
// call site would assert a conjunction to pin one of them.
type Outcome int

const (
	// OutcomeRecorded: the claim was new and the work was done.
	OutcomeRecorded Outcome = iota + 1

	// OutcomeDuplicate: this consumer had already processed the event, so nothing
	// happened. §13.3: "a non-zero duplicate count is healthy, not an error." It
	// is the visible evidence that the inbox is doing its job.
	OutcomeDuplicate

	// OutcomeIgnored: an event type this consumer does not map. Not work, not a
	// failure, and not an inbox row (see handles).
	OutcomeIgnored
)

func (o Outcome) String() string {
	switch o {
	case OutcomeRecorded:
		return "recorded"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeIgnored:
		return "ignored"
	default:
		return "none"
	}
}

// ProcessResult is what one record's processing did. Every field on it becomes a
// field on the worker's log line, because a use case does not log (§6.1, §13.3) —
// and because a value that reaches a dashboard through a result struct is a value
// a unit test can assert.
type ProcessResult struct {
	Outcome Outcome

	// NotificationID is set on OutcomeRecorded, for the log line. It is the row a
	// human asking "did we send it?" would look up.
	NotificationID uuid.UUID
}

// ProcessUserRegistered is the consuming transaction, and the whole reason the
// notifications context is a separate service with a separate database.
//
// One transaction, three writes: the inbox row, the notification row, and — drained
// structurally by the same boundary, never mentioned here — the send-intent row in
// notifications.outbox. Then, and only then, the worker commits the offset.
//
// Two of those three writes are the boundary's, not this use case's. The inbox
// claim is Do's first statement and the send-intent is drained after fn returns;
// what this function asks for is one insert. That is deliberate — see
// UnitOfWork.Do — and it means neither ordering can be got wrong here.
//
// Why the atomicity is not optional (§11.3):
//
//	| Wrong shape                        | What breaks                          |
//	|------------------------------------|--------------------------------------|
//	| Mark processed, then do the work   | A crash between them loses the work  |
//	|                                    | permanently: redelivery deduplicates |
//	|                                    | it away. Silent data loss.           |
//	| Do the work, then mark processed   | A crash between them reprocesses the |
//	|                                    | work — the problem the inbox solves. |
//	| Mark processed in Redis, work in   | Two systems, no shared transaction.  |
//	| Postgres                           | Back to the dual-write problem.      |
//
// And the external send is deliberately not here. If this use case called the
// gateway after committing the inbox row, a transient gateway failure would be
// unrecoverable: the retry arrives, the inbox says "already processed", and the
// email is never sent. With an intent row the send retries independently, forever
// if need be, and the Idempotency-Key keeps a repeat harmless (§11.3, §13).
type ProcessUserRegistered struct {
	uow   UnitOfWork
	ids   IDGen
	clock Clock

	// consumer is this logical consumer's name in the inbox key. It is
	// constructor-injected rather than a constant because the column exists so
	// that a second consumer needs no migration (§8.2), and a constant here would
	// make the second one a code change in this file.
	consumer string
}

func NewProcessUserRegistered(uow UnitOfWork, ids IDGen, clk Clock, consumer string) *ProcessUserRegistered {
	return &ProcessUserRegistered{uow: uow, ids: ids, clock: clk, consumer: consumer}
}

func (uc *ProcessUserRegistered) Execute(ctx context.Context, msg messaging.Message) (ProcessResult, error) {
	// Not work, not a failure, and no inbox row: the offset advances and this
	// process forgets the record, which is the only outcome that leaves a future
	// notifier free to handle the type properly.
	if !handles(msg) {
		return ProcessResult{Outcome: OutcomeIgnored}, nil
	}

	// The anti-corruption layer. Accounts' published language is translated into
	// this context's vocabulary here and nowhere else; no code below this line has
	// ever seen what accounts calls its fields (§11.3).
	req, err := translate(msg)
	if err != nil {
		return ProcessResult{}, err // already ErrPermanent -> the dead-letter topic
	}

	// Everything nondeterministic happens before the transaction opens, so a BEGIN
	// is never held open across a failure that has nothing to do with the database.
	id, err := uc.ids.New()
	if err != nil {
		// Transient, not permanent: the record is valid and the machine is not.
		return ProcessResult{}, fmt.Errorf("%w: notification id: %v", ErrTransient, err)
	}

	notif, err := domain.RequestWelcome(id, req.Recipient, req.User, req.SourceEventID, uc.clock.Now())
	if err != nil {
		// A domain refusal of a well-formed message is permanent: the aggregate
		// will refuse the same input every time it is asked.
		return ProcessResult{}, fmt.Errorf("%w: event %s: %v", ErrPermanent, req.SourceEventID, err)
	}

	claim := InboxClaim{
		Consumer:  uc.consumer,
		EventID:   req.SourceEventID,
		EventType: msg.EventType,
	}

	// Inserting the aggregate is the only write this use case makes. The claim is
	// the boundary's first statement and the send-intent row is drained from the
	// events the aggregate recorded — which is why there is no inbox and no outbox
	// in Work, and no mention of either here (§5, §6.4).
	claimed, err := uc.uow.Do(ctx, metadataFrom(msg.Headers), claim, func(w Work) error {
		return w.Notifications.Insert(ctx, notif)
	})
	if err != nil {
		// Unclassified, and left that way. The worker treats an unclassified error
		// as transient and declines to commit the offset, so Kafka redelivers —
		// the safe default in this direction (see consuming.go).
		return ProcessResult{}, err
	}
	if !claimed {
		return ProcessResult{Outcome: OutcomeDuplicate}, nil
	}
	return ProcessResult{Outcome: OutcomeRecorded, NotificationID: notif.ID()}, nil
}
```

**Note on `notifications.source_event_id UNIQUE`, decided rather than deferred.** §8.2 calls that constraint "redundant with the inbox by design… a bug in the inbox path still cannot produce two notification rows", and an earlier draft of this plan tried to report reaching it as a third kind of duplicate. It cannot be reported. PostgreSQL aborts a transaction on any statement error, so a 23505 on the notification insert leaves the commit returning `ErrTxCommitRollback` — the outcome is an error out of `Do`, and no amount of branching inside `fn` changes that. Making it reportable would cost a savepoint on every insert, on the hot path, to distinguish two states that both mean "the inbox did not hold".

So it surfaces as an ordinary unclassified error with the constraint name in its text. The worker treats it as transient, the record climbs the ladder, and it dead-letters at `CONSUMER_MAX_DELIVERIES` with `notifications_source_event_id_key` in `dlt_error` — which is a louder and more actionable signal than the WARN line the earlier draft wanted, and costs nothing. There is therefore no `domain.ErrDuplicateSource`, no `isUniqueViolation` branch in the repository, and no result field for it.

The general lesson, and the reason this note stays in the plan: a fake unit of work that keeps running after a repository error is *more permissive than the database it stands for*, and a test green against that fake would have guarded nothing. Any fake that models a transaction must refuse to continue after an error, or it will eventually certify a design PostgreSQL cannot execute.

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/notifications/application/ -v`
Expected: PASS.

- [ ] **Step 7: Prove the duplicate short-circuit is structural, not asserted**

There is nothing to sabotage here, and that is the point of the change: try to write a version of `Execute` that inserts the notification before the claim. You cannot — `fn` runs inside `Do`, after the claim, or not at all. Confirm by attempting it for thirty seconds, then move on.

What to check instead is the fake: set `uow.claimed = false` and make `fn` panic. `TestProcessUserRegistered_ADuplicateDoesNoWork` must still pass, because `fn` is never called.

- [ ] **Step 8: Commit**

```bash
git add internal/notifications/application
git commit -m "feat(notifications): the boundary claims the event, so inbox-first is not a rule"
```

---

## Task 7: The persistence layer

**Files:**
- Create: `internal/notifications/infrastructure/postgres/uow.go`, `tracker.go`, `inbox_repo.go`, `notification_repo.go`, `outbox_repo.go`
- Test: `uow_integration_test.go`, `inbox_repo_integration_test.go`, `notification_repo_integration_test.go`, `tracker_test.go`

**Interfaces:**
- Consumes: `internal/notifications/application` (Tasks 4–6), `internal/notifications/domain`, `platformpg.WithTx`, `platformpg.Queryer`, `pgtest.Notifications`.
- Produces: `NewUnitOfWork(pool *pgxpool.Pool, envelopes application.EnvelopeFactory) *UnitOfWork`; `newInboxRepository(q platformpg.Queryer) *inboxRepository`; `newNotificationRepository(q platformpg.Queryer, tr *tracker) *NotificationRepository`; `NewOutboxRepository(q platformpg.Queryer) *OutboxRepository`.

Structurally accounts' `uow.go`, with the claim as the transaction's first statement and a second table drained into. The one genuinely new piece of SQL in the plan is the inbox's, and it is three lines that decide whether the whole design works.

The inbox repository is unexported — type and constructor both. Its only caller is the unit of work in this package, and an exported one would let a process take a deduplication decision outside the transaction that decision exists to protect.

- [ ] **Step 1: Write the failing integration test for the inbox**

`internal/notifications/infrastructure/postgres/inbox_repo_integration_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/application"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func claimFor(eventID uuid.UUID, consumer string) application.InboxClaim {
	return application.InboxClaim{
		Consumer: consumer, EventID: eventID, EventType: "com.example.thing"}
}

// The mechanism, against a real database. ON CONFLICT DO NOTHING affects zero rows
// for a repeat, which is what "not first" means — and it means it under
// concurrency, which no in-memory check can (§12 mechanism 3).
func TestInboxRepository_ClaimIsFirstExactlyOnce(t *testing.T) {
	// pgtest.Notifications truncates all three tables before it returns; a second
	// Truncate here would teach the next reader that it does not.
	_, pool := pgtest.Notifications(t)
	ctx := context.Background()
	eventID := uuid.New()
	repo := newInboxRepository(pool)

	first, err := repo.record(ctx, claimFor(eventID, "notifications.welcome-email"))
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if !first {
		t.Fatal("the first claim reported not-first")
	}

	again, err := repo.record(ctx, claimFor(eventID, "notifications.welcome-email"))
	if err != nil {
		t.Fatalf("second record: %v", err)
	}
	if again {
		t.Error("the second claim reported first; the inbox is not deduplicating")
	}
}

// The consumer column is part of the key, so two logical consumers process the
// same event independently (§8.2). This design has one consumer; the test is what
// keeps the column meaningful for the second.
func TestInboxRepository_TwoConsumersEachGetTheEventOnce(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	ctx := context.Background()
	eventID := uuid.New()
	repo := newInboxRepository(pool)

	for _, consumer := range []string{"notifications.welcome-email", "notifications.audit"} {
		first, err := repo.record(ctx, claimFor(eventID, consumer))
		if err != nil {
			t.Fatalf("record(%s): %v", consumer, err)
		}
		if !first {
			t.Errorf("%s did not get the event", consumer)
		}
	}
}

// Two concurrent claims for the same key: exactly one first, and no error. This is
// the property the primary key provides and a SELECT-then-INSERT would not — two
// notifiers in a rebalance window can be processing the same record, and the loser
// must learn that it lost rather than fail.
func TestInboxRepository_ConcurrentClaimsElectOneWinner(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	ctx := context.Background()
	claim := claimFor(uuid.New(), "notifications.welcome-email")
	repo := newInboxRepository(pool)

	const racers = 8
	results := make(chan bool, racers)
	errs := make(chan error, racers)
	start := make(chan struct{})
	for range racers {
		go func() {
			<-start
			first, err := repo.record(ctx, claim)
			if err != nil {
				errs <- err
				return
			}
			results <- first
		}()
	}
	close(start)

	firsts := 0
	for range racers {
		select {
		case err := <-errs:
			t.Fatalf("record: %v", err)
		case first := <-results:
			if first {
				firsts++
			}
		}
	}
	if firsts != 1 {
		t.Errorf("%d of %d racers claimed the event, want exactly 1", firsts, racers)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test -tags=integration ./internal/notifications/infrastructure/postgres/ -run TestInboxRepository -v`
Expected: FAIL — `undefined: newInboxRepository`.

- [ ] **Step 3: Write `inbox_repo.go`**

```go
// Package postgres is the notifications context's persistence layer: the inbox,
// the Notification repository, the consumer's own outbox, and the unit of work
// that claims an event and makes the send-intent insert structural.
package postgres

import (
	"context"
	"fmt"

	"github.com/AymanKastali/outboxexpress/internal/notifications/application"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// The idempotent consumer, in three lines of SQL.
//
// ON CONFLICT DO NOTHING is the whole mechanism: the statement inserts when the
// event is new and does nothing when the primary key already holds it, reporting
// which happened as the row count. One round trip, atomic, and correct under
// concurrency — which a SELECT followed by an INSERT is not, and the difference
// shows up exactly during a consumer-group rebalance, when two members can briefly
// be processing the same record (§12).
//
// There is no RETURNING clause. An earlier draft had one, on the theory that the
// statement needed to return something in order to report a conflict; it does not.
// pgconn.CommandTag.RowsAffected answers the only question being asked, and a
// RETURNING would put a result-row descriptor, a timestamptz on the wire and a
// time.Time decode on the first statement of every consuming transaction, for a
// value nothing reads. accounts' markOne already uses the row count for exactly
// this question.
//
// processed_at takes its column default, which is now() — and in PostgreSQL now()
// is transaction_timestamp(), so every row a transaction writes shares one instant.
// That is the right timestamp here: the inbox records when the consuming
// transaction happened, not when this statement within it did.
const recordInbox = `
INSERT INTO notifications.inbox (consumer, event_id, event_type)
VALUES ($1, $2, $3)
ON CONFLICT (consumer, event_id) DO NOTHING`

// inboxRepository is unexported, type and constructor both. Its only caller is
// this package's unit of work, and an exported one would let a process take a
// deduplication decision outside the transaction it exists to protect.
type inboxRepository struct {
	q platformpg.Queryer
}

func newInboxRepository(q platformpg.Queryer) *inboxRepository {
	return &inboxRepository{q: q}
}

// record claims the event for this consumer and reports whether the claim was new.
//
// A conflict is the *success* path, not an error: a repeat is normal operation of
// an at-least-once transport (§13.3), and returning an error for one would put it
// on the same path as a broken database.
func (r *inboxRepository) record(ctx context.Context, claim application.InboxClaim) (bool, error) {
	tag, err := r.q.Exec(ctx, recordInbox, claim.Consumer, claim.EventID, claim.EventType)
	if err != nil {
		return false, fmt.Errorf("postgres: claim inbox (%s, %s): %w",
			claim.Consumer, claim.EventID, err)
	}
	return tag.RowsAffected() == 1, nil
}
```

- [ ] **Step 4: Run it and watch the three tests pass**

Run: `go test -tags=integration ./internal/notifications/infrastructure/postgres/ -run TestInboxRepository -v`
Expected: PASS, all three.

- [ ] **Step 5: Write `tracker.go` and its unit test**

Copy `internal/accounts/infrastructure/postgres/tracker.go` and its test, substituting `*domain.Notification` for `*domain.User`. The comment explaining why the tracker exists — a repository records which aggregates it persisted, so the unit of work can drain their events without the use case asking — carries over unchanged. Adjust it to name this context's aggregate and add one sentence: the tracker is per-context for the same reason the recorder is, and since the two contexts' trackers hold different aggregate types there is nothing to share even if sharing were desirable.

- [ ] **Step 6: Write `notification_repo.go`**

```go
package postgres

const insertNotification = `
INSERT INTO notifications.notifications
    (id, user_id, recipient, kind, state, source_event_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`

// NotificationRepository writes the aggregate. Its constructor is unexported —
// newNotificationRepository — because the only legitimate caller is this package's
// unit of work, which binds it to a transaction and to a tracker. An exported
// constructor would let a process wire one against the pool, and a notification
// written outside the consuming transaction is the dual write this design exists to
// prevent.
type NotificationRepository struct {
	q  platformpg.Queryer
	tr *tracker
}

func newNotificationRepository(q platformpg.Queryer, tr *tracker) *NotificationRepository {
	return &NotificationRepository{q: q, tr: tr}
}

// Insert writes the row and tells the tracker, which is what makes the send-intent
// structural: the unit of work drains this aggregate's events on the way to commit,
// and the use case never mentions an outbox.
//
// sent_at is not written. It is NULL until the sender sets it, and passing the
// aggregate's zero time would write 0001-01-01 into a nullable column — a value
// that reads as "sent, a very long time ago" to every query that checks the column
// rather than the state.
func (r *NotificationRepository) Insert(ctx context.Context, n *domain.Notification) error {
	_, err := r.q.Exec(ctx, insertNotification,
		n.ID(), n.UserID(), n.Recipient().String(), n.Kind(),
		n.State().String(), n.SourceEventID(), n.CreatedAt(),
	)
	if err != nil {
		// No special case for the source_event_id UNIQUE. §8.2 calls that
		// constraint a deliberate backstop behind the inbox, and reaching it means
		// the inbox did not hold — but PostgreSQL aborts the transaction on a
		// 23505, so there is nothing this layer could translate it into that the
		// application layer could act on. It travels out as a wrapped driver error
		// with the constraint name in its text, which is what a human reading
		// dlt_error needs. See the note after Task 6, Step 5.
		return fmt.Errorf("postgres: insert notification %s: %w", n.ID(), err)
	}
	r.tr.track(n)
	return nil
}

var _ domain.NotificationRepository = (*NotificationRepository)(nil)
```

Its integration test: insert one, read the row back, assert every column including `state = 'pending'` and `sent_at IS NULL`; then insert a second notification with the same `source_event_id` on a fresh connection and assert the error names `notifications_source_event_id_key`, because that string is what a human replaying from the DLT will search for.

- [ ] **Step 7: Write `outbox_repo.go`**

Accounts' `Append`, with `idempotency_key` added to the column list and `e.IdempotencyKey` to the values. Keep the leading comment about `status`, `attempts` and `available_at` taking their column defaults, adjusted to name the sender as the owner of that bookkeeping.

```go
const insertOutbox = `
INSERT INTO notifications.outbox
    (event_id, aggregate_type, aggregate_id, event_type, schema_version,
     payload, headers, occurred_at, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
```

**Do not copy accounts' batch comment unexamined.** There it argues about N round trips inside a business transaction; here N is always 1, pinned by two tests — `TestRequestWelcome_StartsPendingAndRecordsExactlyOneEvent` and `TestProcessUserRegistered_TheNotificationCarriesTheSendIntent`. So take the single-envelope case to a plain `Exec` and say what the batch is actually for:

```go
// One event per notification today, pinned by the aggregate's tests, so the common
// case is a single Exec. The batch below is not a saving being realised — it is
// what keeps a second event type from arriving as a loop of round trips inside the
// consuming transaction, which is the cost accounts' comment is about (§11.1
// rule 4).
if len(envelopes) == 1 {
	e := envelopes[0]
	if _, err := r.q.Exec(ctx, insertOutbox, e.EventID, e.AggregateType, e.AggregateID,
		e.EventType, e.SchemaVersion, e.Payload, e.Headers, e.OccurredAt,
		e.IdempotencyKey); err != nil {
		return fmt.Errorf("postgres: append intent (event_id %s): %w", e.EventID, err)
	}
	return nil
}
```

While you are here, count the transaction: BEGIN, claim, notification, intent, COMMIT — **five round trips** on the happy path, three on a duplicate. The notification insert and the intent append are the one combinable pair (always one row each, always both-or-neither, no result gating the second), and combining them would cut lock-hold time by about a fifth. It is not done, because the tracker only drains after `fn` returns and `Insert`'s error has to surface from inside `fn`. Record the number and the trade; do not quote a comment about round trips while leaving the round trips uncounted.

- [ ] **Step 8: Write `uow.go`**

Accounts' `uow.go`, with three changes: the claim runs first and gates `fn`, the work set holds one repository, and there is no injected-appender field.

```go
// The two roles the boundary needs, both declared at their consumer. Neither is in
// the application package, because no use case mentions an inbox or an outbox —
// that is the whole point of §5, and an interface exported from application that
// nothing in application calls invites exactly the reach-in that Work's comment
// warns about.
type inboxClaimer interface {
	record(ctx context.Context, claim application.InboxClaim) (bool, error)
}

type outboxAppender interface {
	Append(ctx context.Context, envelopes []application.Envelope) error
}

type UnitOfWork struct {
	pool      *pgxpool.Pool
	envelopes application.EnvelopeFactory
}

func NewUnitOfWork(pool *pgxpool.Pool, envelopes application.EnvelopeFactory) *UnitOfWork {
	return &UnitOfWork{pool: pool, envelopes: envelopes}
}

// Do claims the event, runs fn only if the claim was new, and drains the
// aggregate's events into notifications.outbox on the way to commit.
//
// The three statements are in this order because the order is the mechanism, and
// putting it here rather than in the use case is what makes it unswappable — see
// application.UnitOfWork's doc comment and divergence 6.
func (u *UnitOfWork) Do(
	ctx context.Context,
	meta application.Metadata,
	claim application.InboxClaim,
	fn func(application.Work) error,
) (bool, error) {
	var claimed bool
	err := platformpg.WithTx(ctx, u.pool, func(tx pgx.Tx) error {
		// First statement, before any other write. A record this consumer has
		// already processed costs one insert that touches nothing, and fn — with
		// the notification insert and the event drain behind it — is unreachable.
		first, err := u.claimer(tx).record(ctx, claim)
		if err != nil {
			return err
		}
		if !first {
			// Commit, deliberately. The transaction did real work, and rolling back
			// would leave the offset uncommitted and the record redelivered forever.
			return nil
		}
		claimed = true

		tracker := newTracker()

		// The repository is bound to tx and handed to fn as the only way to reach
		// persistence. The pool is not reachable from here, so there is no path to
		// an untransacted write.
		if err := fn(application.Work{
			Notifications: newNotificationRepository(tx, tracker),
		}); err != nil {
			return err
		}

		// Drain what the repository touched. Nothing external happens in here: no
		// HTTP, no produce, no email, and no offset commit (§11.1 rule 2).
		if events := tracker.drain(); len(events) > 0 {
			envelopes, err := u.envelopes.From(events, meta)
			if err != nil {
				return err
			}
			if err := u.appender(tx).Append(ctx, envelopes); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

func (u *UnitOfWork) claimer(q platformpg.Queryer) inboxClaimer {
	return newInboxRepository(q)
}

func (u *UnitOfWork) appender(q platformpg.Queryer) outboxAppender {
	return NewOutboxRepository(q)
}

var _ application.UnitOfWork = (*UnitOfWork)(nil)
```

**No `outbox func(platformpg.Queryer) outboxAppender` field.** Accounts has one so a test can substitute a failing appender and assert "the outbox insert failed, so the user must not exist". That test is worth having here too, and it does not need the field: `u.envelopes.From` is an injectable failure point on the same code path two lines earlier, with identical rollback semantics — a `CloudEventFactory` whose `IDGen` fails on its second call proves "the drain failed, so the notification must not exist" exactly as well. The field would cost an indirect call and an allocation per record, forever, bought entirely with test money.

This is a deliberate asymmetry with accounts' `uow.go`. Do not add the field back for symmetry, and do not refactor accounts in this plan — Plan 1 is committed and its field has a test depending on it.

- [ ] **Step 9: Write the failing integration tests for the unit of work**

`internal/notifications/infrastructure/postgres/uow_integration_test.go`:

```go
//go:build integration

package postgres

// The headline assertion of the whole plan, against a real database: process one
// record and find three rows in three tables from one transaction — and the use
// case asked for one of them.
func TestUnitOfWork_OneRecordWritesInboxNotificationAndIntent(t *testing.T) { /* below */ }

// The refusal. A failed drain must leave neither the claim nor the notification
// behind: "the send-intent insert failed, so the notification must not exist."
// This is the dual-write problem, tested from the consuming side.
func TestUnitOfWork_AFailedIntentInsertLeavesNothing(t *testing.T) { /* below */ }

// The duplicate path, end to end through the transaction: the same event id twice
// leaves one inbox row, one notification, and — the assertion that matters — one
// send-intent. A second intent would be a second email.
func TestUnitOfWork_ADuplicateLeavesTheOutboxUntouched(t *testing.T) { /* below */ }

// And the duplicate ran no work at all, which the boundary guarantees rather than
// the use case: pass an fn that fails, and assert the transaction still commits and
// reports not-claimed.
func TestUnitOfWork_ADuplicateNeverCallsFn(t *testing.T) { /* below */ }

// The intent row carries the inbound event id as its idempotency_key, which is what
// makes §13's two remedies compose. Read the column, not the payload.
func TestUnitOfWork_TheIntentCarriesTheInboundEventID(t *testing.T) { /* below */ }

// The trace survives into the stored headers, so a registration and the email it
// causes share a traceparent across two databases.
func TestUnitOfWork_StoresTheTraceOnTheIntent(t *testing.T) { /* below */ }
```

Write each body following `internal/accounts/infrastructure/postgres/uow_integration_test.go`: `pgtest.Notifications(t)` (which truncates), construct the real `UnitOfWork` with the real `CloudEventFactory` over `ids.UUIDv7{}` and `clock.System{}`, run `ProcessUserRegistered.Execute` against it with a fixture record, then assert with `QueryRow` counts and column reads. For the failure case, hand the factory an `IDGen` that fails on its second call, so the notification insert succeeds and the drain does not.

Add a `pgtest.CountNotifications(t, pool) (inbox, notifications, outbox int)` helper beside the existing `CountAccounts`, so the assertions read as three numbers rather than three queries.

- [ ] **Step 10: Run them**

Run: `go test -tags=integration ./internal/notifications/... -v`
Expected: PASS. Then `make lint && go test ./...` for the unit suite.

- [ ] **Step 11: Prove the two refusals have teeth**

In `uow.go`, temporarily move the `tracker.drain()` block *before* the `fn(...)` call so it drains nothing and the append never runs. `TestUnitOfWork_AFailedIntentInsertLeavesNothing` must fail — if it passes, it is asserting the absence of rows that were never written for an unrelated reason. Restore.

Then make `record` always return `true`. `TestUnitOfWork_ADuplicateLeavesTheOutboxUntouched` must fail with two of everything, and `TestUnitOfWork_ADuplicateNeverCallsFn` must fail too. Restore.

- [ ] **Step 12: Commit**

```bash
git add internal/notifications/infrastructure internal/platform/pgtest
git commit -m "feat(notifications): three writes, one transaction, and the use case asked for one"
```

---

## Task 8: The consumer client, and the record mapping

**Files:**
- Modify: `internal/platform/kafka/client.go`, `internal/platform/kafkatest/kafkatest.go`, `internal/accounts/infrastructure/kafka/publisher.go`
- Test: `internal/platform/kafka/client_test.go`, `client_integration_test.go`

**Interfaces:**
- Produces: `NewConsumer(brokers, topics []string, group string) (*kgo.Client, error)`; `RecordToMessage(rec *kgo.Record) messaging.Message`; `RecordHeaders(msg messaging.Message) []kgo.RecordHeader` (accounts' `recordHeaders`, exported and moved); `kafkatest.ConsumerGroup(t) string`.

There is no `ConsumerConfig`. An earlier draft had one, reasoning from `ProducerConfig` — but `ProducerConfig` exists because it holds a real tunable with a real default (`RequestTimeoutOverhead`), and its own comment says it "holds only the producer's *values*". The consumer has no values: all six of §10.2's decisions are constants inside `NewConsumer`, which is already the "one place, asserted in one test" the config type was supposed to provide. What a struct would have added is a pass-through constructor, a `validate`, and a test asserting that a constructor copies its parameter.

`RecordToMessage` lives here rather than in `notifications/infrastructure/kafka` for two reasons, and the second one is fatal to the alternative. Its signature names only `kgo.Record` and `messaging.Message`, so there is no context in it — the same argument accounts' `recordHeaders` makes about itself ("this function's entire job is the type change"). And `internal/arch`'s `TestPresentationAndInfrastructureAreSiblings` fails any `presentation` package that imports an `infrastructure` one, so a worker calling it from `notifications/infrastructure` would not build.

- [ ] **Step 1: Write the failing tests for `NewConsumer`**

```go
func TestNewConsumer_RefusesAnIncompleteConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		brokers []string
		topics  []string
		group   string
	}{
		{name: "no brokers", topics: []string{"t"}, group: "g"},
		{name: "no topics", brokers: []string{"b:9092"}, group: "g"},
		// A consumer with no group id is not a consumer group member: franz-go
		// would happily consume without one, committing nothing and rejoining
		// nothing, and the notifier would look like it worked until it restarted
		// and reprocessed the topic from the beginning.
		{name: "no group", brokers: []string{"b:9092"}, topics: []string{"t"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewConsumer(tc.brokers, tc.topics, tc.group); err == nil {
				t.Error("NewConsumer accepted it")
			}
		})
	}
}

// The error names every missing field at once, for the reason LoadRelay joins its
// problems: a process that fails one field per restart is a process someone debugs
// three times.
func TestNewConsumer_NamesEveryMissingField(t *testing.T) {
	_, err := NewConsumer(nil, nil, "")
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"brokers", "topics", "group"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
```

And in `client_integration_test.go`:

```go
//go:build integration

// The property that cannot be asserted from a struct, and the one that matters
// most: this client commits nothing on its own.
//
// Produce three records, consume them without committing, close the client, and
// open a second one in the same group — it must receive all three again. An
// auto-commit ticker anywhere in the configuration makes this test flaky first and
// the system lossy later.
func TestNewConsumer_CommitsNothingOnItsOwn(t *testing.T) {
	broker := kafkatest.Broker(t)
	topic := kafkatest.Topic(t, 1)
	group := kafkatest.ConsumerGroup(t)
	produce(t, broker, topic, 3)

	first, err := NewConsumer([]string{broker}, []string{topic}, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if got := pollN(t, first, 3); got != 3 {
		t.Fatalf("first consumer got %d records, want 3", got)
	}
	first.Close() // no CommitRecords anywhere above

	second, err := NewConsumer([]string{broker}, []string{topic}, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(second.Close)
	if got := pollN(t, second, 3); got != 3 {
		t.Errorf("second consumer in group %s got %d records, want all 3 again — "+
			"something committed an offset that no code asked to commit", group, got)
	}
}

// auto.offset.reset=earliest: a new group sees the whole topic. This demo's entire
// subject is that nothing is lost, and a consumer starting at the end would lose
// everything produced before it first ran.
func TestNewConsumer_StartsAtTheBeginning(t *testing.T) {
	broker := kafkatest.Broker(t)
	topic := kafkatest.Topic(t, 1)
	produce(t, broker, topic, 2) // produced *before* the consumer exists

	cl, err := NewConsumer([]string{broker}, []string{topic}, kafkatest.ConsumerGroup(t))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(cl.Close)
	if got := pollN(t, cl, 2); got != 2 {
		t.Errorf("got %d records, want 2 — a fresh group did not start at the beginning", got)
	}
}
```

`produce` and `pollN` are local test helpers: `produce` uses `kafkatest.Producer(t)` and `ProduceSync`; `pollN` calls `PollRecords` with a deadline context and counts, failing with what it saw. No sleeps (§15).

- [ ] **Step 2: Run, fail, write `NewConsumer`**

```go
// NewConsumer builds the consumer group member of spec §10.2. Every option below
// is a decision, and three of them are the difference between at-least-once and
// something worse.
//
// There is no config struct. The consumer has no tunables — the six options are
// all constants — so a struct would hold nothing but these three arguments, and
// "set in one place, asserted in one test" is already what this function is.
func NewConsumer(brokers, topics []string, group string) (*kgo.Client, error) {
	var problems []error
	if len(brokers) == 0 {
		problems = append(problems, errors.New("kafka: consumer needs at least one of brokers"))
	}
	if len(topics) == 0 {
		problems = append(problems, errors.New("kafka: consumer needs at least one of topics"))
	}
	if strings.TrimSpace(group) == "" {
		problems = append(problems, errors.New("kafka: consumer needs a group id"))
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),

		// The offset is committed after the database commit, by the worker, for
		// the record it just processed. An automatic ticker would commit an offset
		// for a record whose transaction had not landed — and a crash in that
		// window is silent, permanent loss (§10.2, §11.3).
		kgo.DisableAutoCommit(),

		// Read only committed records. Nothing in this design produces
		// transactionally today; the setting is here because the day something
		// does, a consumer reading uncommitted records would act on writes that
		// were rolled back.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),

		// auto.offset.reset=earliest, argued in the test above.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
}
```

**No `OnPartitionsRevoked`.** §10.2 sketches a `stopAndForget`, and there is nothing here for it to forget: Task 10's delivery ledger is keyed by partition and reset by offset, so a revoked partition's entry is overwritten rather than leaked. franz-go's default revocation behaviour is already §10.2's requirement — it commits nothing the worker did not commit, and the new owner redelivers from the last committed offset, which the inbox absorbs.

- [ ] **Step 3: Move `RecordToMessage` and `recordHeaders` into this package**

`RecordToMessage` is new; `RecordHeaders` is `internal/accounts/infrastructure/kafka/publisher.go`'s `recordHeaders`, exported and moved, with its call site updated to `platformkafka.RecordHeaders`. Keep its sorted-key ordering and its comment.

```go
// RecordToMessage maps a fetched record onto the transport envelope.
//
// It reads the routing fields from *headers*, not from the payload, which is the
// promise §9.2 makes to consumers: "a consumer can route and deduplicate without
// deserialising". The payload stays opaque here and is parsed exactly once, in a
// context's own anti-corruption layer.
//
// Nothing in this function can fail, and the alternative is worse: a mapping that
// errored would put a malformed record on the same path as a broken broker, and a
// consumer's two responses to those are opposite — one dead-letters, one retries
// forever. Deciding which is which needs the record in hand, so every absent or
// malformed header yields a zero value and the decision is made a layer up.
func RecordToMessage(rec *kgo.Record) messaging.Message {
	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	// Parse errors ignored deliberately; see above. strconv returns 0 on failure,
	// which is what an absent header would also give, and nothing branches on the
	// schema version.
	version, _ := strconv.Atoi(headers[messaging.HeaderSchemaVersion])

	return messaging.Message{
		EventID:       headers[messaging.HeaderEventID],
		EventType:     headers[messaging.HeaderEventType],
		SchemaVersion: version,
		Key:           string(rec.Key),
		Payload:       rec.Value,
		Headers:       headers,
		Topic:         rec.Topic,
		Partition:     rec.Partition,
		Offset:        rec.Offset,
	}
}
```

The header map costs one allocation and seven string copies per record to answer five lookups (three here, two in `metadataFrom`). A linear scan over `rec.Headers` would be cheaper, and it is not taken: the dead-letter path echoes the original headers verbatim, and the only way to give it them without handing a `*kgo.Record` to a transport-agnostic port is to carry the map. That is a priced trade, not a free choice — say so in the comment rather than presenting the map as obvious.

Its tests: the routing headers are read; a headerless record maps without panicking and invents nothing; an unparseable `schema_version` is zero. Write them as they were specified in the first draft of this plan, in `internal/platform/kafka/client_test.go`.

- [ ] **Step 4: Add `kafkatest.ConsumerGroup(t)`**

One argument, because `brokers` and `topic` cannot contribute to a group name — the uniqueness comes from `t.Name()`, and `topicName(t)` already does the Kafka-legal character mapping a group name needs. Reuse it.

- [ ] **Step 5: Run everything**

Run: `make lint && go test ./internal/platform/... ./internal/accounts/... -v && go test -tags=integration ./internal/platform/kafka/ -v`
Expected: PASS. Accounts' publisher tests cover the moved `RecordHeaders`.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/kafka internal/platform/kafkatest internal/accounts/infrastructure/kafka
git commit -m "feat(kafka): the consumer group member, and the record mapping where both contexts can reach it"
```

---

## Task 9: The dead-letter publisher

**Files:**
- Create: `internal/notifications/infrastructure/kafka/dlt.go`
- Test: `dlt_test.go`, `dlt_integration_test.go`

**Interfaces:**
- Produces: `NewDeadLetterPublisher(client *kgo.Client, topic string) *DeadLetterPublisher` implementing `application.DeadLetterPublisher`.

One adapter, and §12.2 specifies exactly what it must write. The reason is recoverability: a record in the DLT has to be replayable onto the source topic without reconstruction (§12.3).

- [ ] **Step 1: Write the failing tests**

```go
// §12.2's header set, exactly: the original value and headers verbatim, plus six
// dlt_* headers. Verbatim is the requirement that matters — a publisher that
// dropped the original correlation id would break the trace of every replayed
// message.
func TestDeadLetterPublisher_CarriesTheOriginalAndTheDiagnosis(t *testing.T) {
	// original ce_id, ce_type, correlation_id all present and unchanged
	// value byte-identical to msg.Payload
	// dlt_reason = "permanent"; dlt_error contains the error text
	// dlt_deliveries = "3"
	// dlt_original_topic / _partition / _offset name where it came from
}

// The key survives, because a replayed record must land on the same partition as
// the original — per-aggregate ordering is a stated guarantee (§12.4), and a
// replay that changed partitions would reorder one user's events.
func TestDeadLetterPublisher_PreservesThePartitionKey(t *testing.T) {}

// Every error wraps ErrTransient. There is no permanent failure of dead-lettering
// the notifier could act on: if the DLT is unreachable, the right behaviour is to
// stop making progress, not to drop the record.
func TestDeadLetterPublisher_ErrorsAreTransient(t *testing.T) {}

// A very long error is truncated rather than allowed to fail the produce. A DLT
// produce that failed because the *diagnosis* was too big would lose the record it
// was trying to save.
func TestDeadLetterPublisher_TruncatesAnEnormousErrorText(t *testing.T) {}
```

The integration test asserts the same against a real broker, reading it back with `kafkatest.Records(t, dltTopic, 1)`.

- [ ] **Step 2: Write `dlt.go`**

```go
// maxDLTErrorLength bounds the error text. Kafka's default max.message.bytes is
// about a megabyte and an error string is normally a line — but "normally" is doing
// a lot of work in a header composed from an arbitrary error: a driver error
// carrying a whole query, or an error chain built in a retry loop, can be large.
const maxDLTErrorLength = 4096

// DeadLetterPublisher produces a record that will never succeed to the dead-letter
// topic, and waits for the durable ack.
//
// ProduceSync, not Produce: the offset is committed after this returns, so a
// fire-and-forget DLT produce would forget where the record was before it was
// durable anywhere else. §12.2: "the record is durable somewhere else before we
// forget where it was."
//
// The dlt_* header names come from platform/messaging, beside the header names the
// relay writes, because §12.3's replay tool reads them and is specified to have no
// notifier dependency.
type DeadLetterPublisher struct {
	client *kgo.Client
	topic  string
}
```

`Publish` starts from `platformkafka.RecordHeaders(msg)` — the same sorted-key conversion the relay's producer uses, which is where "verbatim" comes from — appends the six `dlt_*` headers using `messaging.HeaderDLT*`, sets `Key: []byte(msg.Key)` and `Value: msg.Payload`, calls `client.ProduceSync(ctx, rec).FirstErr()`, and wraps any error with `application.ErrTransient`.

- [ ] **Step 3: Run everything**

Run: `make lint && go test ./internal/notifications/... -v && go test -tags=integration ./internal/notifications/... -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/notifications/infrastructure/kafka
git commit -m "feat(notifications): a dead letter you can replay"
```

---

## Task 10: The notifier worker

**Files:**
- Create: `internal/notifications/presentation/worker/notifier.go`
- Test: `internal/notifications/presentation/worker/notifier_test.go`

**Interfaces:**
- Consumes: `application.ProcessUserRegistered`, `application.DeadLetterPublisher`, `application.Schedule`, `platformkafka.RecordToMessage`.
- Produces: `NotifierPolicy{RetryAttempts, MaxDeliveries int, DrainGrace time.Duration}`, `NewNotifier(client *kgo.Client, process Processor, dlt application.DeadLetterPublisher, schedule application.Schedule, clk application.Clock, log *slog.Logger, policy NotifierPolicy) *Notifier`, `(*Notifier).Run(ctx) error`.

The most decision-dense file in the plan. It owns five things: the ordering rule, the rewind, the retry ladder, the delivery count, and the drain.

**The ordering rule.** Process the record, commit the transaction, then commit the offset. Never any other order. A crash between the second and the third causes redelivery, the inbox claim conflicts, the duplicate is skipped, and the offset is committed — "that is the design working" (§10.2). Committing the offset first would turn a crash into permanent loss.

**The rewind, which is where the first draft of this plan was wrong.** Rung 2 of §12.2's ladder is "return without committing the offset; Kafka redelivers on the next poll cycle or after a rebalance". The second half of that sentence is true and the first half is not. franz-go hands a record out of `PollRecords` exactly once per session: its consume cursor has already advanced, so breaking out of the loop and polling again yields *later* offsets, not the failed one. Redelivery would happen only at the next rebalance or restart — and in the meantime the worker would process and commit a later record on the same partition, advancing the committed offset **past** the record it failed on. That is the loss the ordering rule exists to prevent, arriving through the back door.

So a failed record must be rewound explicitly, and the notifier does it the simple way: **`PollRecords(ctx, 1)`**. One record at a time means a failure has exactly one partition and exactly one offset to put back, with `client.SetOffsets`, and there is never a "rest of the fetch" whose partitions would each need their own rewind bookkeeping. The cost is more `PollRecords` calls, and they are cheap: franz-go keeps fetched batches buffered in memory and serves them without a network round trip. For a reference implementation whose subject is that nothing is lost, one record per poll is also simply the honest expression of §10.2's "one record per transaction".

Note `SetOffsets`' own documented caveat — call it outside a poll and not concurrently with a commit. Both hold here: the worker is single-goroutine and the rewind happens between polls.

**The retry ladder (§12.2), in order.** Three in-process attempts for a transient error, with the delay from `application.Schedule` — which is `platform/backoff`'s `Exponential` at a 50 ms base, not arithmetic written here. Then redelivery: rewind and stop, so the next poll hands the record back. Then the dead-letter topic, on a permanent error or after `CONSUMER_MAX_DELIVERIES` deliveries.

Retry only what was classified transient. A permanent error must not be retried at all — §11.3's whole argument is that retrying a record that will never succeed is a way of never noticing it — and each retry re-runs the use case's pure prefix (a payload unmarshal, an entropy read, an aggregate). That prefix is deterministic and cheap, so re-running it on a genuine transient failure is the right trade against splitting the use case into prepare-and-commit halves; re-running it three times for a malformed record is not.

**The delivery count** is in-memory, keyed by **`(topic, partition)`** with the offset in the value, and it is a heuristic for "stop retrying this one" — never a correctness input (§6.5). Keying it by offset as well is what an earlier draft did, and it needed pruning on success, pruning on dead-letter, pruning on revocation, an `OnPartitionsRevoked` callback to hang that on, and a test to bound the map. None of that is necessary: one member processes one partition in offset order, one record at a time, and a failure rewinds to the same offset — so at most one offset per partition can ever be in flight. An arriving record whose offset does not match the slot resets it. The map is then bounded by the partition assignment by construction, and §6.5's "that count resets on a rebalance or a restart" becomes true rather than aspirational.

**The drain.** Identical in shape and reason to the relay's `ce144d0`: on `SIGTERM`, stop polling, but let the record in flight finish its transaction *and its offset commit*. §13.6 says it plainly: "`terminationGracePeriod` must exceed one poll-and-commit cycle so a rolling restart does not manufacture redeliveries."

- [ ] **Step 1: Write the failing tests**

```go
// The ordering rule, asserted as a sequence. The most important test in the file:
// every other property could be wrong and the system would still be correct-ish,
// while this one being wrong is silent, permanent data loss.
func TestNotifier_CommitsTheOffsetAfterTheTransaction(t *testing.T) {
	// fake processor records "process"; fake committer records "commit"
	// assert ops == ["process", "commit"]
}

// The rewind. A transient failure must put the partition's cursor back to the
// failed record's offset, or the next poll returns a *later* record and committing
// it advances past the one that failed.
//
// This is the test that would have caught the first draft of this plan. Assert the
// rewind target explicitly: same topic, same partition, offset == the failed
// record's offset.
func TestNotifier_RewindsThePartitionAfterAFailedRecord(t *testing.T) {}

// And the negative, which is the loss case stated directly: after a failure the
// notifier must not commit any offset at all — not the failed record's, and not a
// later one.
func TestNotifier_CommitsNothingAfterAFailedRecord(t *testing.T) {}

// A transient failure retries in process, up to RetryAttempts, taking its delay
// from the Schedule port so the test needs no sleep.
func TestNotifier_RetriesTransientFailuresBeforeGivingUp(t *testing.T) {}

// A permanent failure is not retried at all, and its pure prefix runs once. Assert
// the processor was called exactly once.
func TestNotifier_DoesNotRetryAPermanentFailure(t *testing.T) {}

// A permanent failure goes to the DLT *and then* commits, in that order.
func TestNotifier_DeadLettersAPermanentFailureBeforeCommitting(t *testing.T) {}

// The ladder's top rung: the same record handed back MaxDeliveries times is
// dead-lettered with dlt_reason=max_deliveries even though every failure was
// transient. A transient error that never clears is a poison record in disguise.
func TestNotifier_DeadLettersAfterMaxDeliveries(t *testing.T) {}

// The count is per partition, not global. Two partitions each failing twice must
// not dead-letter either record.
func TestNotifier_TheDeliveryCountIsPerPartition(t *testing.T) {}

// And it resets when the partition moves on, which is what makes the map bounded
// without any pruning: a record at a new offset overwrites the slot.
func TestNotifier_TheDeliveryCountResetsOnANewOffset(t *testing.T) {}

// A failed DLT produce must not commit the offset. The one place where giving up
// would be losing the record: the DLT is where a record goes to be remembered.
func TestNotifier_AFailedDeadLetterDoesNotCommit(t *testing.T) {}

// A duplicate commits the offset — there is nothing left to do and the partition
// must advance. A duplicate that declined to commit would redeliver forever.
func TestNotifier_ADuplicateCommitsTheOffset(t *testing.T) {}

// An ignored record commits the offset too, and wrote no inbox row (the use case
// saw to that). The log line says ignored, which is how a reader finds out that
// accounts has started publishing something new.
func TestNotifier_AnIgnoredRecordCommitsTheOffset(t *testing.T) {}

// The drain, and the lesson of the relay's ce144d0: SIGTERM stops the polling, not
// the record in flight. Cancel ctx mid-transaction and assert that both the
// transaction and the offset commit completed.
func TestNotifier_ShutdownLetsTheRecordInFlightCommitItsOffset(t *testing.T) {}

// And the negative: a record that outlasts the grace is abandoned, loudly, with a
// line saying a redelivery is coming. Silence here makes a duplicate look like a
// mystery.
func TestNotifier_ARecordThatOutlastsTheGraceIsAbandonedLoudly(t *testing.T) {}
```

- [ ] **Step 2: Run, fail, write `notifier.go`**

The loop, in the order the code should read:

```go
func (n *Notifier) Run(ctx context.Context) error {
	// Two contexts, and the difference between them is graceful shutdown: ctx
	// means stop polling for new records, work means abandon the one in hand. The
	// same construction as the relay's, and the same argument — see
	// (*Relay).draining and spec §11.2's "a SIGTERM is not a crash".
	work, abandon := n.draining(ctx)
	defer abandon()

	for {
		if ctx.Err() != nil {
			n.stopping(work)
			return nil
		}

		// One record per poll. Not a throughput choice — it is what makes the
		// rewind below a single partition and a single offset instead of
		// per-partition bookkeeping over an abandoned fetch. franz-go serves this
		// from its in-memory buffer, so it is not a network round trip per record.
		fetches := n.client.PollRecords(ctx, 1)
		if err := fetches.Err0(); err != nil {
			if ctx.Err() != nil {
				n.stopping(work)
				return nil
			}
			// A fetch error is not a record error. Log it and poll again; the
			// client is already reconnecting underneath.
			n.log.Warn("fetch failed", "error", err)
			continue
		}

		rec := firstRecord(fetches)
		if rec == nil {
			continue
		}

		if err := n.handle(work, rec); err != nil {
			// Nothing was committed. Put the cursor back so the next poll hands
			// this same record over again — franz-go has already advanced past it,
			// and processing a later record on this partition would commit an
			// offset beyond the one that failed.
			n.rewind(rec)
			n.log.Error("record not processed; rewound for redelivery",
				"error", err, "topic", rec.Topic,
				"partition", rec.Partition, "offset", rec.Offset)
		}
	}
}

// rewind puts one partition's consume cursor back to rec's offset.
//
// SetOffsets' documentation asks for two things: call it outside a poll, and not
// concurrently with a commit. Both hold — the worker is one goroutine, and this
// runs between polls, after handle has returned without committing.
func (n *Notifier) rewind(rec *kgo.Record) {
	n.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		rec.Topic: {rec.Partition: {Epoch: rec.LeaderEpoch, Offset: rec.Offset}},
	})
}
```

`handle` is the ladder and the ordering rule together:

```go
func (n *Notifier) handle(ctx context.Context, rec *kgo.Record) error {
	msg := platformkafka.RecordToMessage(rec)
	part := partition{topic: rec.Topic, partition: rec.Partition}
	deliveries := n.deliveries.observe(part, rec.Offset) // 1 on first sight

	res, err := n.process(ctx, msg)
	switch {
	case err == nil:
		// The transaction has committed. Only now is the offset safe to commit.
		if err := n.client.CommitRecords(ctx, rec); err != nil {
			return err
		}
		n.logRecord(msg, res)
		return nil

	case errors.Is(err, application.ErrPermanent):
		return n.deadLetter(ctx, rec, msg, reasonPermanent, err, deliveries)

	case deliveries >= n.policy.MaxDeliveries:
		// A transient error that never clears is a poison record in disguise.
		return n.deadLetter(ctx, rec, msg, reasonMaxDeliveries, err, deliveries)

	default:
		// Transient, and the in-process retries are spent. Return without
		// committing; Run rewinds, and the count is what eventually escalates.
		return err
	}
}

// process runs the use case, retrying only what is worth retrying.
//
// A permanent error returns on the first attempt: retrying a record that will never
// succeed is a way of never noticing it. A transient one is retried up to
// RetryAttempts, with the delay from the Schedule port rather than arithmetic
// written here — the formula is platform/backoff's, and injecting it is what lets a
// test assert three attempts without waiting 350ms (§15).
//
// Each attempt re-runs the use case's pure prefix: one payload unmarshal, one
// entropy read, one discarded aggregate. That is deliberate. The alternative is
// splitting Execute into prepare-and-commit halves so the prefix runs once, which
// buys back microseconds on a path that is already sleeping, in exchange for a
// two-call use-case API and a Prepared value crossing the layer boundary.
func (n *Notifier) process(ctx context.Context, msg messaging.Message) (application.ProcessResult, error) {
	var res application.ProcessResult
	var err error
	for attempt := 0; attempt < n.policy.RetryAttempts; attempt++ {
		if attempt > 0 {
			if !n.wait(ctx, n.schedule.After(attempt-1)) {
				return res, err // the grace expired; keep the last error
			}
		}
		res, err = n.process.Execute(ctx, msg)
		if err == nil || errors.Is(err, application.ErrPermanent) {
			return res, err
		}
	}
	return res, err
}

// deadLetter produces the record somewhere durable, then commits the offset. §12.2:
// "the record is durable somewhere else before we forget where it was."
func (n *Notifier) deadLetter(
	ctx context.Context, rec *kgo.Record, msg messaging.Message,
	reason string, cause error, deliveries int,
) error {
	dlr := application.DeadLetterReason{
		Reason:     reason,
		Err:        cause.Error(),
		Deliveries: deliveries,
	}
	if err := n.dlt.Publish(ctx, msg, dlr); err != nil {
		// Do not commit. The DLT is where a record goes to be remembered, and
		// forgetting where it was because we could not remember it elsewhere is
		// exactly backwards.
		return errors.Join(cause, err)
	}
	if err := n.client.CommitRecords(ctx, rec); err != nil {
		return err
	}
	n.log.Error("record dead-lettered", "reason", reason, "error", dlr.Err,
		"deliveries", deliveries, "event_id", msg.EventID,
		"partition", msg.Partition, "offset", msg.Offset)
	return nil
}
```

Two `reason` constants — `reasonPermanent = "permanent"`, `reasonMaxDeliveries = "max_deliveries"` — passed as literals from the two switch arms. An earlier draft merged those arms and then called a `reasonFor(err, attempt, max)` helper that re-tested both halves of the condition the switch had just evaluated, in order to recover which arm it was in. Two arms and two literals is the same code with the question asked once.

The ledger:

```go
// partition is a ledger key. Not the offset — see observe.
type partition struct {
	topic     string
	partition int32
}

// deliveries counts how many times the worker has been handed the *current*
// record on each partition. No mutex: every call is on Run's goroutine, and a
// comment saying so beats a lock nobody needs.
type deliveries map[partition]*delivery

type delivery struct {
	offset int64
	count  int
}

// observe records one more delivery of rec's offset and returns the running count.
//
// An arriving record whose offset differs from the slot resets it, and that is what
// keeps this map bounded with no pruning anywhere: one member processes one
// partition in offset order, one record at a time, and a failure rewinds to the
// same offset — so at most one offset per partition can be in flight. The map is
// bounded by the partition assignment, and a revoked partition's stale entry is
// overwritten the first time its new owner hands us a record.
func (d deliveries) observe(p partition, offset int64) int {
	cur, ok := d[p]
	if !ok || cur.offset != offset {
		d[p] = &delivery{offset: offset, count: 1}
		return 1
	}
	cur.count++
	return cur.count
}
```

The log line, per §13.3 ("The notifier logs `duplicate` and `recorded` per record, which is what makes the inbox's work visible"):

```go
func (n *Notifier) logRecord(msg messaging.Message, res application.ProcessResult) {
	attrs := []any{
		"event_id", msg.EventID, "event_type", msg.EventType,
		"partition", msg.Partition, "offset", msg.Offset,
		"outcome", res.Outcome.String(),
	}
	switch res.Outcome {
	case application.OutcomeRecorded:
		n.log.Info("record processed", append(attrs, "notification_id", res.NotificationID)...)
	case application.OutcomeDuplicate:
		// INFO, not WARN. A non-zero duplicate count is the inbox doing its job
		// (§13.3), and logging it as a problem trains a reader to ignore it.
		n.log.Info("duplicate absorbed", attrs...)
	case application.OutcomeIgnored:
		n.log.Info("event type not handled by this consumer", attrs...)
	}
}
```

§13.3 asks for `duplicate` and `recorded` as fields, and this emits one `outcome` field instead. That is the same information without a pair of booleans that can disagree, and the switch is exhaustive over an enum rather than ordered over flags. Note the change in Task 14 so the spec and the code agree.

`draining` and `stopping` are copied from `internal/accounts/presentation/worker/relay.go`, comments included: **no logging from inside the `context.AfterFunc` callback** — `stop()` reports the callback has *started* and offers no way to join it, so a line from it can outlive `Run`. That bug was found by `-race` once already; do not reintroduce it.

- [ ] **Step 3: Prove the ordering test has teeth**

Move `CommitRecords` to before `n.process`. `TestNotifier_CommitsTheOffsetAfterTheTransaction` must fail. Restore.

- [ ] **Step 4: Prove the rewind tests have teeth**

Delete the `n.rewind(rec)` call. `TestNotifier_RewindsThePartitionAfterAFailedRecord` must fail. Restore.

This is the check that matters most in the whole plan, because the first draft shipped without the rewind and every other test still passed: the failed record's offset was simply never committed, which looks correct until a later record on the same partition commits past it.

- [ ] **Step 5: Prove the drain tests have teeth**

Replace `draining` with `func (n *Notifier) draining(ctx context.Context) (context.Context, context.CancelFunc) { return context.WithCancel(ctx) }`. Both drain tests must fail. Restore.

- [ ] **Step 6: Run under the race detector**

Run: `go test -race ./internal/notifications/... -v`
Expected: PASS with no race reports. If the detector fires on a log line, the callback is logging — fix the design, not the test.

- [ ] **Step 7: Commit**

```bash
git add internal/notifications/presentation
git commit -m "feat(notifier): the offset commits after the transaction, and a failed record is rewound"
```

---

## Task 11: The inbox purge

**Files:**
- Create: `internal/notifications/application/purge_inbox.go`, `internal/notifications/infrastructure/postgres/inbox_purge.go`, `internal/notifications/presentation/worker/purger.go`
- Test: `purge_inbox_test.go`, `inbox_purge_integration_test.go`

**Interfaces:**
- Produces: `InboxPurger` port with `PurgeProcessed(ctx context.Context, retention time.Duration, limit int) (int, error)`; `PurgeInbox` use case; `NewPurger(purge, log, policy)`.

**Follow `internal/accounts/application/purge_published.go` and `internal/accounts/infrastructure/postgres/outbox_relay.go`'s `PurgePublished` exactly**, including the two things an earlier draft of this plan got wrong by inventing its own shape. The port takes a `retention time.Duration`, not a computed `before time.Time` — so there is no `Clock` in this use case, because the cutoff is `now() - make_interval(secs => $1)` in SQL. That is not a stylistic preference: PostgreSQL's `now()` is the transaction timestamp, so computing the cutoff in Go and passing it in makes the boundary depend on two clocks agreeing. `make_interval` is there because formatting a Go duration into PostgreSQL interval text is a conversion with no upside. Two purge jobs in one codebase with two port signatures, one Clock and two SQL idioms is a worse outcome than either shape alone.

Mechanically the relay's purge job against a different table. What is different is what the retention *means*, and the doc comment has to say it: §13.2 — "Inbox retention is not storage hygiene — it is the boundary of the deduplication guarantee." A purged inbox row is an event this consumer will process again if it ever arrives again, and §12.3 records the incident that follows: reset the group to earliest and every event whose inbox row was purged is processed twice.

That is why the default is 7 days rather than 24 hours, and why the guidance is `> broker_retention + max_replay_lag + max_outage_duration`.

- [ ] **Step 1** Write the use case as `purge_published.go`'s twin: it loops until a pass removes fewer rows than the limit, returns `PurgeResult{Deleted int, Passes int}`, and stops on a cancelled context without treating cancellation as a failure. Assert with a fake purger that the loop terminates, that it respects the limit, and that it passes the configured retention through unchanged.
- [ ] **Step 2** Write the SQL. PostgreSQL has no `DELETE … LIMIT`, so the bound goes in a subquery — and the subquery returns `ctid`, not the primary key:

```sql
DELETE FROM notifications.inbox
 WHERE ctid IN (
    SELECT ctid FROM notifications.inbox
     WHERE processed_at < now() - make_interval(secs => $1)
     ORDER BY processed_at
     LIMIT $2
 )
```

`ORDER BY processed_at` matches `inbox_processed_at_idx` from migration 0001, so the bound page is an index walk rather than a sort. The `ctid` is the part worth explaining, because the obvious version — selecting `(consumer, event_id)` and comparing a row constructor — is measurably worse here. Measured on PostgreSQL 18.6 with 300k rows and 150k eligible, one page of 1000:

| Outer predicate | Buffers | Time |
|---|---|---|
| `(consumer, event_id) IN (…)` | 5023 shared hits | 6.75 ms |
| `ctid IN (…)` | 2023 shared hits | 1.64 ms |

The composite-key version spends 4000 of its 5023 hits on 1000 `inbox_pkey` lookups: the inner scan already located every row and reached its heap tuple, and the outer `DELETE` throws that away and buys it back by key. `ctid` is the identity it already had. 150k eligible rows is 150 pages, so this is ~600k redundant index searches per run, once a minute, for nothing.

Accounts' purge pays the same shape at half the cost because its key is one column. That is worth a line in the comment: this table's composite key is why the two purges' SQL is not identical, and it is the only reason.

- [ ] **Step 3** Integration test: insert rows with `processed_at` spread across two weeks (pass explicit timestamps), purge with a 7-day retention, assert only the old ones are gone and a second call deletes nothing. Also assert the empty case is cheap — the accounts purge added a partial index in migration `0002` specifically because "the index exists for the *empty* case", and here `inbox_processed_at_idx` already covers it (measured: 3 buffer hits, 0.036 ms on an empty eligible set), which is why this plan adds no migration.
- [ ] **Step 4** The worker is a ticker calling the use case and logging one line per run, structurally the relay's `purger.go`. Copy it.
- [ ] **Step 5** `make lint && go test -race ./internal/notifications/... && go test -tags=integration ./internal/notifications/...`
- [ ] **Step 6** Commit: `feat(notifications): purge the inbox, and say what the retention actually bounds`

---

## Task 12: Configuration and the process

**Files:**
- Modify: `internal/platform/config/config.go`, `config_test.go`, `.env.example`, `deploy/docker-compose.yml`, `deploy/kafka-init.sh`, `Makefile`
- Create: `cmd/notifier/main.go`

**Interfaces:**
- Produces: `config.LoadNotifier(getenv func(string) string) (Notifier, error)` — `config.Notifier`, not `NotifierConfig`, because the package's existing types are `API`, `Migrate` and `Relay` and a lone `*Config` suffix would be the odd one out — with `NotificationsDatabaseURL`, `KafkaBrokers`, `KafkaTopic`, `KafkaDLTTopic`, `KafkaGroupID`, `AdminAddr`, `LogLevel`, `MaxDeliveries`, `RetryAttempts`, `DrainGrace`, `InboxRetention`, `PurgeInterval`, `PurgeBatch`, `ChaosEnabled`.

§14's table is the specification; the existing `LoadRelay` is the shape. Every value validated at startup, every problem reported at once, and the error names the variable.

- [ ] **Step 1: Write the failing config tests**

Mirror `TestLoadRelay_Defaults`, `_ReadsEveryVariable`, `_ReportsEveryProblemAtOnce` and `_RejectsUnparseableValues`. The defaults from §14: `KAFKA_DLT_TOPIC=accounts.user.v1.DLT`, `KAFKA_GROUP_ID=notifications.welcome-email`, `ADMIN_ADDR=127.0.0.1:8083`, `CONSUMER_MAX_DELIVERIES=5`, `CONSUMER_RETRY_ATTEMPTS=3`, `INBOX_RETENTION=168h`, `PURGE_INTERVAL=1m`, `PURGE_BATCH=1000`, `CHAOS_ENABLED=false`, `LOG_LEVEL=info`.

`RELAY_DRAIN_GRACE` is reused rather than given a notifier-specific name, because §14 lists it for "relay, sender" and the notifier needs the identical thing for the identical reason. Add the notifier to that row's process list when you touch the spec in Task 14.

Two validations that are not in §14 and should be:

```go
// A DLT topic equal to the source topic would make every dead-lettered record
// arrive back at this consumer, which is an infinite loop that looks like a
// working system until the DLT is the only thing in the cluster.
if cfg.KafkaDLTTopic == cfg.KafkaTopic {
	problems = append(problems, fmt.Errorf("%w: KAFKA_DLT_TOPIC must differ from KAFKA_TOPIC", ErrInvalid))
}

// An inbox retention shorter than the broker's own retention is the §12.3
// incident, configured. It cannot be checked against the broker from here — the
// value lives in Kafka — so the floor is the documented minimum instead, and the
// README carries the real rule: > broker_retention + max_replay_lag +
// max_outage_duration.
if cfg.InboxRetention < 24*time.Hour {
	problems = append(problems, fmt.Errorf(
		"%w: INBOX_RETENTION below 24h; it bounds the deduplication guarantee, not disk use",
		ErrInvalid))
}
```

- [ ] **Step 2: Implement `LoadNotifier`** following `LoadRelay` exactly, including `errors.Join` for the problem list and `positive(...)` for the counts.

- [ ] **Step 3: Write `cmd/notifier/main.go`**

Following `cmd/relay/main.go`: load config, build the logger, open the *notifications* pool only, build the consumer client with `platformkafka.NewConsumer`, wire the use case with `ids.UUIDv7{}` and `clock.System{}`, wire the retry schedule as `backoff.NewExponential(50*time.Millisecond, cfg.RetryCap)`, start the admin listener and the two workers under an `errgroup`, and — the part that makes the drain mean anything — order every `defer` so that it runs after `g.Wait()`.

Readiness is not hand-rolled. `cmd/relay/main.go` and `cmd/api/main.go` both do it with the same two calls, and this is the third: `migrations.Ready(ctx, pool, migrations.Latest(migrations.Notifications))`. "Like the relay's" is how a codebase ends up with three different readiness queries; name the functions.

```go
// Every defer below runs after g.Wait(), and therefore after the notifier's
// drain: the pool, the Kafka client, the admin listener. That ordering is what
// makes CONSUMER drain grace mean anything — a pool closed while a record's
// transaction was still committing would turn a graceful shutdown into the crash
// the drain exists to avoid.
```

`internal/arch`'s `TestNoProcessWiresBothContexts` will fail if this file imports anything from `internal/accounts`. It must not, and there is nothing it would want.

- [ ] **Step 4: Compose, topics, Makefile, env**

- `deploy/kafka-init.sh` creates `accounts.user.v1.DLT` alongside the source topic. Same partition count, because a replayed record must be able to land on the partition its key hashes to.
- `deploy/docker-compose.yml` gains the `notifier` service: `NOTIFICATIONS_DATABASE_URL`, the Kafka variables, `ADMIN_ADDR=0.0.0.0:8083` inside the container with the port published to `127.0.0.1:8083`, and `depends_on` the migration job and Kafka.
- `Makefile`: `run-notifier`, and the notifier's admin URL in `make demo`'s output.
- `.env.example`: every §14 variable the notifier reads, with the `INBOX_RETENTION` comment carrying the real rule.

- [ ] **Step 5: Run the process by hand**

This step is not optional and it is not covered by any test. Start the stack, register a user through the API, and watch the record land:

```bash
make up && make migrate
go run ./cmd/api & go run ./cmd/relay & go run ./cmd/notifier &
curl -sS -XPOST localhost:8080/users -d '{"email":"ada@example.com","display_name":"Ada Lovelace"}'
```

Expected: one relay pass line with `published:1`, one notifier line with `recorded:true duplicate:false`, and three rows in `oe_notifications`. Then send `SIGTERM` to the notifier mid-load and confirm the drain line rather than an abandoned record.

Two duplicate-signalling log lines caught a real bug once (`b9600ad`): two processes logging the same sentence made a stop look like it happened twice. Read the notifier's output for the same problem before committing.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/config cmd/notifier deploy Makefile .env.example
git commit -m "feat(notifier): the process, and a DLT topic that cannot be the source topic"
```

---

## Task 13: The path, end to end

**Files:**
- Create: `internal/notifications/presentation/worker/consumepath_integration_test.go`
- Test: itself

**Interfaces:** none. This task adds no production code. If it needs any, something earlier is wrong.

Plan 2 ended with `relaypath_integration_test.go`, which drove the real relay against a real Postgres and a real Kafka. This is its counterpart, and it is the first test in the project that exercises both halves of the pattern: the relay produces, the notifier consumes, and the assertions are about what §18 promises.

- [ ] **Step 1: The happy path**

Register through the real accounts unit of work, run one real relay pass, then run the real notifier until it has processed the record. Assert: one row in each of the three notifications tables, the notification's `state='pending'`, and its `source_event_id` equal to the accounts outbox row's `event_id`. Poll with `waitFor` and a deadline; no sleeps (§15).

- [ ] **Step 2: The duplicate path (§11.5), which is the whole point**

Produce the *same* record twice — the relay's crash window simulated by publishing one outbox row's payload and headers to the topic a second time, with the same `ce_id`. Run the notifier until both offsets are committed. Assert:

- one inbox row,
- one notification,
- **one** send-intent row in `notifications.outbox`,
- and one notifier line with `duplicate=true`.

That last assertion is what makes the demo honest. §13.3: the duplicate count is "what makes the inbox's work visible", and a test that only counted rows would pass against an implementation that silently swallowed the second record without ever recording that it had.

- [ ] **Step 3: Redelivery without an offset commit**

Process a record with a processor that commits the transaction and then returns an error before the offset commit — the crash window of §12.5's `crash-before-offset-commit`, without the chaos hook. Restart the notifier. Assert the record is redelivered, absorbed as a duplicate, and that the tables still hold exactly one of everything.

This test double and §12.5's chaos hook inject the same fault by different means, and they must not diverge: when Plan 5 adds the `Chaos` port, this test should be rewritten to arm it rather than left as a second, drifting definition of the same window. Note that in Plan 5's plan.

This is the test that would catch a broken implementation, and it is worth writing carefully: it is the only test in the project that proves the *ordering* rule end to end rather than through a fake committer.

- [ ] **Step 4: A permanent failure reaches the DLT**

Produce a record with a malformed payload. Assert it appears on `accounts.user.v1.DLT` with the original value verbatim and `dlt_reason=permanent`, that the source offset was committed, and that no notification exists.

- [ ] **Step 5: Run the full suite from a cold cache**

```bash
go clean -testcache
make lint && go test -race ./... && make test-integration
```

Expected: every package green. The integration suite now starts two containers and both contexts' schemas; if the run time roughly doubles, that is the cost of the second half of the pattern and it is fine.

- [ ] **Step 6: Commit**

```bash
git add internal/notifications/presentation/worker
git commit -m "test(notifications): the duplicate path, proved rather than asserted"
```

---

## Task 14: Documentation, and the spec's corrections

**Files:**
- Modify: `README.md`, `docs/superpowers/specs/2026-08-26-transactional-outbox-demo-design.md`, this plan
- Test: none

- [ ] **Step 1: Annotate §11.3 in the spec** with the divergences this plan records — `msg.Metadata()`, `IDGen`'s error, the package qualifier, and the inbox claim moving into the unit of work. Write them as corrections to the sketch, in the spec's own voice, the way §13.3's stats paragraph was corrected in `d7107ee`. A spec that keeps a sketch nobody implemented is a spec readers learn to distrust.

The fourth is the substantive one and deserves the most space: §11.3's sketch puts the inbox insert in the use case, which makes the design's headline ordering a property of a call site. §5/D4 already argues that such rules do not survive, and the boundary can enforce this one by construction.

- [ ] **Step 2: Correct §6.5's field list.** It specifies `Attempt` on the shared transport type. Add the argument against it: the field's only writer and only reader are two lines apart in one worker in one context, so it buys nothing and costs a paragraph on the wire contract explaining that one member of the wire contract is not part of the wire contract. The count travels to the dead-letter headers on the reason struct instead. §6.5's own case against a shared domain model is the case against it.

There is **no** third error class to add to §12.2. An earlier draft of this plan proposed one for "an event type this consumer does not handle" and then found it was a predicate, not an error: it was constructed with `%w`, unwrapped three lines later, and converted into a non-error, while making the worker's classification switch three-valued for a case that must never reach it. §12.2's two classes stand unamended.

- [ ] **Step 3: Add the notifier to §14's `RELAY_DRAIN_GRACE` row**, add `CONSUMER_RETRY_CAP` if the schedule needs a cap variable, and add the two new validations (`KAFKA_DLT_TOPIC ≠ KAFKA_TOPIC`, `INBOX_RETENTION ≥ 24h`) to §14's prose.

- [ ] **Step 4: Correct §13.3's notifier line.** It asks for `duplicate` and `recorded` as separate fields; the implementation emits one `outcome` field, because the three outcomes are mutually exclusive and two booleans that can disagree are a worse contract than one enum. Same information, one field.

- [ ] **Step 5: Correct §12.2 rung 2, which is wrong as written.** "Kafka redelivers on the next poll cycle" is not what happens: franz-go hands a record out once per session, so declining to commit does not rewind anything, and the worker would process a later record on the same partition and commit past the one that failed. Redelivery without a restart or a rebalance requires an explicit rewind. Say so, and say what the notifier does about it (`PollRecords(ctx, 1)` plus `SetOffsets`). This is the most consequential correction in the plan: the sentence as written describes silent data loss and reads like a description of at-least-once delivery.

- [ ] **Step 6: README.** The consuming half needs three things a reader cannot get from the code: what the notifier's log line means (`outcome`, and why a stream of `duplicate` is good news); the guided duplicate demo, as commands to run and output to expect; and the honest limit — inbox retention is the boundary of the deduplication guarantee, so a group reset past it reprocesses, and that is a property of the design rather than a bug in it.

- [ ] **Step 7: Append a post-implementation review section to this plan**, as Plan 2 did: what the code taught that the plan got wrong, and which of those generalise to Plan 4. Two candidates are visible from here — whether `SetOffsets` behaves as documented under a rebalance mid-rewind, and whether one-record polling costs anything measurable against the batch it replaced.

- [ ] **Step 8: Commit**

```bash
git add README.md docs
git commit -m "docs: the consuming half, and correct 11.3, 12.2 and 6.5 where they were wrong"
```

---

## Self-review

Run against the spec with fresh eyes, per the plan-writing checklist.

**Spec coverage.** §6.5 → Task 1 (with one field refused, and the refusal argued). §7's notifications column → Tasks 2, 3, 6. §8.2's tables → Tasks 3, 7 (the schema already exists in `migrations/notifications/0001_init.sql`; this plan adds no migration, verified by measuring the purge and the repositories against the indexes that are there). §10.2 → Tasks 8, 10. §11.3 → Tasks 4, 5, 6, 7. §11.5 → Task 13. §12.2 → Task 10, with rung 2 corrected. §12.3's retention → Task 11. §13.2's inbox purge → Task 11. §13.3's notifier line → Task 10. §13.5's `:8083` → Task 12. §14's notifier variables → Task 12. §15's levels → distributed across every task, with the end-to-end invariant in Task 13.

**Deliberately out of scope**, named so a reader does not think they were forgotten:

- **§12.5's chaos hooks and the `Chaos` port.** They span all four processes and belong with the process that completes the set. Task 13 proves the same two windows with tests instead, which is what §12.5 says the hooks are: "the tests of §21 exposed as buttons." Task 13, Step 3 notes that the test double and the hook must not become two definitions of one fault.
- **§13.4's readiness for group membership.** franz-go exposes it only indirectly; it goes with the admin surface work in Plan 5. Task 12 wires `/healthz` and a pool-and-migration `/readyz` using `migrations.Ready`, like the other two processes.
- **§12.3's `replay-dlt` command.** A separate tool. Task 1 puts the `dlt_*` header names in `platform/messaging` so that it can read them without importing the notifier.
- **Everything the `sender` owns** — the email gateway, the `Idempotency-Key` call, `notifications.state = 'sent'`, the notifications outbox purge and its missing purge index. That is Plan 4. This plan writes the intent row and stops, which is the seam §11.3 and §11.4 draw. Task 3's `repository.go` carries a specific instruction for it: add the transition-mediating repository method rather than going around `MarkSent` with a bare `UPDATE`, or the state machine ends up guarding nothing and §8.2's own complaint comes true from the inside.

**Placeholder scan.** Test bodies are specified by name, assertion list and argument rather than written out in four places: Task 7's `uow_integration_test.go`, Task 8's `RecordToMessage` tests, Task 9's four DLT tests, and Task 10's fifteen notifier tests. That is a deliberate exception to "show the code", taken because each has a near-exact counterpart already committed in `internal/accounts` — and a plan that transcribes 600 lines of existing test scaffolding buries the assertions that are actually new. Every one names its subject, its assertion and its reason; none says "test the above". If any turns out underspecified, that is a plan bug worth recording in the post-implementation section.

**Type consistency.** `application.Metadata` is declared in Task 6 and used in Task 5; Task 5's interface block says so, and the two must be done in order or the type moved. `Envelope.IdempotencyKey` (Task 5) is read by Task 7's `insertOutbox`. `Outcome` (Task 6) is switched on by Task 10's `logRecord`. `InboxClaim` (Task 6) is built by the use case, passed through `UnitOfWork.Do`, and consumed by Task 7's `inboxRepository.record`. `UnitOfWork.Do`'s `(claimed bool, err error)` is the one signature change from §11.3 that ripples: Task 6's fake, Task 7's real implementation and Task 7's integration tests all depend on it. `DeadLetterReason` (Task 4) is built in Task 10 and written in Task 9. `messaging.HeaderDLT*` and `messaging.SchemaBase` (Task 1) are read in Tasks 9 and 5 respectively. `application.Schedule` (Task 4) is implemented by `platform/backoff` and wired in Task 12.

**What the first draft of this plan got wrong**, kept because the corrections are the interesting part and because two of them are patterns Plan 4 would have repeated:

1. **Rung 2 of the retry ladder did not exist.** Declining to commit an offset does not rewind franz-go's cursor, so the failed record was never redelivered within the session and the next record on its partition would have committed *past* it. Silent loss, in the one design whose subject is that nothing is lost, and every other test still passed. Fixed with one-record polling and an explicit `SetOffsets`; Task 10, Step 4 checks the test has teeth. Plan 4's sender polls a table rather than a topic and does not inherit this — but it does inherit the lesson that "the framework will redeliver it" is a claim to verify against the framework, not from the shape of the API.
2. **"Inbox first" was a comment in a call site.** The plan reproduced accounts' argument that such rules do not survive and then made an exception for the one rule this plan exists to demonstrate — with a test that admitted its own failure mode was silent. Moved into the transaction boundary, where the ordering is not expressible any other way.
3. **`ConstraintCaught` was a state PostgreSQL cannot produce**, and the plan deferred deciding to a runtime observation. The deeper problem was the fake: a `fakeUOW` that keeps running after a repository error is more permissive than the database it stands for, and a test green against it guards nothing. Any fake modelling a transaction must refuse to continue after an error.
4. **A four-boolean result for three mutually exclusive outcomes**, which made every assertion a conjunction and made `logRecord`'s branch order load-bearing.
5. **`50ms · 2^n` written out** in a package whose sibling `platform/backoff` was written for this exact consumer and says so in its package doc, with the overflow already handled.
6. **`presentation` importing `infrastructure`**, which `internal/arch` forbids — the record mapping belonged in `platform/kafka` all along, where it also stops duplicating accounts' `recordHeaders`.
7. **A grep test that had to be weakened until it permitted most of what it forbade.** A text search cannot tell this context's `user_id` from accounts', so it asserts the one string that is unambiguous and says plainly that the rest is the doc comment's job.
8. **Costs quoted without being counted**: a batch comment about round trips over a table that always appends one row, "a duplicate costs one round trip" for a path that costs three, a purge whose index claim covered the inner scan and not the join back, and a delivery map with an unbounded key.
