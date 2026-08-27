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
- **The header names are constants in `platform/messaging`, not string literals.** `HeaderEventID`, `HeaderEventType`, `HeaderSpecVersion`, `HeaderSchemaVersion`, `HeaderContentType`, `HeaderCorrelationID`, `HeaderTraceparent`. The relay writes them; this plan reads them. Two contexts agreeing on a spelling by each holding their own literal is how a contract breaks quietly.
- **`platformpg.WithTx` owns begin/rollback/commit and returns `fn`'s error unwrapped**, so domain sentinels survive the trip out. This context's unit of work uses it. Do not write a second one.
- **`platformpg.Queryer` is the "runs on a transaction or a pool" interface** with `Exec` and `QueryRow` and deliberately no `Query`. The inbox's `ON CONFLICT` needs only `QueryRow`; do not widen the interface.
- **Each context has its own `pgxpool` and its own `uow.go`** (§16). There is no connection on which a cross-context transaction could be written, and `internal/arch`'s `TestNoProcessWiresBothContexts` already fails any `main` that opens both.
- **Ports live beside what they are about, not in a file called `ports.go`.** This is `internal/accounts/application/doc.go`'s stated convention as of commit `3055f06`, written down precisely so this plan would inherit the considered layout: the repository port sits beside the type it returns, the envelope factory beside the envelope it produces and its only implementation, a single-consumer port beside its consumer, and `machine.go` holds only the residue that genuinely belongs nowhere (`Clock`, `IDGen`).
- **The domain's `eventRecorder` is unexported and per-context, by design.** `internal/accounts/domain/recorder.go` says so: "the notifications context is a separate model and gets its own recorder if it needs one. Fifteen duplicated lines cost less than a type two bounded contexts must agree on." Copy it. Do not extract it into a shared kernel, and do not import the accounts one — `internal/arch` forbids it and it would be wrong even if it did not.
- **`domain.Event` exposes routing only** — `EventType`, `AggregateType`, `AggregateID`, `OccurredAt` — and deliberately has no `SchemaVersion`, because a wire version is message contract, not domain vocabulary. This context's `Event` interface is the same four methods for the same reasons, in its own package.
- **A use case never logs and never takes a logger.** It returns a result struct; the presentation layer logs it (§6.1, §13.3). Every number on a log line must be a field on a result struct, which is what makes it assertable in a unit test.
- **The lesson of `ce144d0`: `SIGTERM` is not a crash.** The relay drains the pass in flight for `RELAY_DRAIN_GRACE` because a produce that is acked but not yet marked is a duplicate the next pass will create. The notifier has the identical window — a database commit that has landed but whose offset is not yet committed — and Task 11 drains it for the identical reason. Do not implement the loop as `for { select { case <-ctx.Done(): return } }`.
- **The lesson of `f6f88ea`: observation is not the delivery path.** Any count-the-table query is pool-bound, read after the commit, and its failure is recorded on the result rather than returned. PostgreSQL aborts a transaction on any statement error, so an observability read inside one can undo a durable commit.
- **JSONB normalises stored JSON.** It sorts object keys and drops insignificant whitespace, so a payload read back out of `notifications.outbox` is semantically identical to but not byte-identical with what the envelope factory produced. Do not assert byte equality between the two.
- **`internal/platform/kafkatest` and `internal/platform/pgtest` are the only non-`_test.go` files allowed to contain test-only code**, because a container helper is imported by other packages' tests. `kafkatest` already gives you `Broker(t)`, `Producer(t)`, `Topic(t, partitions)` and `Records(t, topic, want)`. Task 9 adds a consumer-side helper to it and nothing to any other package.
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

A fifth, smaller note: §11.3's table of wrong shapes is worth copying into the use case's doc comment nearly verbatim. It is the clearest statement in the spec of why this transaction is not optional, and it belongs where someone editing the transaction will read it.

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
    inbox.go                InboxRepository, beside nothing else — it is its own idea
    envelope.go             Envelope (with IdempotencyKey), EnvelopeFactory, CloudEventFactory
    translate.go            the anti-corruption layer: WelcomeRequest, translate
    consuming.go            ErrTransient, ErrPermanent, DeadLetterPublisher
    process_user_registered.go   the consuming transaction
    purge_inbox.go          the retention job (§13.2)
  infrastructure/
    postgres/
      uow.go                this context's transaction boundary + event drain
      tracker.go            records which aggregates were persisted
      inbox_repo.go         Record: ON CONFLICT DO NOTHING -> first bool
      notification_repo.go  Insert
      outbox_repo.go        Append, including idempotency_key
      inbox_purge.go        the pool-bound delete loop
    kafka/
      consumer.go           kgo.Record -> messaging.Message
      dlt.go                DeadLetterPublisher: original record + dlt_* headers
  presentation/
    worker/
      notifier.go           the poll loop: retry ladder, DLT, offset commit, drain
      purger.go             the inbox purge ticker
cmd/notifier/main.go
```

**Modified:**

```
internal/platform/messaging/message.go        + Partition, Offset, Attempt (§6.5)
internal/platform/kafka/config.go             + ConsumerConfig
internal/platform/kafka/client.go             + NewConsumer
internal/platform/kafkatest/kafkatest.go      + a records-produced helper for consumer tests
internal/platform/config/config.go            + LoadNotifier
deploy/docker-compose.yml                     + the notifier service
deploy/kafka-init.sh                          + the dead-letter topic
Makefile                                      + notifier targets
README.md                                     the consuming half, and what to watch
.env.example                                  the notifier's variables
docs/superpowers/specs/...-design.md          §11.3's sketch annotated with the four divergences
```

Two notes on the layout, both following from the convention `doc.go` records.

`inbox.go` holds `InboxRepository` alone. It has one method and one consumer, which by the convention argues for declaring it inside `process_user_registered.go` — but it is the mechanism the entire plan exists to demonstrate, and a reader looking for "where is the inbox" should find a file called `inbox.go`. The convention says ports live beside what they are about; what this port is about is the inbox, and it has no other file.

`consuming.go` holds the two error classes and the dead-letter port together because they are one idea from two directions: `ErrPermanent` is the classification, and `DeadLetterPublisher` is what happens to a record that earns it. Splitting them would put the question and its answer in different files.

---

## Task 1: The consumer-side fields of the shared envelope

**Files:**
- Modify: `internal/platform/messaging/message.go`
- Test: `internal/platform/messaging/message_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `messaging.Message` gains `Partition int32`, `Offset int64`, `Attempt int`. Every later task reads them.

This is the smallest task in the plan and the one with the most misleading size. `Attempt` is a field on the type two bounded contexts share, and it is the only field on it that is not part of the wire contract. That has to be written down where a reader will find it, or someone will one day persist it, publish it, or trust it for correctness.

- [ ] **Step 1: Write the failing test**

Add to `internal/platform/messaging/message_test.go`:

```go
// The consumer-side fields exist so a worker can identify a record without
// keeping a parallel map keyed by something it invented. Partition and Offset are
// the record's coordinates; Attempt is the worker's own count.
func TestMessage_CarriesTheRecordCoordinates(t *testing.T) {
	msg := Message{
		EventID:   "0199a4f0-0000-7000-8000-000000000001",
		Topic:     "accounts.user.v1",
		Partition: 3,
		Offset:    4711,
		Attempt:   2,
	}
	if msg.Partition != 3 || msg.Offset != 4711 {
		t.Errorf("coordinates = %d/%d, want 3/4711", msg.Partition, msg.Offset)
	}
	if msg.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", msg.Attempt)
	}
}

// A produced message has no coordinates. The zero values are what a producer
// leaves behind, and a consumer must be able to tell them apart from a real
// partition 0 — which it can, because a record it did not fetch has no Topic
// either. This test exists to pin the *absence* of a sentinel: there is no
// -1-means-unset convention here, and nothing should add one.
func TestMessage_AProducedMessageHasNoCoordinates(t *testing.T) {
	msg := Message{EventID: "x", Topic: "accounts.user.v1", Payload: []byte("{}")}
	if msg.Partition != 0 || msg.Offset != 0 || msg.Attempt != 0 {
		t.Errorf("produced message has coordinates: %d/%d/%d",
			msg.Partition, msg.Offset, msg.Attempt)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/platform/messaging/ -run TestMessage_Carries -v`
Expected: FAIL — `unknown field Partition in struct literal`.

- [ ] **Step 3: Add the fields and rewrite the paragraph that promised them**

In `internal/platform/messaging/message.go`, replace the final paragraph of the `Message` doc comment — the one beginning "The consumer-side fields of §6.5 … arrive with the consumer that reads them, in Plan 3" — with the text below, and add the three fields after `Topic`:

```go
// Partition and Offset are a fetched record's coordinates. They are set by the
// consumer adapter and are empty on a produced message, which is not a sentinel
// convention: partition 0 offset 0 is a real record, and the field that tells you
// whether these mean anything is Topic — a message the process did not fetch has
// none.
//
// Attempt is process-local, and it is the one field here that is not part of the
// wire contract. Kafka has no delivery counter: a record carries no "how many
// times have I been handed out" field and there is nowhere to put one without
// rewriting the record. The notifier keeps an in-memory count keyed by
// (topic, partition, offset), and that count resets on a rebalance or a restart.
//
// It is therefore a heuristic for "stop retrying this one and dead-letter it",
// and never a correctness input. The inbox is what makes reprocessing safe. Two
// consequences follow, and both are the reason this paragraph is long: Attempt
// must never be persisted, because a number that resets on restart would be a lie
// in a table; and it must never be published, because it describes this process's
// history with the record rather than anything about the event (spec §6.5, §12.2).
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
	Attempt       int
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/platform/messaging/ -v`
Expected: PASS, all of them. The relay's producer path constructs `Message` with field names, so nothing else changes.

- [ ] **Step 5: Prove nothing else moved**

Run: `make lint && go test ./...`
Expected: green. If a positional `Message{...}` literal exists anywhere it fails here; there should be none.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/messaging
git commit -m "feat(messaging): the consumer-side fields, and why one of them is not a contract"
```

---

## Task 2: The notifications domain — value objects

**Files:**
- Create: `internal/notifications/domain/recipient.go`, `user_ref.go`, `state.go`, `errors.go`
- Test: `internal/notifications/domain/recipient_test.go`, `user_ref_test.go`, `state_test.go`

**Interfaces:**
- Consumes: nothing. This package imports stdlib and `github.com/google/uuid` and nothing else, forever.
- Produces: `NewRecipient(string) (Recipient, error)`, `Recipient.String() string`; `NewUserRef(uuid.UUID) (UserRef, error)`, `UserRef.ID() uuid.UUID`, `UserRef.String() string`; `NotificationState` with `StatePending`/`StateSent`/`StateFailed`, `ParseState(string) (NotificationState, error)`, `NotificationState.String() string`, `NotificationState.canTransitionTo(NotificationState) bool`; the four sentinels in `errors.go`.

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
	ErrUnknownState     = errors.New("notifications: unknown notification state")

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

// String is the column form.
func (u UserRef) String() string { return u.id.String() }
```

Run again: PASS.

- [ ] **Step 8: Write the failing tests for `NotificationState`**

`internal/notifications/domain/state_test.go`:

```go
package domain

import (
	"errors"
	"testing"
)

func TestParseState_RoundTripsTheThreeLegalValues(t *testing.T) {
	for _, want := range []NotificationState{StatePending, StateSent, StateFailed} {
		got, err := ParseState(want.String())
		if err != nil {
			t.Fatalf("ParseState(%q): %v", want, err)
		}
		if got != want {
			t.Errorf("round trip = %v, want %v", got, want)
		}
	}
}

// A row read back from the database goes through ParseState, so an unknown value
// must be an error rather than a zero value. The CHECK constraint makes this
// unreachable through this application — and an unreachable branch that returns
// a valid-looking state is how a migration, a manual UPDATE or a future
// constraint change becomes a silent bug.
func TestParseState_RejectsAnythingElse(t *testing.T) {
	for _, s := range []string{"", "PENDING", "queued", "sent "} {
		if _, err := ParseState(s); !errors.Is(err, ErrUnknownState) {
			t.Errorf("ParseState(%q) err = %v, want ErrUnknownState", s, err)
		}
	}
}

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

Run: `go test ./internal/notifications/domain/ -run TestParseState -v` → FAIL, `undefined: NotificationState`. Then:

```go
package domain

// The three legal states of a notification (spec §8.2). They are a defined type
// rather than string constants so that a function taking a NotificationState
// cannot be handed "snet", and so that the transition rules have somewhere to
// live that is not a call site.
type NotificationState string

const (
	StatePending NotificationState = "pending"
	StateSent    NotificationState = "sent"
	StateFailed  NotificationState = "failed"
)

// ParseState converts a stored value back into the type, refusing anything the
// domain does not define.
//
// It is strict on purpose: no trimming, no case folding. A column that contains
// "Sent" was not written by this application, and guessing what it meant is how a
// data problem becomes a behaviour problem.
func ParseState(s string) (NotificationState, error) {
	switch state := NotificationState(s); state {
	case StatePending, StateSent, StateFailed:
		return state, nil
	default:
		return "", ErrUnknownState
	}
}

func (s NotificationState) String() string { return string(s) }

// canTransitionTo is the state machine. `sent` is terminal and `pending` is the
// only legal predecessor of either outcome (spec §8.2).
//
// Note what is absent: pending -> pending and sent -> sent are both illegal. A
// self-transition looks harmless and would make MarkSent idempotent, which is
// precisely the wrong shape here — the sender calling MarkSent twice means the
// gateway was called twice for one intent, and the aggregate refusing is how that
// surfaces instead of being absorbed. Idempotency in this design lives in the
// inbox and the Idempotency-Key, both of which are checked *before* any work
// happens; an aggregate that also shrugged at repeats would hide the failure of
// the mechanisms that are supposed to prevent them.
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
- Produces: `RequestWelcome(id uuid.UUID, to Recipient, user UserRef, sourceEventID uuid.UUID, now time.Time) (*Notification, error)`; accessors `ID()`, `UserID()`, `Recipient()`, `Kind()`, `State()`, `SourceEventID()`, `CreatedAt()`, `SentAt()`; transitions `MarkSent(at time.Time) error`, `MarkFailed(reason string) error`; `PullEvents() []Event`; the `Event` interface; `WelcomeEmailRequested`; `NotificationRepository`.

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
	if err := sent.MarkFailed("gateway 503"); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("MarkFailed after sent err = %v, want ErrIllegalTransition", err)
	}

	failed := newRequest(t)
	if err := failed.MarkFailed("gateway 503"); err != nil {
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
	if err := n.MarkFailed("first"); err != nil {
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
// reason is accepted and deliberately not stored. There is no column for it: the
// failure detail belongs to the send-intent row, whose last_error the sender
// writes with the full error text and an attempt count beside it. Storing a copy
// here would give an incident two accounts of the same event, which diverge the
// first time one of the two writes fails. The parameter stays because a
// transition that discards its reason at the call site reads like a bug, and
// because a future notifications table with a failure_reason column should be a
// change to this method's body and nothing else.
func (n *Notification) MarkFailed(reason string) error {
	if !n.state.canTransitionTo(StateFailed) {
		return ErrIllegalTransition
	}
	n.state = StateFailed
	return nil
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/notifications/domain/ -v`
Expected: PASS. `staticcheck` will flag the unused `reason` parameter under `-checks=all`; the doc comment above is the answer, and `_ = reason` is not — if the linter insists, name it `reason` and add `//lint:ignore SA4006` only as a last resort. Prefer keeping the parameter used by writing the reason into the returned error on the *illegal* path: `return fmt.Errorf("%w: cannot fail a %s notification (%s)", ErrIllegalTransition, n.state, reason)`. Do that; it is better than an ignore directive and it makes the log line more useful.

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
// operations. The consuming transaction only ever creates: a notification's later
// transitions are written by the sender, in its own transaction, against a row it
// claimed. A Save here would advertise an update path that this plan does not
// implement and that the next plan should not implement by accident.
//
// Insert returns ErrDuplicateSource when source_event_id is already present —
// the UNIQUE constraint of §8.2 translated back into the domain's vocabulary.
type NotificationRepository interface {
	Insert(ctx context.Context, n *Notification) error
}
```

Add `ErrDuplicateSource` to `errors.go`:

```go
	// ErrDuplicateSource is the source_event_id UNIQUE constraint, translated.
	// Reaching it means the inbox did not stop a duplicate — either a bug or a
	// purged inbox row (§12.3) — so it is worth its own sentinel rather than
	// arriving as an opaque pgx error: the application layer treats it as a
	// duplicate rather than a failure, and logs loudly that the second line of
	// defence was the one that held.
	ErrDuplicateSource = errors.New("notifications: a notification for this source event already exists")
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
- Produces: `ErrTransient`, `ErrPermanent`, `ErrNotForThisConsumer`; `Clock`, `IDGen`; `WelcomeRequest{Recipient, User, SourceEventID}`; `translate(msg messaging.Message) (WelcomeRequest, error)`.

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

	// ErrNotForThisConsumer is neither, and it is not in the spec: §12.2 has two
	// classes because it describes a topic with one event type on it.
	//
	// A topic is a stream of a context's published language, not a queue for one
	// consumer, and accounts.user.v1 will carry a second event type the day
	// accounts grows a second use case. This consumer wants user.registered.
	// Anything else is not an error at all — dead-lettering it would flood the DLT
	// on a deployment that has nothing to do with notifications, and retrying it
	// would stall the partition forever.
	//
	// So it is skipped and its offset is committed, and — deliberately — no inbox
	// row is written. An inbox row is a record that this consumer *processed* an
	// event; writing one for an event it declined to understand would be a lie
	// that survives a later deployment, so that the version of the notifier which
	// finally handles that type would skip every record the old one passed over.
	// Skipping is idempotent on its own; it needs no help.
	ErrNotForThisConsumer = errors.New("notifications: event type is not handled by this consumer")
)

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

// A different event type on the same topic is neither an error nor work. It must
// not be dead-lettered: accounts adding a second event type to its own topic is a
// deployment that has nothing to do with notifications, and a DLT full of records
// this consumer simply does not want is indistinguishable from a real incident.
func TestTranslate_AnotherEventTypeIsNotForThisConsumer(t *testing.T) {
	msg := validRecord(t, nil)
	msg.EventType = "com.outboxexpress.accounts.user.deactivated"
	_, err := translate(msg)
	if !errors.Is(err, ErrNotForThisConsumer) {
		t.Fatalf("err = %v, want ErrNotForThisConsumer", err)
	}
	// And explicitly not the other two, because the worker branches on all three.
	if errors.Is(err, ErrPermanent) || errors.Is(err, ErrTransient) {
		t.Errorf("err = %v; an unhandled type is not a failure of any kind", err)
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
// Every failure here is ErrPermanent, with one exception. A record on this topic
// is immutable — retrying a malformed one is a way of never noticing it, which is
// why §11.3 sends it to the dead-letter topic rather than into a retry loop. The
// exception is an event type this consumer does not handle, which is not a failure
// at all; see ErrNotForThisConsumer.
func translate(msg messaging.Message) (WelcomeRequest, error) {
	if msg.EventType != eventTypeUserRegistered {
		return WelcomeRequest{}, fmt.Errorf("%w: %q", ErrNotForThisConsumer, msg.EventType)
	}

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

- [ ] **Step 8: Assert the boundary with a grep, and make it a test**

Add to `translate_test.go`:

```go
// The anti-corruption layer is only a boundary if it is the *only* boundary. This
// asserts the property the whole file exists for: accounts' vocabulary appears in
// translate.go and nowhere else in the context.
//
// A grep in a test is unusual and this one earns it. The alternative is a comment
// asking future readers not to reach past the translator, and the entire argument
// of §11.3 is that a rule living in a call site is a rule someone forgets.
func TestTranslate_IsTheOnlyPlaceThatKnowsAccountsVocabulary(t *testing.T) {
	const root = "../.." // internal/
	forbidden := []string{"user_id", "display_name", "com.outboxexpress.accounts"}

	err := filepath.WalkDir(filepath.Join(root, "notifications"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".go" {
			return err
		}
		base := filepath.Base(path)
		// translate.go owns this vocabulary; its test necessarily contains a
		// sample of accounts' wire format.
		if base == "translate.go" || base == "translate_test.go" {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, word := range forbidden {
			if bytes.Contains(body, []byte(word)) {
				t.Errorf("%s contains %q; accounts' vocabulary belongs in translate.go alone", path, word)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
```

Run: `go test ./internal/notifications/application/ -run TestTranslate_IsTheOnly -v`
Expected: PASS.

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

const (
	// This context's identity on the wire. Constants rather than constructor
	// parameters: there is one notifications service and no configuration path
	// sets either value.
	source     = "/services/notifications"
	schemaBase = "https://schemas.outboxexpress.dev"
)

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
		data, schema, key, err := mapData(event)
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
			SchemaName:  schema.name,
			SchemaBase:  schemaBase,
			Version:     schema.version,
			Traceparent: meta.Traceparent,
		}, data)
		if err != nil {
			return nil, err
		}

		envelopes = append(envelopes, Envelope{
			EventID:        eventID,
			AggregateType:  event.AggregateType(),
			AggregateID:    aggregateID,
			EventType:      eventType,
			SchemaVersion:  schema.version,
			Payload:        payload,
			Headers:        h,
			OccurredAt:     occurred,
			IdempotencyKey: key,
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

// mapData is the context-specific part of the envelope: which domain event
// becomes which data shape, which schema describes it, and — unique to this
// context — what the idempotency key for the resulting external call is.
//
// The key is returned here rather than derived by the caller because it is a
// per-event-type decision. WelcomeEmailRequested's key is the source event id;
// a future event whose external effect is keyed differently would say so here,
// beside its own payload mapping, instead of in a switch somewhere else.
func mapData(event domain.Event) (any, messageSchema, string, error) {
	switch e := event.(type) {
	case domain.WelcomeEmailRequested:
		return welcomeEmailRequestedData{
			NotificationID: e.NotificationID.String(),
			UserID:         e.UserID.String(),
			Recipient:      e.Recipient.String(),
			SourceEventID:  e.SourceEventID.String(),
			RequestedAt:    e.RequestedAt.UTC().Format(messaging.TimeFormat),
		}, messageSchema{name: "notifications/welcome_email.requested", version: 1},
			e.SourceEventID.String(), nil
	default:
		return nil, messageSchema{}, "", fmt.Errorf("%w: %s", ErrUnmappedEvent, event.EventType())
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

Note: `user_id` appears in this file, which Task 4's grep test forbids outside `translate.go`. It is *this context's* `user_id` in *this context's* published schema, which is a genuine coincidence of naming and not a leak of accounts' vocabulary — but the test cannot tell the difference, and a test that cannot tell the difference is worse than no test. Resolve it by narrowing the forbidden list to `com.outboxexpress.accounts` and `display_name` — the two strings that are unambiguously accounts' — and by adding `envelope.go` nowhere: the assertion must stay a whole-context walk with a single exemption, or it stops being an assertion. Update Task 4's test when you reach this step, and note in its comment why `user_id` had to come off the list.

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
- Produces: `Metadata`, `metadataFrom(map[string]string) Metadata`, `Work{Notifications domain.NotificationRepository, Inbox InboxRepository}`, `UnitOfWork`; `InboxRepository.Record(ctx, consumer, eventID uuid.UUID, eventType string) (first bool, err error)`; `ProcessResult{Recorded, Duplicate, Ignored bool, NotificationID uuid.UUID}`; `NewProcessUserRegistered(uow, ids, clk, consumer string) *ProcessUserRegistered` and its `Execute(ctx, msg messaging.Message) (ProcessResult, error)`.

The use case is short and every line of it is load-bearing. §11.3's table of wrong shapes goes in its doc comment.

- [ ] **Step 1: Write `unit_of_work.go`**

```go
package application

import (
	"context"

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
// Do not add an outbox to this struct — the same prohibition accounts' Work
// carries, for the same reason. The send-intent row is drained structurally by the
// unit of work because the aggregate recorded an event; the moment a use case can
// append one by hand, "every state change emits its event" becomes a rule someone
// has to remember, with no failing test to notice.
//
// The inbox *is* here, and the difference is worth stating. An inbox row is not an
// event and is not drained from anything: it is a write the use case makes
// deliberately, first, as the thing that decides whether any other write happens.
// It has to be reachable for exactly the reason the outbox must not be.
type Work struct {
	Notifications domain.NotificationRepository
	Inbox         InboxRepository
}

// UnitOfWork is this context's transaction boundary, over this context's own pool.
// Everything fn does commits together or not at all — including the send-intent
// row in notifications.outbox, which fn never mentions (§5, §6.4, §11.3).
//
// It takes Metadata for the same reason accounts' does: the envelope factory needs
// the trace, and the only place that knows it is the caller.
type UnitOfWork interface {
	Do(ctx context.Context, meta Metadata, fn func(Work) error) error
}
```

- [ ] **Step 2: Write `inbox.go`**

```go
package application

import (
	"context"

	"github.com/google/uuid"
)

// InboxRepository is the idempotent consumer's memory (spec §12 mechanism 3,
// §11.3). It is an application port and not a domain repository, for the reason
// D5 gives: an inbox is not a domain concept. No notifications expert has an
// opinion about it; it exists because the transport delivers at least once.
//
// Record inserts and reports whether this consumer had seen the event before.
// first is true when the row was new — when this call is the one that claimed the
// work.
//
// Three things about that signature are deliberate.
//
// It reports rather than errors, because a duplicate is not a failure. §13.3: "a
// non-zero duplicate count is healthy, not an error." An error return would put
// the normal operation of an at-least-once transport on the same path as a broken
// database.
//
// It takes consumer, because several logical consumers may share this database and
// each must process an event independently (§8.2). This design has one; the
// parameter is what makes a second one a wiring change rather than a migration.
//
// And it must be called *first* in the transaction, before any other write. That
// ordering is the mechanism, not a preference: a duplicate then costs one round
// trip and touches nothing, and — the part that matters — the notification insert
// and the outbox drain are unreachable for a record already processed. An inbox
// checked after the work is an inbox that has already let the work happen twice.
type InboxRepository interface {
	Record(ctx context.Context, consumer string, eventID uuid.UUID, eventType string) (first bool, err error)
}
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
	uow := &fakeUOW{
		inbox:         &fakeInbox{first: true},
		notifications: &fakeNotifications{},
	}
	uc := NewProcessUserRegistered(
		uow,
		fixedIDs{next: uuid.MustParse("0199a4f0-0000-7000-8000-00000000000a")},
		fixedClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)},
		testConsumer,
	)
	return uc, uow
}

func TestProcessUserRegistered_WritesTheInboxRowThenTheNotification(t *testing.T) {
	uc, uow := newProcess(t)

	res, err := uc.Execute(context.Background(), validRecord(t, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Recorded || res.Duplicate || res.Ignored {
		t.Fatalf("result = %+v, want Recorded", res)
	}
	if !uow.committed {
		t.Error("the transaction did not commit")
	}

	// The order is the mechanism (§11.3): the inbox decides whether any other
	// write happens, so it has to be first. A test that only asserted both writes
	// occurred would pass against the shape that reprocesses.
	if want := []string{"inbox", "notification"}; !equal(uow.ops, want) {
		t.Errorf("ops = %v, want %v", uow.ops, want)
	}

	if got := uow.inbox.calls; len(got) != 1 || got[0].consumer != testConsumer {
		t.Errorf("inbox calls = %+v, want one for %q", got, testConsumer)
	}
	// The inbox key is the inbound event id, which is also what becomes the
	// gateway's Idempotency-Key. One identity, used by three mechanisms.
	if uow.inbox.calls[0].eventID.String() != "0199a4f0-0000-7000-8000-00000000000c" {
		t.Errorf("inbox event id = %v", uow.inbox.calls[0].eventID)
	}
}

// The notification the transaction inserts carries an event, and the unit of work
// is what turns that event into the send-intent row. The use case never mentions
// an outbox, and this test asserts that the *aggregate* is the thing that carries
// the intent out — which is what makes the guarantee structural.
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

// The headline case. A duplicate short-circuits before any work: no notification,
// and — the assertion that matters — nothing for the unit of work to drain, so no
// second send-intent. "Already processed" that still emitted an intent would be a
// second email with an inbox that thinks it did its job.
func TestProcessUserRegistered_ADuplicateDoesNoWork(t *testing.T) {
	uc, uow := newProcess(t)
	uow.inbox.first = false

	res, err := uc.Execute(context.Background(), validRecord(t, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Duplicate || res.Recorded {
		t.Fatalf("result = %+v, want Duplicate", res)
	}
	// It still commits. The inbox insert is a real write even when it conflicts —
	// and more importantly, rolling back here would leave the offset uncommitted
	// and the record redelivered forever.
	if !uow.committed {
		t.Error("a duplicate rolled back; it must commit so the offset can advance")
	}
	if len(uow.notifications.inserted) != 0 {
		t.Errorf("a duplicate inserted %d notifications, want 0", len(uow.notifications.inserted))
	}
	if want := []string{"inbox"}; !equal(uow.ops, want) {
		t.Errorf("ops = %v, want %v — a duplicate must touch nothing else", uow.ops, want)
	}
}

// The database's second line of defence, reached only if the inbox failed to stop
// a duplicate — a bug, or an inbox row purged while the event was still on the
// topic (§12.3). It is reported as a duplicate, not as a failure, because the
// outcome is the one we wanted; the worker logs it loudly.
func TestProcessUserRegistered_ADuplicateSourceIsAbsorbed(t *testing.T) {
	uc, uow := newProcess(t)
	uow.notifications.err = domain.ErrDuplicateSource

	res, err := uc.Execute(context.Background(), validRecord(t, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Duplicate || !res.ConstraintCaught {
		t.Fatalf("result = %+v, want Duplicate with ConstraintCaught", res)
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
	// processed must not cost a BEGIN, and — more to the point — must not be able
	// to leave a transaction open on the way to the dead-letter topic.
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
	if !res.Ignored || res.Recorded || res.Duplicate {
		t.Fatalf("result = %+v, want Ignored", res)
	}
	// No transaction, and specifically no inbox row: see ErrNotForThisConsumer.
	// An inbox row here would make every one of these records invisible to the
	// future notifier that finally handles the type.
	if uow.calls != 0 {
		t.Errorf("opened %d transactions for an event it does not handle", uow.calls)
	}
}

// A failed id generation is transient, not permanent. The record is fine; the
// machine is not. Dead-lettering a valid registration because the entropy pool
// hiccuped would be a permanent decision about a temporary condition.
func TestProcessUserRegistered_AFailedIDIsTransient(t *testing.T) {
	uow := &fakeUOW{inbox: &fakeInbox{first: true}, notifications: &fakeNotifications{}}
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

// A database failure is returned unclassified-as-transient and the transaction
// does not commit, so the worker declines to commit the offset and Kafka
// redelivers (§12.2 rung 2). The important half is what did *not* happen: no
// offset commit, which is the only thing standing between a transient failure and
// silent loss.
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

// The trace survives the hop. This is the assertion that keeps §9.2's promise
// meaningful across two databases: the metadata handed to the transaction is the
// metadata that came off the record, and the unit of work is what puts it on the
// outgoing envelope.
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
// fakeUOW runs fn immediately and records the write order, because in this use
// case the order *is* the mechanism.
type fakeUOW struct {
	calls         int
	committed     bool
	lastMeta      Metadata
	ops           []string
	inbox         *fakeInbox
	notifications *fakeNotifications
	commitErr     error
}

func (f *fakeUOW) Do(ctx context.Context, meta Metadata, fn func(Work) error) error {
	f.calls++
	f.lastMeta = meta
	f.inbox.record = func() { f.ops = append(f.ops, "inbox") }
	f.notifications.record = func() { f.ops = append(f.ops, "notification") }
	if err := fn(Work{Notifications: f.notifications, Inbox: f.inbox}); err != nil {
		return err
	}
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = true
	return nil
}

type inboxCall struct {
	consumer  string
	eventID   uuid.UUID
	eventType string
}

type fakeInbox struct {
	first  bool
	err    error
	calls  []inboxCall
	record func()
}

func (f *fakeInbox) Record(_ context.Context, consumer string, eventID uuid.UUID, eventType string) (bool, error) {
	if f.record != nil {
		f.record()
	}
	f.calls = append(f.calls, inboxCall{consumer: consumer, eventID: eventID, eventType: eventType})
	if f.err != nil {
		return false, f.err
	}
	return f.first, nil
}

type fakeNotifications struct {
	inserted []*domain.Notification
	err      error
	record   func()
}

func (f *fakeNotifications) Insert(_ context.Context, n *domain.Notification) error {
	if f.record != nil {
		f.record()
	}
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, n)
	return nil
}

func equal(got, want []string) bool {
	return slices.Equal(got, want)
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
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/notifications/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// ProcessResult is what one record's processing did. Every field on it becomes a
// field on the worker's log line, because a use case does not log (§6.1, §13.3) —
// and because a number that reaches a dashboard through a result struct is a
// number a unit test can assert.
type ProcessResult struct {
	// Recorded: the inbox row was new and the work was done.
	Recorded bool

	// Duplicate: this consumer had already processed the event, so nothing
	// happened. §13.3: "a non-zero duplicate count is healthy, not an error." It
	// is the visible evidence that the inbox is doing its job.
	Duplicate bool

	// ConstraintCaught is set when the duplicate was caught by
	// notifications.source_event_id UNIQUE rather than by the inbox. Both outcomes
	// are correct, but they are not equally interesting: this one means the first
	// line of defence did not hold, which is either a bug or an inbox row purged
	// while its event was still on the topic (§12.3). The worker logs it at WARN.
	ConstraintCaught bool

	// Ignored: an event type this consumer does not handle. Not work, not a
	// failure, and not an inbox row (see ErrNotForThisConsumer).
	Ignored bool

	// NotificationID is set when Recorded, for the log line. It is the row a human
	// asked "did we send it?" would look up.
	NotificationID uuid.UUID
}

// ProcessUserRegistered is the consuming transaction, and the whole reason the
// notifications context is a separate service with a separate database.
//
// One transaction, three writes: the inbox row, the notification row, and — drained
// structurally by the same unit of work, never mentioned here — the send-intent row
// in notifications.outbox. Then, and only then, the worker commits the offset.
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
	// The anti-corruption layer. Accounts' published language is translated into
	// this context's vocabulary here and nowhere else; no code below this line has
	// ever seen what accounts calls its fields (§11.3).
	req, err := translate(msg)
	switch {
	case errors.Is(err, ErrNotForThisConsumer):
		// Not work, not a failure, and no inbox row: the offset advances and this
		// process forgets the record, which is the only outcome that leaves a
		// future notifier free to handle the type properly.
		return ProcessResult{Ignored: true}, nil
	case err != nil:
		return ProcessResult{}, err // already ErrPermanent -> the dead-letter topic
	}

	// Everything nondeterministic happens before the transaction opens, so that a
	// BEGIN is never held open across a failure that has nothing to do with the
	// database.
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

	res := ProcessResult{}
	err = uc.uow.Do(ctx, metadataFrom(msg.Headers), func(w Work) error {
		// First, before any other write. A duplicate then costs one round trip and
		// touches nothing, and the two writes below are unreachable for a record
		// this consumer has already processed.
		first, err := w.Inbox.Record(ctx, uc.consumer, req.SourceEventID, msg.EventType)
		if err != nil {
			return err
		}
		if !first {
			res.Duplicate = true
			return nil // already processed; no work, and the transaction still commits
		}

		// Inserting the aggregate is the only write this use case makes. The
		// send-intent row is the unit of work's business, drained from the events
		// the aggregate recorded — which is why there is no outbox in Work and no
		// mention of one here (§5, §6.4).
		if err := w.Notifications.Insert(ctx, notif); err != nil {
			// The UNIQUE on source_event_id, reached only if the inbox did not
			// stop a duplicate. The outcome is the one we wanted, so it is not a
			// failure — but it is worth distinguishing, because it means the first
			// mechanism did not hold (§8.2, §12.3).
			if errors.Is(err, domain.ErrDuplicateSource) {
				res.Duplicate = true
				res.ConstraintCaught = true
				return nil
			}
			return err
		}
		res.Recorded = true
		res.NotificationID = notif.ID()
		return nil
	})
	if err != nil {
		// Unclassified, and left that way. The worker treats an unclassified error
		// as transient and declines to commit the offset, so Kafka redelivers —
		// which is the safe default in this direction (see consuming.go).
		return ProcessResult{}, err
	}
	return res, nil
}
```

Note on the `ConstraintCaught` path: it returns `nil` from inside the transaction, and that is a real subtlety. In PostgreSQL a unique violation aborts the transaction, so the *implementation* of this must not swallow the error and continue writing — it does not, because there is nothing after it, and the commit of an aborted transaction returns `ErrTxCommitRollback` which surfaces as an error from `Do`. Task 8's integration test asserts the real behaviour against a real database; if it turns out that a `ConstraintCaught` result cannot be produced without a savepoint, the correct resolution is to report it as a transient error rather than to add a savepoint — the inbox is the mechanism, this is the backstop, and a backstop that costs a savepoint on every insert is not worth it. Record whichever way it lands in the plan's post-implementation notes.

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/notifications/application/ -v`
Expected: PASS. Add `slices` to `fakes_test.go`'s imports.

- [ ] **Step 7: Prove the duplicate test has teeth**

Temporarily move the `w.Inbox.Record` call *after* `w.Notifications.Insert`. Run `TestProcessUserRegistered_ADuplicateDoesNoWork` — it must fail on the `ops` assertion. Restore.

This is the one test in the plan whose failure mode is silent: with the writes in the wrong order every other assertion still passes, because a duplicate would still be reported as a duplicate. It would just have inserted a notification first.

- [ ] **Step 8: Commit**

```bash
git add internal/notifications/application
git commit -m "feat(notifications): the consuming transaction, inbox first"
```

---

## Task 7: The persistence layer

**Files:**
- Create: `internal/notifications/infrastructure/postgres/uow.go`, `tracker.go`, `inbox_repo.go`, `notification_repo.go`, `outbox_repo.go`
- Test: `uow_integration_test.go`, `inbox_repo_integration_test.go`, `notification_repo_integration_test.go`, `tracker_test.go`

**Interfaces:**
- Consumes: `internal/notifications/application` (Tasks 4–6), `internal/notifications/domain`, `platformpg.WithTx`, `platformpg.Queryer`, `pgtest.Notifications`.
- Produces: `NewUnitOfWork(pool *pgxpool.Pool, envelopes application.EnvelopeFactory) *UnitOfWork`; `NewInboxRepository(q platformpg.Queryer) *InboxRepository`; `newNotificationRepository(q platformpg.Queryer, tr *tracker) *NotificationRepository`; `NewOutboxRepository(q platformpg.Queryer) *OutboxRepository`.

Structurally accounts' `uow.go` with a second repository in the work set and a second table drained into. The one genuinely new piece of SQL in the plan is the inbox's, and it is three lines that decide whether the whole design works.

- [ ] **Step 1: Write the failing integration test for the inbox**

`internal/notifications/infrastructure/postgres/inbox_repo_integration_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

// The mechanism, against a real database. ON CONFLICT DO NOTHING reports zero
// rows for a repeat, which is what "not first" means — and it means it under
// concurrency, which no in-memory check can (§12 mechanism 3).
func TestInboxRepository_RecordIsFirstExactlyOnce(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	pgtest.Truncate(t, pool, "notifications.inbox")
	ctx := context.Background()
	eventID := uuid.New()

	repo := NewInboxRepository(pool)

	first, err := repo.Record(ctx, "notifications.welcome-email", eventID, "com.example.thing")
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if !first {
		t.Fatal("the first Record reported not-first")
	}

	again, err := repo.Record(ctx, "notifications.welcome-email", eventID, "com.example.thing")
	if err != nil {
		t.Fatalf("second Record: %v", err)
	}
	if again {
		t.Error("the second Record reported first; the inbox is not deduplicating")
	}
}

// The consumer column is part of the key, so two logical consumers process the
// same event independently (§8.2). This design has one consumer; the test is what
// keeps the column meaningful for the second.
func TestInboxRepository_TwoConsumersEachGetTheEventOnce(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	pgtest.Truncate(t, pool, "notifications.inbox")
	ctx := context.Background()
	eventID := uuid.New()
	repo := NewInboxRepository(pool)

	for _, consumer := range []string{"notifications.welcome-email", "notifications.audit"} {
		first, err := repo.Record(ctx, consumer, eventID, "com.example.thing")
		if err != nil {
			t.Fatalf("Record(%s): %v", consumer, err)
		}
		if !first {
			t.Errorf("%s did not get the event", consumer)
		}
	}
}

// Two concurrent Records for the same key: exactly one first, and no error. This
// is the property the primary key provides and a SELECT-then-INSERT would not —
// two notifiers in a rebalance window can be processing the same record, and the
// loser must learn that it lost rather than fail.
func TestInboxRepository_ConcurrentRecordsElectOneWinner(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	pgtest.Truncate(t, pool, "notifications.inbox")
	ctx := context.Background()
	eventID := uuid.New()
	repo := NewInboxRepository(pool)

	const racers = 8
	results := make(chan bool, racers)
	errs := make(chan error, racers)
	start := make(chan struct{})
	for range racers {
		go func() {
			<-start
			first, err := repo.Record(ctx, "notifications.welcome-email", eventID, "com.example.thing")
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
			t.Fatalf("Record: %v", err)
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
Expected: FAIL — `undefined: NewInboxRepository`.

- [ ] **Step 3: Write `inbox_repo.go`**

```go
// Package postgres is the notifications context's persistence layer: the inbox,
// the Notification repository, the consumer's own outbox, and the unit of work
// that makes the send-intent insert structural.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/google/uuid"

	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// The idempotent consumer, in three lines of SQL.
//
// ON CONFLICT DO NOTHING with a RETURNING clause is the whole mechanism: the
// statement inserts and reports a row when the event is new, and reports nothing
// when the primary key already holds it. It is one round trip, it is atomic, and
// it works under concurrency — which a SELECT followed by an INSERT does not, and
// the difference shows up exactly during a consumer-group rebalance, when two
// members can briefly be processing the same record (§12).
//
// processed_at takes its column default, which is now() — and in PostgreSQL now()
// is transaction_timestamp(), so every row a transaction writes shares one
// instant. That is the right timestamp here: the inbox records when the consuming
// transaction happened, not when this statement within it did.
const recordInbox = `
INSERT INTO notifications.inbox (consumer, event_id, event_type)
VALUES ($1, $2, $3)
ON CONFLICT (consumer, event_id) DO NOTHING
RETURNING processed_at`

type InboxRepository struct {
	q platformpg.Queryer
}

func NewInboxRepository(q platformpg.Queryer) *InboxRepository {
	return &InboxRepository{q: q}
}

// Record inserts and reports whether the row was new.
//
// pgx.ErrNoRows is the *success* path for a duplicate, which is why it is matched
// explicitly and returned as (false, nil): a repeat is normal operation of an
// at-least-once transport, not a failure (§13.3).
func (r *InboxRepository) Record(
	ctx context.Context, consumer string, eventID uuid.UUID, eventType string,
) (bool, error) {
	var processedAt time.Time
	err := r.q.QueryRow(ctx, recordInbox, consumer, eventID, eventType).Scan(&processedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("postgres: record inbox (%s, %s): %w", consumer, eventID, err)
	default:
		return true, nil
	}
}

var _ application.InboxRepository = (*InboxRepository)(nil)
```

Add `time` and the `application` import; the scan target is discarded but `RETURNING` needs one, and scanning the real timestamp rather than a `struct{}` keeps the statement honest about what it returns.

- [ ] **Step 4: Run it and watch it pass**

Run: `go test -tags=integration ./internal/notifications/infrastructure/postgres/ -run TestInboxRepository -v`
Expected: PASS, all three.

- [ ] **Step 5: Write `tracker.go` and its unit test**

Copy `internal/accounts/infrastructure/postgres/tracker.go` and its test, substituting `*domain.Notification` for `*domain.User`. The comment explaining why the tracker exists — a repository records which aggregates it persisted, so the unit of work can drain their events without the use case asking — carries over unchanged. Adjust it to name this context's aggregate and add one sentence: the tracker is per-context for the same reason the recorder is, and the two contexts' trackers hold different aggregate types, so there is nothing to share even if sharing were desirable.

- [ ] **Step 6: Write `notification_repo.go`**

```go
package postgres

const insertNotification = `
INSERT INTO notifications.notifications
    (id, user_id, recipient, kind, state, source_event_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`

// NotificationRepository writes the aggregate. It is unexported-constructed —
// newNotificationRepository — because the only legitimate caller is this package's
// unit of work, which binds it to a transaction and to a tracker. An exported
// constructor would let a process wire one against the pool, and a notification
// written outside the consuming transaction is the dual write this design exists
// to prevent.
type NotificationRepository struct {
	q  platformpg.Queryer
	tr *tracker
}

func newNotificationRepository(q platformpg.Queryer, tr *tracker) *NotificationRepository {
	return &NotificationRepository{q: q, tr: tr}
}

// Insert writes the row and tells the tracker, which is what makes the
// send-intent structural: the unit of work drains this aggregate's events on the
// way to commit, and the use case never mentions an outbox.
//
// sent_at is not written. It is NULL until the sender sets it, and passing the
// aggregate's zero time would write 0001-01-01 into a nullable column — a value
// that reads as "sent, a very long time ago" to every query that checks the
// column rather than the state.
func (r *NotificationRepository) Insert(ctx context.Context, n *domain.Notification) error {
	_, err := r.q.Exec(ctx, insertNotification,
		n.ID(), n.UserID(), n.Recipient().String(), n.Kind(),
		n.State().String(), n.SourceEventID(), n.CreatedAt(),
	)
	if err != nil {
		// The UNIQUE on source_event_id, translated into the domain's vocabulary
		// so that the application layer can absorb it as a duplicate rather than
		// matching on a driver error (§8.2). Same pattern as accounts'
		// ErrEmailTaken: the rule is the domain's, the enforcement is the
		// database's, and the translation happens here.
		if isUniqueViolation(err, "notifications_source_event_id_key") {
			return domain.ErrDuplicateSource
		}
		return fmt.Errorf("postgres: insert notification %s: %w", n.ID(), err)
	}
	r.tr.track(n)
	return nil
}
```

`isUniqueViolation(err error, constraint string) bool` unwraps to `*pgconn.PgError` and compares `Code == "23505"` and `ConstraintName`. Accounts' `user_repo.go` already has this shape for `ErrEmailTaken` — read it and follow it; if it is written inline there, factor neither, because a two-line helper duplicated across two contexts is cheaper than a shared one (the same trade the recorder makes). Confirm the constraint name with `\d notifications.notifications` rather than guessing it: PostgreSQL derives it from the table and column, and a mistyped name would make this branch dead code that silently reports every insert failure as a driver error.

- [ ] **Step 7: Write `outbox_repo.go`**

Accounts' `Append`, with `idempotency_key` added to the column list and `e.IdempotencyKey` to the batch. Keep the `pgx.Batch` and the comment about why: these inserts sit inside the consuming transaction, and a transaction open for N round trips holds its locks N times longer (§11.1 rule 4). Keep the leading comment about `status`, `attempts` and `available_at` taking their defaults, adjusted to name the sender as the owner of that bookkeeping.

```go
const insertOutbox = `
INSERT INTO notifications.outbox
    (event_id, aggregate_type, aggregate_id, event_type, schema_version,
     payload, headers, occurred_at, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
```

- [ ] **Step 8: Write `uow.go`**

Accounts' `uow.go`, with two changes. The work set has two repositories; and the appender is this context's.

```go
// outboxAppender is the append role, declared at its consumer. It is not in the
// application package because no use case mentions an outbox — that is the whole
// point of §5, and an interface exported from application that nothing in
// application uses invites exactly the reach-in that Work's comment warns about.
type outboxAppender interface {
	Append(ctx context.Context, envelopes []application.Envelope) error
}

type UnitOfWork struct {
	pool      *pgxpool.Pool
	envelopes application.EnvelopeFactory

	// outbox builds the appender for a transaction. A field rather than a direct
	// call so that a test can substitute an appender that fails: "the send-intent
	// insert failed, so the notification must not exist" is the dual-write refusal
	// in its purest form, and there is no way to provoke it from outside — the
	// factory mints a fresh UUIDv7 per event, so not even the event_id constraint
	// can be made to fire.
	outbox func(platformpg.Queryer) outboxAppender
}

func (u *UnitOfWork) Do(ctx context.Context, meta application.Metadata, fn func(application.Work) error) error {
	return platformpg.WithTx(ctx, u.pool, func(tx pgx.Tx) error {
		tracker := newTracker()

		// Both repositories are bound to tx and handed to fn as the only way to
		// reach persistence. The pool is not reachable from here, so there is no
		// path to an untransacted write.
		work := application.Work{
			Notifications: newNotificationRepository(tx, tracker),
			Inbox:         NewInboxRepository(tx),
		}
		if err := fn(work); err != nil {
			return err
		}

		// Drain what the repositories touched. Nothing external happens in here:
		// no HTTP, no produce, no email, and no offset commit (§11.1 rule 2).
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
```

- [ ] **Step 9: Write the failing integration tests for the unit of work**

`internal/notifications/infrastructure/postgres/uow_integration_test.go`:

```go
//go:build integration

package postgres

// The headline assertion of the whole plan, against a real database: process one
// record and find three rows, in three tables, from one transaction — and the use
// case asked for two of them.
func TestUnitOfWork_OneRecordWritesInboxNotificationAndIntent(t *testing.T) { /* ... */ }

// The refusal. An appender that fails must leave neither the inbox row nor the
// notification behind: "the send-intent insert failed, so the notification must
// not exist." This is the dual-write problem, tested from the consuming side.
func TestUnitOfWork_AFailedIntentInsertLeavesNothing(t *testing.T) { /* ... */ }

// The duplicate path, end to end through the transaction: the same event id twice
// leaves one inbox row, one notification, and — the assertion that matters — one
// send-intent. A second intent would be a second email.
func TestUnitOfWork_ADuplicateLeavesTheOutboxUntouched(t *testing.T) { /* ... */ }

// The intent row carries the inbound event id as its idempotency_key, which is
// what makes §13's two remedies compose. Read the column, not the payload.
func TestUnitOfWork_TheIntentCarriesTheInboundEventID(t *testing.T) { /* ... */ }

// The trace survives into the stored headers, so a registration and the email it
// causes share a traceparent across two databases.
func TestUnitOfWork_StoresTheTraceOnTheIntent(t *testing.T) { /* ... */ }
```

Write each body following `internal/accounts/infrastructure/postgres/uow_integration_test.go`: `pgtest.Notifications(t)`, `pgtest.Truncate(t, pool, "notifications.inbox", "notifications.notifications", "notifications.outbox")`, construct the real `UnitOfWork` with the real `CloudEventFactory` over `ids.System{}` (or the platform ids package's exported generator), run `ProcessUserRegistered.Execute` against it with a fixture record, then assert with `QueryRow` counts and column reads. For the failure case, replace `u.outbox` with a function returning a stub whose `Append` returns an error — that field exists for this test.

Add a `pgtest.CountNotifications(t, pool) (inbox, notifications, outbox int)` helper beside the existing `CountAccounts`, so the assertions read as three numbers rather than three queries.

- [ ] **Step 10: Run them**

Run: `go test -tags=integration ./internal/notifications/... -v`
Expected: PASS. Then `make lint && go test ./...` for the unit suite.

- [ ] **Step 11: Prove the refusal test has teeth**

In `uow.go`, temporarily move the `tracker.drain()` block *before* `fn(work)` so that it drains nothing and the append never runs. `TestUnitOfWork_AFailedIntentInsertLeavesNothing` must fail — if it passes, it is asserting the absence of rows that were never written for an unrelated reason. Restore.

- [ ] **Step 12: Commit**

```bash
git add internal/notifications/infrastructure internal/platform/pgtest
git commit -m "feat(notifications): three writes, one transaction, and the use case asked for two"
```

---

## Task 8: The consumer client

**Files:**
- Modify: `internal/platform/kafka/config.go`, `internal/platform/kafka/client.go`, `internal/platform/kafkatest/kafkatest.go`
- Test: `internal/platform/kafka/config_test.go`, `client_integration_test.go`

**Interfaces:**
- Produces: `ConsumerConfig{Brokers []string, Topics []string, GroupID string, OnRevoked func(context.Context, *kgo.Client, map[string][]int32)}`, `DefaultConsumerConfig(brokers []string, topics []string, group string) ConsumerConfig`, `NewConsumer(cfg ConsumerConfig) (*kgo.Client, error)`; `kafkatest.ConsumerGroup(t, brokers, topic) string`.

Six options, each of which is a decision from §10.2. The config type exists so that they are set in one place and asserted in a test, rather than spread across `cmd/notifier`'s wiring where a future edit can quietly drop one.

- [ ] **Step 1: Write the failing test**

`internal/platform/kafka/config_test.go` additions:

```go
// Spec §10.2's option list, asserted. The two that matter most are the ones whose
// absence is invisible until it costs a message.
func TestDefaultConsumerConfig_MatchesTheSpec(t *testing.T) {
	cfg := DefaultConsumerConfig([]string{"localhost:9092"},
		[]string{"accounts.user.v1"}, "notifications.welcome-email")

	if cfg.GroupID != "notifications.welcome-email" {
		t.Errorf("GroupID = %q", cfg.GroupID)
	}
	if err := cfg.validate(); err != nil {
		t.Errorf("the default config does not validate: %v", err)
	}
}

func TestConsumerConfig_ValidateRefusesAnIncompleteConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  ConsumerConfig
	}{
		{name: "no brokers", cfg: ConsumerConfig{Topics: []string{"t"}, GroupID: "g"}},
		{name: "no topics", cfg: ConsumerConfig{Brokers: []string{"b:9092"}, GroupID: "g"}},
		// A consumer with no group id is not a consumer group member: franz-go
		// would happily consume without one, committing nothing and rejoining
		// nothing, and the notifier would look like it worked until it restarted
		// and reprocessed from the beginning.
		{name: "no group", cfg: ConsumerConfig{Brokers: []string{"b:9092"}, Topics: []string{"t"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.validate(); err == nil {
				t.Error("validate accepted it")
			}
		})
	}
}
```

And an integration test in `client_integration_test.go`:

```go
//go:build integration

// The properties that cannot be asserted from a struct: that the client joins the
// group, fetches from the beginning, and commits nothing on its own.
//
// The last one is the important one and the hardest to see. Produce three records,
// consume them without committing, close the client, and open a second one in the
// same group: it must receive all three again. An auto-commit ticker anywhere in
// the configuration turns this test flaky first and lossy later.
func TestNewConsumer_CommitsNothingOnItsOwn(t *testing.T) { /* ... */ }

func TestNewConsumer_StartsAtTheBeginning(t *testing.T) { /* ... */ }
```

- [ ] **Step 2: Run, fail, implement**

Add to `internal/platform/kafka/config.go`:

```go
// ConsumerConfig is spec §10.2's option list as a value, so that the six
// decisions it encodes live in one place and are asserted in one test rather than
// spread through a process's wiring.
type ConsumerConfig struct {
	Brokers []string
	Topics  []string

	// GroupID is the consumer group. It is required and has no default: a
	// consumer without a group commits nothing and rejoins nothing, which looks
	// like it works until the first restart reprocesses the topic.
	GroupID string

	// OnRevoked runs when the group takes partitions away. §10.2: on revocation
	// the notifier stops processing, does not commit offsets for records it has
	// not finished, and lets the new owner redeliver them — the inbox absorbs the
	// overlap. It is a field rather than a fixed callback because the behaviour is
	// the worker's, and this package must not know what a notifier is.
	OnRevoked func(context.Context, *kgo.Client, map[string][]int32)
}

func DefaultConsumerConfig(brokers, topics []string, group string) ConsumerConfig {
	return ConsumerConfig{Brokers: brokers, Topics: topics, GroupID: group}
}

func (c ConsumerConfig) validate() error { /* joined errors, naming each field */ }
```

And to `client.go`:

```go
// NewConsumer builds the consumer group member of spec §10.2. Every option below
// is a decision, and three of them are the difference between at-least-once and
// something worse.
func NewConsumer(cfg ConsumerConfig) (*kgo.Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),

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

		// auto.offset.reset=earliest. A new group must see the whole topic: this
		// is a demo whose entire subject is that nothing is lost, and a consumer
		// that started at the end would lose everything produced before it first
		// ran.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if cfg.OnRevoked != nil {
		opts = append(opts, kgo.OnPartitionsRevoked(cfg.OnRevoked))
	}
	return kgo.NewClient(opts...)
}
```

- [ ] **Step 3: Add the test helper**

`kafkatest.ConsumerGroup(t, brokers, topic) string` returns a unique group name per test (the existing `topicName(t)` shows the pattern) so that two integration tests in the same package do not share a group's committed offsets. Add it to `kafkatest` and nowhere else.

- [ ] **Step 4: Run everything**

Run: `make lint && go test ./internal/platform/kafka/ -v && go test -tags=integration ./internal/platform/kafka/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/kafka internal/platform/kafkatest
git commit -m "feat(kafka): the consumer group member, and the three options that make it at-least-once"
```

---

## Task 9: The Kafka adapters

**Files:**
- Create: `internal/notifications/infrastructure/kafka/consumer.go`, `dlt.go`
- Test: `consumer_test.go`, `dlt_test.go`, `dlt_integration_test.go`

**Interfaces:**
- Produces: `RecordToMessage(rec *kgo.Record) messaging.Message`; `NewDeadLetterPublisher(client *kgo.Client, topic string) *DeadLetterPublisher` implementing `application.DeadLetterPublisher`.

Two small adapters. The interesting one is the dead-letter publisher, because §12.2 specifies exactly what a DLT record must carry and the reason is recoverability: a record in the DLT has to be replayable onto the source topic without reconstruction.

- [ ] **Step 1: Write the failing test for the record mapping**

`internal/notifications/infrastructure/kafka/consumer_test.go`:

```go
// The mapping is the inbound half of the relay's publisher, and the fields it
// reads are the ones §9.2 promises a consumer can route and deduplicate on
// without deserialising the payload.
func TestRecordToMessage_ReadsTheRoutingHeaders(t *testing.T) {
	rec := &kgo.Record{
		Topic:     "accounts.user.v1",
		Partition: 2,
		Offset:    17,
		Key:       []byte("user-9f3c"),
		Value:     []byte(`{"specversion":"1.0"}`),
		Headers: []kgo.RecordHeader{
			{Key: messaging.HeaderEventID, Value: []byte("0199a4f0-0000-7000-8000-00000000000c")},
			{Key: messaging.HeaderEventType, Value: []byte("com.outboxexpress.accounts.user.registered")},
			{Key: messaging.HeaderSchemaVersion, Value: []byte("1")},
			{Key: messaging.HeaderCorrelationID, Value: []byte("corr-1")},
		},
	}

	msg := RecordToMessage(rec)

	if msg.EventID != "0199a4f0-0000-7000-8000-00000000000c" {
		t.Errorf("EventID = %q", msg.EventID)
	}
	if msg.EventType != "com.outboxexpress.accounts.user.registered" {
		t.Errorf("EventType = %q", msg.EventType)
	}
	if msg.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", msg.SchemaVersion)
	}
	if msg.Partition != 2 || msg.Offset != 17 {
		t.Errorf("coordinates = %d/%d, want 2/17", msg.Partition, msg.Offset)
	}
	if msg.Key != "user-9f3c" {
		t.Errorf("Key = %q", msg.Key)
	}
	if msg.Headers[messaging.HeaderCorrelationID] != "corr-1" {
		t.Errorf("headers = %v", msg.Headers)
	}
}

// A record with no headers at all must map to a Message rather than panic. The
// translator will reject it as permanent and the DLT will carry it, which is a
// far better outcome than a crash loop on one malformed record — and a record
// produced by something that is not our relay is exactly the case worth
// surviving.
func TestRecordToMessage_SurvivesAHeaderlessRecord(t *testing.T) {
	msg := RecordToMessage(&kgo.Record{Topic: "accounts.user.v1", Value: []byte("{}")})
	if msg.Topic != "accounts.user.v1" {
		t.Errorf("Topic = %q", msg.Topic)
	}
	if msg.EventID != "" || msg.SchemaVersion != 0 {
		t.Errorf("invented values from an empty header set: %+v", msg)
	}
}

// An unparseable schema_version is zero rather than an error. The field is
// informational for this consumer — nothing branches on it — and failing the whole
// record over it would dead-letter a message whose payload is perfectly good.
func TestRecordToMessage_AnUnparseableSchemaVersionIsZero(t *testing.T) {
	rec := &kgo.Record{Topic: "t", Headers: []kgo.RecordHeader{
		{Key: messaging.HeaderSchemaVersion, Value: []byte("one")},
	}}
	if got := RecordToMessage(rec).SchemaVersion; got != 0 {
		t.Errorf("SchemaVersion = %d, want 0", got)
	}
}
```

- [ ] **Step 2: Run, fail, write `consumer.go`**

```go
// Package kafka is the notifications context's transport adapter: it turns Kafka
// records into the shared transport Message, and dead-letters the ones that will
// never succeed.
//
// It does not decide *when* to consume. The loop, the retry ladder and the offset
// commit are the worker's, in presentation (spec §6.3): deciding when to run a use
// case is a presentation concern, and an adapter that owned the loop would put
// business sequencing in infrastructure.
package kafka

// RecordToMessage maps a fetched record onto the transport envelope.
//
// It reads the routing fields from *headers*, not from the payload, which is the
// promise §9.2 makes to consumers: "a consumer can route and deduplicate without
// deserialising". The payload stays opaque here and is parsed exactly once, in the
// application layer's translator.
//
// Nothing in this function can fail. Every absent or malformed header yields a
// zero value, and the reason is that the alternative is worse: a mapping that
// errored would put a malformed record on the same path as a broken broker, and
// the notifier's two paths for those are opposite — one dead-letters, one retries
// forever. Deciding which is which is the translator's job, and it needs the
// record in hand to do it.
func RecordToMessage(rec *kgo.Record) messaging.Message {
	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	// Parse errors are ignored deliberately; see the doc comment. strconv returns
	// 0 on failure, which is the value an absent header would also give.
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
		// Attempt is not set here. It is process-local bookkeeping the worker
		// owns, and an adapter that guessed at it would be inventing a fact
		// (§6.5).
	}
}
```

- [ ] **Step 3: Write the failing tests for the dead-letter publisher**

The unit test asserts header composition against a fake produce; the integration test asserts the real record against a real broker.

```go
// §12.2's header set, exactly: the original value and headers verbatim, plus six
// dlt_* headers. Verbatim is the requirement that matters — a DLT record must be
// replayable onto the source topic without reconstruction (§12.3), and a
// publisher that dropped the original correlation id would break the trace of
// every replayed message.
func TestDeadLetterPublisher_CarriesTheOriginalAndTheDiagnosis(t *testing.T) {
	// original headers survive; ce_id, ce_type, correlation_id all present
	// dlt_reason = "permanent", dlt_error contains the error text
	// dlt_deliveries = "3"
	// dlt_original_topic / _partition / _offset name where it came from
}

// The key survives, because a replayed record must land on the same partition as
// the original — per-aggregate ordering is a stated guarantee (§12.4), and a
// replay that changed partitions would reorder one user's events.
func TestDeadLetterPublisher_PreservesThePartitionKey(t *testing.T) {}

// Every error wraps ErrTransient. There is no permanent failure of dead-lettering
// the notifier could act on: if the DLT is unreachable the right behaviour is to
// stop making progress, not to drop the record.
func TestDeadLetterPublisher_ErrorsAreTransient(t *testing.T) {}
```

- [ ] **Step 4: Write `dlt.go`**

```go
// The dlt_* header names of §12.2. Constants because a replay tool reads them and
// a human greps for them, and a typo would be invisible until the day it mattered.
const (
	headerDLTReason          = "dlt_reason"
	headerDLTError           = "dlt_error"
	headerDLTDeliveries      = "dlt_deliveries"
	headerDLTOriginalTopic   = "dlt_original_topic"
	headerDLTOriginalPart    = "dlt_original_partition"
	headerDLTOriginalOffset  = "dlt_original_offset"
)

// maxDLTErrorLength bounds the error text. Kafka's default max.message.bytes is
// about a megabyte and an error string is normally a line, but "normally" is doing
// a lot of work in a header composed from an arbitrary error: a driver error
// carrying a whole query, or an error chain built in a retry loop, can be large,
// and a DLT produce that fails because the *diagnosis* was too big would lose the
// record it was trying to save.
const maxDLTErrorLength = 4096

// DeadLetterPublisher produces the record that will never succeed to the
// dead-letter topic, and waits for the durable ack.
//
// ProduceSync, not Produce: the offset is committed after this returns, so a
// fire-and-forget DLT produce would forget where the record was before it was
// durable anywhere else. §12.2: "the record is durable somewhere else before we
// forget where it was."
type DeadLetterPublisher struct {
	client *kgo.Client
	topic  string
}
```

`Publish` builds `[]kgo.RecordHeader` from the original `msg.Headers` in sorted key order (deterministic, so a test can assert without sorting), appends the six `dlt_*` headers, sets `Key: []byte(msg.Key)` and `Value: msg.Payload`, calls `client.ProduceSync(ctx, rec).FirstErr()`, and wraps any error with `application.ErrTransient`.

- [ ] **Step 5: Run everything**

Run: `make lint && go test ./internal/notifications/... -v && go test -tags=integration ./internal/notifications/... -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/notifications/infrastructure/kafka
git commit -m "feat(notifications): the record mapping, and a dead letter you can replay"
```

---

## Task 10: The notifier worker

**Files:**
- Create: `internal/notifications/presentation/worker/notifier.go`
- Test: `internal/notifications/presentation/worker/notifier_test.go`

**Interfaces:**
- Consumes: `application.ProcessUserRegistered`, `application.DeadLetterPublisher`, `kafkainfra.RecordToMessage`.
- Produces: `NotifierPolicy{RetryAttempts int, RetryBase time.Duration, MaxDeliveries int, DrainGrace time.Duration}`, `NewNotifier(client *kgo.Client, process Processor, dlt application.DeadLetterPublisher, log *slog.Logger, policy NotifierPolicy) *Notifier`, `(*Notifier).Run(ctx) error`.

The most decision-dense file in the plan. It owns four things: the ordering rule, the retry ladder, the delivery count, and the drain.

**The ordering rule.** Process the record, commit the transaction, then commit the offset. Never any other order. A crash between the second and the third causes redelivery, the inbox insert conflicts, the duplicate is skipped, and the offset is committed — "that is the design working" (§10.2). Committing the offset first would turn a crash into permanent loss.

**The retry ladder (§12.2), in order.** Three in-process attempts at 50ms·2^n with jitter for a transient error, offset uncommitted. Then redelivery: return without committing, and Kafka hands the record back on the next poll cycle or after a rebalance. Then the dead-letter topic, on a permanent error or after `CONSUMER_MAX_DELIVERIES` deliveries.

**The delivery count** is in-memory, keyed by `(topic, partition, offset)`, and resets on a rebalance or a restart (§6.5). It is a heuristic for "stop retrying this one", never a correctness input. It must be pruned on a successful commit and on revocation, or a long-running notifier's map grows without bound — a detail the spec does not mention and a reviewer would find.

**The drain.** Identical in shape and reason to the relay's `ce144d0`: on `SIGTERM`, stop fetching, but let the record in flight finish its transaction *and its offset commit*. The window here is smaller than the relay's and its cost is the same — a database commit whose offset was never committed is a redelivery, absorbed by the inbox but not free. §13.6 says it plainly: "`terminationGracePeriod` must exceed one poll-and-commit cycle so a rolling restart does not manufacture redeliveries."

- [ ] **Step 1: Write the failing tests**

```go
// The ordering rule, asserted as a sequence. This is the test that matters most in
// the file: every other property could be wrong and the system would still be
// correct-ish, while this one being wrong is silent, permanent data loss.
func TestNotifier_CommitsTheOffsetAfterTheTransaction(t *testing.T) {
	// fake processor records "process"; fake committer records "commit"
	// assert ops == ["process", "commit"]
}

// A transient failure retries in process, up to RetryAttempts, and then stops
// without committing — so Kafka redelivers.
func TestNotifier_RetriesTransientFailuresThenLeavesTheOffset(t *testing.T) {}

// A permanent failure goes to the DLT *and then* commits, in that order, and does
// not retry at all: retrying a record that will never succeed is a way of never
// noticing it.
func TestNotifier_DeadLettersAPermanentFailureBeforeCommitting(t *testing.T) {}

// The delivery ladder's top rung: the same record handed back MaxDeliveries times
// is dead-lettered with dlt_reason=max_deliveries even though every failure was
// transient. A transient error that never clears is a poison record wearing a
// disguise.
func TestNotifier_DeadLettersAfterMaxDeliveries(t *testing.T) {}

// And the count is per record, not global. Two records each failing twice must not
// dead-letter either of them.
func TestNotifier_TheDeliveryCountIsPerRecord(t *testing.T) {}

// A failed DLT produce must not commit the offset. This is the one place where
// giving up would be losing the record: the DLT is where a record goes to be
// remembered, and forgetting where it was because we could not remember it
// elsewhere is exactly backwards.
func TestNotifier_AFailedDeadLetterDoesNotCommit(t *testing.T) {}

// A duplicate commits the offset, because there is nothing left to do and the
// partition must advance. A duplicate that declined to commit would redeliver
// forever.
func TestNotifier_ADuplicateCommitsTheOffset(t *testing.T) {}

// An ignored record commits the offset too, and writes no inbox row (the use case
// saw to that). The log line says ignored, which is how a reader finds out that
// accounts has started publishing something new.
func TestNotifier_AnIgnoredRecordCommitsTheOffset(t *testing.T) {}

// The delivery map is pruned. A notifier that ran for a month would otherwise hold
// one entry per record it ever processed.
func TestNotifier_ForgetsRecordsItHasFinished(t *testing.T) {}

// The drain, and the lesson of the relay's ce144d0: SIGTERM stops the fetching,
// not the record in flight. Cancel ctx while a record is mid-transaction and
// assert that its transaction *and* its offset commit both completed.
func TestNotifier_ShutdownLetsTheRecordInFlightCommitItsOffset(t *testing.T) {}

// And the negative: a record that outlasts the grace is abandoned, loudly, with a
// log line saying a redelivery is coming. Silence here would make a duplicate look
// like a mystery.
func TestNotifier_ARecordThatOutlastsTheGraceIsAbandonedLoudly(t *testing.T) {}
```

- [ ] **Step 2: Run, fail, write `notifier.go`**

The loop, in the order the code should read:

```go
func (n *Notifier) Run(ctx context.Context) error {
	// Two contexts, and the difference between them is graceful shutdown: ctx
	// means stop fetching new records, record means abandon the one in hand. The
	// same construction as the relay's, and the same argument — see
	// (*Relay).draining and spec §11.2's "a SIGTERM is not a crash".
	work, abandon := n.draining(ctx)
	defer abandon()

	for {
		if ctx.Err() != nil {
			n.stopping(work)
			return nil
		}
		fetches := n.client.PollRecords(ctx, n.policy.PollMax)
		if err := n.fetchErr(fetches); err != nil {
			// A fetch error is not a record error. Log it and poll again; the
			// client is already reconnecting underneath.
			n.log.Warn("fetch failed", "error", err)
			continue
		}
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			// One record, one transaction (§10.2). Batching would be faster and
			// is the wrong default for a reference implementation: a single
			// poison record would fail its whole batch, and partial progress
			// could not be committed.
			if err := n.handle(work, rec); err != nil {
				// Nothing was committed. Stop consuming this fetch and let Kafka
				// redeliver from the last committed offset — continuing would
				// commit a later offset than one we failed on, which is the
				// same loss as committing before the transaction.
				n.log.Error("record not processed; leaving the offset for redelivery",
					"error", err, "topic", rec.Topic,
					"partition", rec.Partition, "offset", rec.Offset)
				break
			}
			if work.Err() != nil {
				// The grace expired mid-fetch. Stop cleanly rather than starting
				// another transaction that cannot finish.
				break
			}
		}
	}
}
```

`handle` is the ladder and the ordering rule together:

```go
func (n *Notifier) handle(ctx context.Context, rec *kgo.Record) error {
	msg := kafkainfra.RecordToMessage(rec)
	key := coord{topic: rec.Topic, partition: rec.Partition, offset: rec.Offset}
	msg.Attempt = n.deliveries.count(key)

	res, err := n.processWithRetry(ctx, msg)
	switch {
	case err == nil:
		// The transaction has committed. Only now is the offset safe to commit.
		if err := n.client.CommitRecords(ctx, rec); err != nil {
			return err
		}
		n.deliveries.forget(key)
		n.logRecord(msg, res)
		return nil

	case errors.Is(err, application.ErrPermanent), msg.Attempt+1 >= n.policy.MaxDeliveries:
		// The record is durable somewhere else before we forget where it was
		// (§12.2).
		reason := application.DeadLetterReason{
			Reason:     reasonFor(err, msg.Attempt+1, n.policy.MaxDeliveries),
			Err:        err.Error(),
			Deliveries: msg.Attempt + 1,
		}
		if dltErr := n.dlt.Publish(ctx, msg, reason); dltErr != nil {
			// Do not commit. The DLT is where a record goes to be remembered.
			return errors.Join(err, dltErr)
		}
		if err := n.client.CommitRecords(ctx, rec); err != nil {
			return err
		}
		n.deliveries.forget(key)
		n.log.Error("record dead-lettered", "reason", reason.Reason, /* ... */)
		return nil

	default:
		// Transient, and the in-process retries are spent. Count the delivery and
		// return without committing: rung 2 of the ladder is Kafka's redelivery,
		// and the count is what eventually escalates to rung 3.
		n.deliveries.increment(key)
		return err
	}
}
```

The log line, per §13.3 ("The notifier logs `duplicate` and `recorded` per record, which is what makes the inbox's work visible"):

```go
func (n *Notifier) logRecord(msg messaging.Message, res application.ProcessResult) {
	attrs := []any{
		"event_id", msg.EventID, "event_type", msg.EventType,
		"partition", msg.Partition, "offset", msg.Offset,
		"recorded", res.Recorded, "duplicate", res.Duplicate,
	}
	switch {
	case res.ConstraintCaught:
		// The inbox did not stop this one; the UNIQUE constraint did. Both are
		// correct and only one is expected, so this is the one duplicate that gets
		// a WARN (§8.2, §12.3).
		n.log.Warn("duplicate absorbed by source_event_id, not by the inbox; "+
			"check inbox retention against broker retention", attrs...)
	case res.Duplicate:
		// INFO, not WARN. A non-zero duplicate count is the inbox doing its job
		// (§13.3), and logging it as a problem trains a reader to ignore it.
		n.log.Info("duplicate absorbed", attrs...)
	case res.Ignored:
		n.log.Info("event type not handled by this consumer", attrs...)
	default:
		n.log.Info("record processed", append(attrs, "notification_id", res.NotificationID)...)
	}
}
```

`deliveries` is a small type over `map[coord]int` — no mutex, because every call is on `Run`'s goroutine, and a comment saying so beats a mutex nobody needs. `draining` and `stopping` are copied from `internal/accounts/presentation/worker/relay.go`, comments included: **no logging from inside the `context.AfterFunc` callback** — `stop()` reports the callback has *started* and offers no way to join it, so a line from it can outlive `Run`. That bug was found by `-race` once already; do not reintroduce it.

- [ ] **Step 3: Prove the ordering test has teeth**

Swap `CommitRecords` to before `processWithRetry`. `TestNotifier_CommitsTheOffsetAfterTheTransaction` must fail. Restore.

- [ ] **Step 4: Prove the drain tests have teeth**

Replace `draining` with `func (n *Notifier) draining(ctx context.Context) (context.Context, context.CancelFunc) { return context.WithCancel(ctx) }`. Both drain tests must fail. Restore.

- [ ] **Step 5: Run under the race detector**

Run: `go test -race ./internal/notifications/... -v`
Expected: PASS with no race reports. If the detector fires on a log line, the callback is logging — fix the design, not the test.

- [ ] **Step 6: Commit**

```bash
git add internal/notifications/presentation
git commit -m "feat(notifier): the offset commits after the transaction, and the record in flight finishes"
```

---

## Task 11: The inbox purge

**Files:**
- Create: `internal/notifications/application/purge_inbox.go`, `internal/notifications/infrastructure/postgres/inbox_purge.go`, `internal/notifications/presentation/worker/purger.go`
- Test: `purge_inbox_test.go`, `inbox_purge_integration_test.go`

**Interfaces:**
- Produces: `InboxPurger` port with `PurgeProcessedBefore(ctx, before time.Time, limit int) (int, error)`; `PurgeInbox` use case; `NewPurger(purge, log, policy)`.

Mechanically the relay's purge job against a different table. What is different is what the retention *means*, and the doc comment has to say it: §13.2 — "Inbox retention is not storage hygiene — it is the boundary of the deduplication guarantee." A purged inbox row is an event this consumer will process again if it ever arrives again, and §12.3 records the incident that follows: reset the group to earliest and every event whose inbox row was purged is processed twice.

That is why the default is 7 days rather than 24 hours, and why the guidance is `> broker_retention + max_replay_lag + max_outage_duration`.

- [ ] **Step 1** Write the use case with a fake purger: it loops until a pass removes fewer rows than the limit, returns `PurgeResult{Deleted int, Passes int}`, and stops on a cancelled context without treating cancellation as a failure. Assert the loop terminates, that it respects the limit, and that `before` is computed from the injected clock minus the retention.
- [ ] **Step 2** Write the SQL with the subquery bound, because PostgreSQL has no `DELETE … LIMIT`:

```sql
DELETE FROM notifications.inbox
 WHERE (consumer, event_id) IN (
    SELECT consumer, event_id FROM notifications.inbox
     WHERE processed_at < $1
     ORDER BY processed_at
     LIMIT $2
 )
```

The `ORDER BY processed_at` matches `inbox_processed_at_idx` (created in migration 0001), so the bound page is found by an index walk rather than a sort of the whole table. Note the difference from accounts' purge: the key is composite, so the subquery selects two columns and the `IN` compares a row constructor.

- [ ] **Step 3** Integration test: insert rows with `processed_at` spread across two weeks (pass explicit timestamps — do not rely on `now()`), purge with a 7-day retention, assert only the old ones are gone and that a second call deletes nothing.
- [ ] **Step 4** The worker is a ticker calling the use case and logging one line per run, structurally the relay's `purger.go`. Copy it.
- [ ] **Step 5** `make lint && go test -race ./internal/notifications/... && go test -tags=integration ./internal/notifications/...`
- [ ] **Step 6** Commit: `feat(notifications): purge the inbox, and say what the retention actually bounds`

---

## Task 12: Configuration and the process

**Files:**
- Modify: `internal/platform/config/config.go`, `config_test.go`, `.env.example`, `deploy/docker-compose.yml`, `deploy/kafka-init.sh`, `Makefile`
- Create: `cmd/notifier/main.go`

**Interfaces:**
- Produces: `config.LoadNotifier(getenv func(string) string) (NotifierConfig, error)` with `NotificationsDatabaseURL`, `KafkaBrokers`, `KafkaTopic`, `KafkaDLTTopic`, `KafkaGroupID`, `AdminAddr`, `LogLevel`, `MaxDeliveries`, `RetryAttempts`, `DrainGrace`, `InboxRetention`, `PurgeInterval`, `PurgeBatch`, `ChaosEnabled`.

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

Following `cmd/relay/main.go`: load config, build the logger, open the *notifications* pool only, build the consumer client, wire the use case, start the admin listener and the two workers under an `errgroup`, and — the part that makes the drain mean anything — order every `defer` so that it runs after `g.Wait()`.

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

## Task 14: Documentation, and the spec's four corrections

**Files:**
- Modify: `README.md`, `docs/superpowers/specs/2026-08-26-transactional-outbox-demo-design.md`, this plan
- Test: none

- [ ] **Step 1: Annotate §11.3 in the spec** with the four divergences this plan records — `msg.Metadata()`, `IDGen`'s error, the package qualifier, and the inbox-first ordering. Write them as corrections to the sketch, in the spec's own voice, the way §13.3's stats paragraph was corrected in `d7107ee`. A spec that keeps a sketch nobody implemented is a spec readers learn to distrust.

- [ ] **Step 2: Add `ErrNotForThisConsumer` to §12.2** as a third outcome beside transient and permanent, with the argument: a topic is a context's published language, not a queue for one consumer, and dead-lettering an event type this consumer does not want would flood the DLT on someone else's deployment.

- [ ] **Step 3: Add the notifier to §14's `RELAY_DRAIN_GRACE` row**, and add the two new validations (`KAFKA_DLT_TOPIC ≠ KAFKA_TOPIC`, `INBOX_RETENTION ≥ 24h`) to §14's prose.

- [ ] **Step 4: README.** The consuming half needs three things a reader cannot get from the code: what the notifier's log line means (`recorded`, `duplicate`, and why a non-zero duplicate count is good news); the guided duplicate demo, as commands to run and output to expect; and the honest limit — inbox retention is the boundary of the deduplication guarantee, so a group reset past it reprocesses, and that is a property of the design rather than a bug in it.

- [ ] **Step 5: Append a post-implementation review section to this plan**, as Plan 2 did: what the code taught that the plan got wrong, and which of those generalise to Plan 4. Two candidates are already visible from here — whether `ConstraintCaught` survived contact with PostgreSQL's transaction-abort semantics (Task 6, Step 5), and whether the delivery-count map needed anything the plan did not anticipate.

- [ ] **Step 6: Commit**

```bash
git add README.md docs
git commit -m "docs: the consuming half, and correct 11.3 where its sketch could not compile"
```

---

## Self-review

Run against the spec with fresh eyes, per the plan-writing checklist.

**Spec coverage.** §6.5 → Task 1. §7's notifications column → Tasks 2, 3, 6. §8.2's tables → Tasks 3, 7 (the schema itself already exists in `migrations/notifications/0001_init.sql`; this plan adds no migration and needs none). §10.2 → Task 8, Task 10. §11.3 → Tasks 4, 5, 6, 7. §11.5 → Task 13. §12.2 → Task 10. §12.3's retention → Task 11. §13.2's inbox purge → Task 11. §13.3's notifier line → Task 10. §13.5's `:8083` → Task 12. §14's notifier variables → Task 12. §15's levels → distributed across every task, with the end-to-end invariant in Task 13.

**Deliberately out of scope**, and named so that a reader does not think they were forgotten:

- **§12.5's chaos hooks and the `Chaos` port.** They span all four processes and belong with the process that completes the set. Task 13 proves the same two windows with tests instead, which is what §12.5 says the hooks are: "the tests of §21 exposed as buttons."
- **§13.4's readiness for group membership.** The notifier's `/readyz` should report whether the group is joined, and franz-go exposes that only indirectly. It goes with the admin surface work in Plan 5 rather than being half-done here; Task 12 wires `/healthz` and a pool-and-migration `/readyz` like the relay's.
- **§12.3's `replay-dlt` command.** A separate tool with no notifier dependency.
- **Everything the `sender` owns** — the email gateway, the `Idempotency-Key` call, `notifications.state='sent'`, the notifications outbox purge and its missing purge index. That is Plan 4. This plan writes the intent row and stops, which is exactly the seam §11.3 and §11.4 draw between them.

**Placeholder scan.** Four test bodies are specified by name, assertion list and the existing file to mirror rather than written out in full: Task 7's `uow_integration_test.go`, Task 8's two client integration tests, Task 9's three DLT tests, and Task 10's twelve notifier tests. That is a deliberate exception to "show the code", taken because each has a near-exact counterpart already committed in `internal/accounts` — and a plan that transcribes 600 lines of existing test scaffolding buries the twelve assertions that are actually new. Every one of them names its subject, its assertion and its argument; none says "test the above". If the implementer finds any of them underspecified, that is a plan bug and worth recording in the post-implementation section.

**Type consistency.** `application.Metadata` is declared in Task 6 and used in Task 5; Task 5's interface block says so, and the two tasks must be done in order or the type moved. `Envelope.IdempotencyKey` (Task 5) is read by Task 7's `insertOutbox`. `ProcessResult`'s four booleans (Task 6) are read by Task 10's `logRecord`. `InboxRepository.Record`'s `(first bool, err error)` (Task 6) is implemented in Task 7 and no test asserts an error for a duplicate. `DeadLetterReason` (Task 4) is built in Task 10 and consumed in Task 9. `messaging.Message.Attempt` (Task 1) is written only in Task 10 and read only in Task 9's reason — nothing persists it, which is the property Task 1's comment demands.

**One known conflict, resolved in the plan text.** Task 4's grep test forbids accounts' vocabulary outside `translate.go`; Task 5's `envelope.go` legitimately contains `user_id` in *this* context's published schema. Task 5, Step 3 says how to resolve it and why the forbidden list narrows to `com.outboxexpress.accounts` and `display_name` rather than exempting a second file.
