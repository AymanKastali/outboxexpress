package application

import (
	"context"
	"time"
)

// PublishUnitOfWork is the relay's transaction boundary, and a second unit of
// work rather than a mode of the first. The separation is load-bearing twice.
//
// UnitOfWork's Work must never expose an outbox: the moment it does, RegisterUser
// can append by hand and D4's structural guarantee becomes a rule people have to
// remember, with no failing test to notice — the architecture test walks imports
// and this would be an intra-package reference.
//
// And this transaction takes no Metadata and drains no aggregate events, because
// nothing in a relay pass loads an aggregate. A shared boundary would carry a
// tracker and an envelope factory that have nothing to do here, and their
// presence would suggest they might.
type PublishUnitOfWork interface {
	Do(ctx context.Context, fn func(PublishWork) error) error
}

// PublishWork is the repository set bound to the relay's claiming transaction.
type PublishWork struct {
	Outbox OutboxRepository
}

// OutboxRepository is the relay's half of the outbox table.
//
// It lives in application rather than domain because an outbox is not a domain
// concept — D5: "no domain expert has heard of one" — unlike domain.UserRepository,
// which is a Repository in the DDD sense and therefore belongs to the domain.
//
// Every method is called inside the claiming transaction, on the same connection
// that holds the row locks. That is not incidental: it is what makes the marks
// and the publish atomic with the claim (D6).
type OutboxRepository interface {
	// ClaimPending takes up to limit due rows in id order and holds them for the
	// rest of the transaction.
	ClaimPending(ctx context.Context, limit int) ([]PendingMessage, error)

	// MarkPublished records a durable ack.
	MarkPublished(ctx context.Context, id int64) error

	// Reschedule records a transient failure: one more attempt, a later
	// available_at, and the reason. The row stays pending. That it does is D8.
	Reschedule(ctx context.Context, id int64, availableAt time.Time, lastError string) error

	// MarkFailed parks a row a human has to look at.
	MarkFailed(ctx context.Context, id int64, lastError string) error

	// Stats reads the pass line's backlog fields. maxAttempts is the threshold
	// above which a still-retrying row is counted as stuck.
	Stats(ctx context.Context, maxAttempts int) (PassStats, error)
}

// PendingMessage is one claimed outbox row as the relay sees it: the routing
// columns, the bookkeeping needed to compute a backoff, and an opaque payload.
//
// There is deliberately no business field here, and no way to add one without
// noticing: the relay is a dumb pipe, and the moment it can read a user's email
// it is a service with an opinion about users.
type PendingMessage struct {
	// Envelope is embedded rather than restated: it is already exactly this
	// table's contract columns, seen from the write side, and two structs that
	// must agree column-for-column are two edits for every schema change with
	// nothing failing if only one is made.
	Envelope

	ID       int64
	Attempts int
}

// PassStats is what one pass can see of the outbox table as a whole (spec §13.3).
//
// It comes from a single query issued on the transaction the pass has already
// opened, so observation costs one round trip rather than a background collector
// holding its own connection.
//
// Every field here is a fact about the outbox table and nothing else.
// pg_notification_queue_usage() — which §13.3 also wants on the pass line — is
// deliberately *not* here: it is the health of the wakeup mechanism, so the
// component that owns that mechanism reports it (see the Usage method on the
// wakeup port in Task 7). Sourcing it from this query would put a notify-
// subsystem read inside the delivery transaction, which is the inversion §13.1
// exists to forbid, and would keep charging for it under RELAY_USE_NOTIFY=false
// where there is no listener at all.
type PassStats struct {
	Backlog          int
	OldestPendingAge time.Duration
	FailedRows       int
	StuckRows        int
}
