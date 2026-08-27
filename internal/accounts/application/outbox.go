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

// OutboxStats is the state of the outbox table as a whole: the backlog fields of
// spec §13.3, which are the only view an operator gets of whether the relay is
// winning.
//
// Every field here is a fact about the table and nothing else — which is why it
// is not called PassStats. It describes the outbox, not the pass; PublishResult
// is the pass's own statistics, and nesting one inside the other under two names
// that both said "stats of the pass" left one of them lying.
//
// pg_notification_queue_usage() — which §13.3 also wants on the pass line — is
// deliberately absent. It is the health of the wakeup mechanism, so the component
// that owns that mechanism reports it (see the Usage method on the wakeup port in
// presentation/worker). Sourcing it from this query would keep charging for it
// under RELAY_USE_NOTIFY=false, where there is no listener at all.
type OutboxStats struct {
	Backlog          int
	OldestPendingAge time.Duration
	FailedRows       int
	StuckRows        int
}

// OutboxStatsReader reads OutboxStats. It is a second port over the same table,
// and it is pool-bound rather than transaction-bound. That is the whole point.
//
// The obvious design puts Stats on OutboxRepository and issues it on the
// transaction the pass has already opened, which is what §13.3 describes and
// what this code did. It is one round trip cheaper and it is wrong, because
// inside a transaction there is no such thing as a tolerable failure. PostgreSQL
// aborts a transaction on any statement error, so a statement_timeout on this
// query — and this is the query that gets slow, because counting the backlog
// costs the backlog — leaves the commit that would have marked a durably-acked
// batch returning ErrTxCommitRollback instead. Every message in that batch is
// then published a second time because a count of rows could not be read.
//
// §13.1 states the rule for the wakeup: "an optimisation, never the delivery
// path." Observation is the same rule's other half, and this port is what makes
// it structural — there is no transaction in scope here to abort.
//
// Reading after the commit also makes the numbers honest. Read inside the
// transaction they described a state that had not happened yet and might never:
// a backlog that excluded rows the pass had marked, in a pass that then rolled
// back.
type OutboxStatsReader interface {
	// Read reports the table's state. maxAttempts is the threshold above which a
	// still-retrying row is counted as stuck (§12.1: an alert threshold, not a
	// state transition).
	Read(ctx context.Context, maxAttempts int) (OutboxStats, error)
}
