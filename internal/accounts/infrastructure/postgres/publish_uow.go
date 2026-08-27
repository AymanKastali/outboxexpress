package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// PublishUnitOfWork is the relay's transaction boundary (spec §11.2, D6).
//
// It is a second unit of work rather than a mode of the first, and the
// separation is load-bearing twice over.
//
// UnitOfWork's Work must never expose an outbox — the moment it does, a use case
// can append to it by hand and D4's structural guarantee becomes a rule people
// have to remember, with no failing test to notice because the architecture test
// walks imports and that would be an intra-package reference. And this
// transaction drains no aggregate events: nothing in a relay pass loads an
// aggregate, so a shared boundary would carry a tracker and an envelope factory
// that have no work to do here, and their presence would suggest they might.
//
// Publishing happens inside this transaction, which §11.1 rule 2 forbids for the
// registration transaction and D6 requires here. The rules do not contradict each
// other, because the two transactions' locks protect different things.
// Registration holds a lock on a user's row and must not hold it across a call to
// a system it does not control. This transaction's locks *are* the claim, and
// releasing them before the ack is exactly what would let a second relay publish
// the same row. The cost is a transaction held open for one produce — which is why
// spec §10.1 bounds it with RequestTimeoutOverhead and why §13.3 puts batch_ms on
// the pass line as "the signal for moving to claim-commit-publish".
type PublishUnitOfWork struct {
	pool *pgxpool.Pool
}

func NewPublishUnitOfWork(pool *pgxpool.Pool) *PublishUnitOfWork {
	return &PublishUnitOfWork{pool: pool}
}

func (u *PublishUnitOfWork) Do(ctx context.Context, fn func(application.PublishWork) error) error {
	// WithTx owns the begin/rollback/commit rules, including returning fn's
	// error unwrapped so that errors.Is against the two publish classes still
	// works several layers up.
	return platformpg.WithTx(ctx, u.pool, func(tx pgx.Tx) error {
		// Bound to tx, so the claim's locks and the marks are on one connection.
		// The pool is not reachable from fn, so there is no path to a mark
		// outside the transaction that claimed the row.
		return fn(application.PublishWork{Outbox: NewRelayOutbox(tx)})
	})
}

var _ application.PublishUnitOfWork = (*PublishUnitOfWork)(nil)
