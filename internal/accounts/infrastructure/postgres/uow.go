package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// UnitOfWork is the transaction boundary, and the reason this project has an
// outbox at all.
//
// Spec §5 is the argument: writing the business row and the outbox row in one
// transaction is the entire pattern, and making that structural — something the
// persistence layer does because an aggregate was persisted — is "the only way
// 'every state change emits its event' becomes structural rather than a rule
// people must remember at each call site".
//
// The cost is that the two inserts are no longer adjacent in the code a reader
// follows. That is why this file is short, and why Task 10's tests assert the
// invariant directly rather than trusting it.
// outboxAppender is the append role, declared at its consumer. It is not in
// application/ports.go because no use case mentions an outbox — that is the
// whole point of spec §5, and an interface exported from application that
// nothing in application uses invites exactly the reach-in that ports.go warns
// about for Work. The relay's claim/mark/fail role (Plan 2) is a different port
// with a real application consumer, and gets declared there.
type outboxAppender interface {
	Append(ctx context.Context, envelopes []application.Envelope) error
}

type UnitOfWork struct {
	pool      *pgxpool.Pool
	envelopes application.EnvelopeFactory

	// outbox builds the appender for a transaction. It is a field rather than a
	// direct call to NewOutboxRepository so that a test can substitute an
	// appender that fails: "the outbox insert failed, so the user must not
	// exist" is the dual-write refusal in its purest form, and there is no way
	// to provoke it from outside — the factory mints a fresh UUIDv7 per event,
	// so not even the event_id constraint can be made to fire.
	//
	// This is not the thing platform/clock declines to ship. That is about an
	// exported, wireable hook that a binary could reach; this is an unexported
	// field with no setter, invisible outside the package and unreachable from
	// any process's wiring. The seam a test needs and the seam a caller could
	// misuse are different seams.
	outbox func(platformpg.Queryer) outboxAppender
}

func NewUnitOfWork(pool *pgxpool.Pool, envelopes application.EnvelopeFactory) *UnitOfWork {
	return &UnitOfWork{
		pool:      pool,
		envelopes: envelopes,
		outbox: func(q platformpg.Queryer) outboxAppender {
			return NewOutboxRepository(q)
		},
	}
}

func (u *UnitOfWork) Do(ctx context.Context, meta application.Metadata, fn func(application.Work) error) error {
	// WithTx owns the begin/rollback/commit rules — including returning fn's
	// error unwrapped, so that domain sentinels survive the trip out (spec §6.4).
	return platformpg.WithTx(ctx, u.pool, func(tx pgx.Tx) error {
		tracker := newTracker()

		// Repositories are bound to tx and handed to fn as the only way to reach
		// persistence. The pool is not reachable from here, so there is no path
		// to an untransacted write (spec §11.1 rule 1).
		if err := fn(application.Work{Users: newUserRepository(tx, tracker)}); err != nil {
			return err
		}

		// Drain what the repositories touched. Nothing external happens in here:
		// no HTTP, no produce, no email (spec §11.1 rule 2).
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

var _ application.UnitOfWork = (*UnitOfWork)(nil)
