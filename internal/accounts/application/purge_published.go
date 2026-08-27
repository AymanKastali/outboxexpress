package application

import (
	"context"
	"time"
)

// PurgePublished deletes published rows past their retention, in bounded pages
// (spec §13.2).
//
// Mark-and-purge is D10: a row is marked published and deleted later by this
// job, never deleted at publish time. Deleting at publish would make the outbox
// unauditable exactly when it matters — "did we send it?" during an incident is
// the question §4 says this table exists to answer.
//
// Failed rows are never purged. That is enforced in the SQL, not here, and it is
// deliberate: a parked row is the only record that something needs a human.
type PurgePublished struct {
	outbox    OutboxPurger
	retention time.Duration
	batch     int
}

func NewPurgePublished(outbox OutboxPurger, retention time.Duration, batch int) *PurgePublished {
	return &PurgePublished{outbox: outbox, retention: retention, batch: batch}
}

// Execute deletes until a page comes back short.
//
// The loop is the point. PostgreSQL has no DELETE … LIMIT, so the bound goes in
// a subquery and the caller repeats — which means no single delete takes a long
// lock or holds a long transaction on the hottest table in the schema. One
// unbounded DELETE would be simpler and would be the thing §13.2 warns about.
func (uc *PurgePublished) Execute(ctx context.Context) (PurgeResult, error) {
	var res PurgeResult
	for {
		// Checked between pages, not just at the top: a purge is interruptible
		// by design, and the rows it did not delete are still published and
		// still purgeable on the next run.
		if err := ctx.Err(); err != nil {
			return res, err
		}
		deleted, err := uc.outbox.PurgePublished(ctx, uc.retention, uc.batch)
		if err != nil {
			// res carries what was already deleted, because those rows are gone
			// and the log line should say so rather than reporting zero.
			return res, err
		}
		res.Deleted += deleted
		res.Passes++

		// Fewer rows than asked for means the eligible set is exhausted.
		if deleted < uc.batch {
			return res, nil
		}
	}
}

// PurgeResult is what one purge run did. Passes is on the log line because it is
// how you notice a retention that is too long for the batch size: a run that
// takes hundreds of passes every minute is a table that is not keeping up.
type PurgeResult struct {
	Deleted int
	Passes  int
}

// OutboxPurger is the mark-and-purge half of D10, and a separate port from
// OutboxRepository because it takes no transaction: §13.2 requires each bounded
// delete to be its own short transaction, so that no purge holds a long lock on
// what is the hottest table in the schema.
type OutboxPurger interface {
	// PurgePublished deletes up to limit published rows older than retention and
	// returns how many it removed. Failed rows are never purged.
	PurgePublished(ctx context.Context, retention time.Duration, limit int) (int, error)
}
