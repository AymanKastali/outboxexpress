package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// RelayOutbox is the relay's half of accounts.outbox, bound to one claiming
// transaction: claim, the three marks, and the stats one pass reads.
//
// It is a distinct type from the producer's OutboxRepository, and the split is
// by *transaction lifetime* rather than by concern. Its constructor takes a
// pgx.Tx and nothing else, so the only production path to a claim or a mark is
// through PublishUnitOfWork.Do — there is no way to hold one of these against a
// pool.
//
// That matters because it is the same guarantee Plan 1 spent its headline effort
// on: application.Work must never expose an outbox, so that no use case can write
// an outbox row outside the transaction that produced it. A single pool-bound
// type carrying both Append and MarkPublished would put both of those back within
// reach of any wiring function that happened to build one — outbox.Append with no
// user row, outbox.MarkPublished on a row nobody claimed — and the compiler would
// have nothing to say about it. Three doc comments asserting a rule are worth
// less than one constructor that cannot express its violation.
//
// The purge and the stats read are the other two lifetimes — no transaction at
// all — and get their own pool-bound types below. Neither can append and neither
// can mark, so putting them on the pool costs nothing this type is protecting.
type RelayOutbox struct {
	tx pgx.Tx
}

// NewRelayOutbox takes a pgx.Tx, not a Queryer: every method here is called
// inside the transaction that holds the row locks, and that is not incidental —
// it is what makes the marks and the publish atomic with the claim (D6).
func NewRelayOutbox(tx pgx.Tx) *RelayOutbox {
	return &RelayOutbox{tx: tx}
}

// ClaimPending takes up to limit due rows and holds them for the rest of the
// transaction.
//
// It can return fewer rows than limit while more are pending: PostgreSQL applies
// LIMIT before skipping locked rows. That is not a bug to work around — the rows
// it skipped are held by another relay, and the next pass sees whatever is left.
func (r *RelayOutbox) ClaimPending(ctx context.Context, limit int) ([]application.PendingMessage, error) {
	rows, err := r.tx.Query(ctx, claimPending, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox: %w", err)
	}
	defer rows.Close()

	// limit is the exact upper bound, and PendingMessage is a wide struct, so
	// growing from nil would reallocate and copy several times per pass.
	batch := make([]application.PendingMessage, 0, limit)

	// Scanned field by field rather than with pgx.RowToStructByPos, so that the
	// SELECT's column order and PendingMessage's field order are free to differ.
	// A positional mapping would make reordering a struct field a silent data
	// corruption — and PendingMessage embeds Envelope, which a positional
	// mapping would flatten in an order nothing pins.
	for rows.Next() {
		var pending application.PendingMessage
		if err := rows.Scan(
			&pending.ID, &pending.EventID, &pending.AggregateType, &pending.AggregateID,
			&pending.EventType, &pending.SchemaVersion, &pending.Payload,
			&pending.Headers, &pending.OccurredAt, &pending.Attempts,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		batch = append(batch, pending)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: claim outbox: %w", err)
	}
	return batch, nil
}

// claimPending is the SELECT of spec §11.2.
//
// Claim by state, never by cursor. A WHERE id > :last_seen relay loses events:
// sequence values are assigned at INSERT but rows become visible at COMMIT, and
// those orders differ — a row that commits late is already behind the cursor and
// is never published, silently. Claiming by status has no such flaw, because
// uncommitted rows are simply not in this transaction's snapshot.
//
// ORDER BY id, never occurred_at: clocks skew, NTP steps backwards, and two rows
// written in the same millisecond have no defined order. id is the ordering
// authority (spec §8.1).
//
// FOR UPDATE SKIP LOCKED gives mutual exclusion per row, no blocking between
// relays, and a lease that releases itself — a crashed relay's locks are dropped
// by the database when its connection goes, so there is no stuck claim and no
// reaper to write. A locked_by/locked_at column would need a lease longer than
// the worst-case publish time or it becomes a duplicate factory.
//
// The index outbox_pending_idx (available_at, id) WHERE status = 'pending' is
// exactly this predicate.
const claimPending = `
SELECT id, event_id, aggregate_type, aggregate_id, event_type, schema_version,
       payload, headers, occurred_at, attempts
  FROM accounts.outbox
 WHERE status = 'pending'
   AND available_at <= now()
 ORDER BY id
 LIMIT $1
 FOR UPDATE SKIP LOCKED`

func (r *RelayOutbox) MarkPublished(ctx context.Context, id int64) error {
	return r.markOne(ctx, markPublished, "mark published", id)
}

// last_error is cleared, because a published row carrying the reason its previous
// attempt failed reads as a problem to whoever is looking at this table during an
// incident.
const markPublished = `
UPDATE accounts.outbox
   SET status = 'published', published_at = now(), last_error = NULL
 WHERE id = $1`

func (r *RelayOutbox) Reschedule(ctx context.Context, id int64, availableAt time.Time, lastError string) error {
	return r.markOne(ctx, rescheduleRow, "reschedule", id, availableAt, lastError)
}

// There is no status change here, and that absence is D8: a transient failure
// never moves a row towards 'failed', however many times it recurs. attempts is
// incremented in SQL rather than passed in because the row is locked by this
// transaction — attempts + 1 cannot disagree with the row, whereas a value
// computed in Go from a snapshot could.
const rescheduleRow = `
UPDATE accounts.outbox
   SET attempts     = attempts + 1,
       available_at = $2,
       last_error   = $3
 WHERE id = $1`

func (r *RelayOutbox) MarkFailed(ctx context.Context, id int64, lastError string) error {
	return r.markOne(ctx, markFailed, "mark failed", id, lastError)
}

// attempts is left alone. It drives backoff, and a failed row is never retried,
// so there is nothing left for it to schedule; spec §12.1's permanent row asks
// for a status and a last_error and says nothing about attempts.
const markFailed = `
UPDATE accounts.outbox
   SET status = 'failed', last_error = $2
 WHERE id = $1`

// markOne runs an UPDATE that must touch exactly one row.
//
// The row-count check is the point. Every one of these runs inside the
// transaction that holds the row's lock, so zero rows affected means the id is
// wrong or somebody deleted the row from under a locked claim. Either way the
// pass is publishing messages it cannot account for, and a failed transaction is
// better than a committed lie.
func (r *RelayOutbox) markOne(ctx context.Context, sql, what string, id int64, args ...any) error {
	tag, err := r.tx.Exec(ctx, sql, append([]any{id}, args...)...)
	if err != nil {
		return fmt.Errorf("postgres: %s (outbox id %d): %w", what, id, err)
	}
	if affected := tag.RowsAffected(); affected != 1 {
		return fmt.Errorf("postgres: %s (outbox id %d): %d rows affected, want 1",
			what, id, affected)
	}
	return nil
}

var _ application.OutboxRepository = (*RelayOutbox)(nil)

// purgePublished is the delete of spec §13.2.
//
// PostgreSQL has no DELETE … LIMIT, so the bound goes in a subquery. ORDER BY id
// inside it means each page takes the oldest rows, so the work is done in index
// order and a run that is interrupted has still made progress at the front.
//
// status = 'published' is what keeps failed rows out. §13.2: "failed rows are
// never purged automatically" — a parked row is the only record that something
// needs a human, and a retention job is not the thing that should decide nobody
// is coming.
//
// make_interval(secs => $1) rather than $1::interval, because binding an interval
// means formatting a Go duration into PostgreSQL's interval syntax as text.
// make_interval takes a number, so the duration crosses as a float and no string
// is built.
const purgePublished = `
DELETE FROM accounts.outbox
 WHERE id IN (
    SELECT id
      FROM accounts.outbox
     WHERE status = 'published'
       AND published_at < now() - make_interval(secs => $1)
     ORDER BY id
     LIMIT $2
 )`

// outbox_purge_idx (published_at, id) WHERE status = 'published' is what keeps
// this off a full scan when nothing is eligible — a fresh deploy, a quiet night,
// or a retention that was just raised. In the steady state the eligible rows sit
// at the front of the table and it stops early either way; it is the empty case
// that runs once a minute forever.

// OutboxPurger is the third lifetime this table has, and the reason it is a third
// type rather than another method on RelayOutbox: §13.2 requires each bounded
// delete to be its own short transaction, so this one is pool-bound by
// construction. RelayOutbox takes a pgx.Tx precisely so that a claim or a mark
// cannot happen outside a transaction; the purge is the opposite requirement, and
// putting both on one type would leave neither enforced.
type OutboxPurger struct {
	pool *pgxpool.Pool
}

func NewOutboxPurger(pool *pgxpool.Pool) *OutboxPurger {
	return &OutboxPurger{pool: pool}
}

// PurgePublished deletes up to limit eligible rows and returns how many it
// removed. The caller loops (see application.PurgePublished) until a page comes
// back short.
//
// Each call is its own implicit transaction — pool.Exec, no BEGIN — which is what
// §13.2 asks for: the lock is held for one bounded delete and no longer.
func (r *OutboxPurger) PurgePublished(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.pool.Exec(ctx, purgePublished, retention.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("postgres: purge published outbox rows: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

var _ application.OutboxPurger = (*OutboxPurger)(nil)

// OutboxStatsReader answers §13.3's backlog fields, on the pool and never on the
// claiming transaction.
//
// That is not a convenience. Inside the transaction a failure here is not
// tolerable — PostgreSQL aborts a transaction on any statement error, so a
// statement_timeout on this query would turn the commit that marks a
// durably-acked batch into an ErrTxCommitRollback, republishing every message in
// it because a count could not be read. application.OutboxStatsReader argues it
// at length. This type is the half of that argument the compiler can hold: it has
// no transaction to abort.
//
// It costs one round trip more than sharing the pass's transaction. It does not
// cost a background collector or a held connection, which is what §13.3's "rather
// than a background collector holding its own connection" was protecting.
type OutboxStatsReader struct {
	pool *pgxpool.Pool
}

func NewOutboxStatsReader(pool *pgxpool.Pool) *OutboxStatsReader {
	return &OutboxStatsReader{pool: pool}
}

// Read reports the table's state, in one query, in its own implicit transaction.
func (r *OutboxStatsReader) Read(ctx context.Context, maxAttempts int) (application.OutboxStats, error) {
	var (
		stats             application.OutboxStats
		oldestPendingAgeS float64
	)
	err := r.pool.QueryRow(ctx, outboxStats, maxAttempts).Scan(
		&stats.Backlog,
		&oldestPendingAgeS,
		&stats.FailedRows,
		&stats.StuckRows,
	)
	if err != nil {
		return application.OutboxStats{}, fmt.Errorf("postgres: outbox stats: %w", err)
	}
	stats.OldestPendingAge = time.Duration(oldestPendingAgeS * float64(time.Second))
	return stats, nil
}

// outboxStats is the whole of §13.3's backlog reporting, in one query.
//
// One query rather than four, on the transaction the pass has already opened, so
// that observing the relay costs one round trip. There is no metrics registry and
// no background collector by design (spec §13.3).
//
// Four scalar subqueries rather than four aggregates with FILTER over one scan,
// and this is the difference between O(backlog) and O(table). A bare
// `SELECT count(*) FILTER (...) FROM accounts.outbox` has no WHERE clause, so the
// planner cannot use a partial index for any of the aggregates and must
// sequentially scan every row the table retains — with OUTBOX_RETENTION at 24h
// that is every event published in the last day, on every pass, and after a full
// batch the next pass starts immediately. Giving each aggregate its own predicate
// lets the pending ones ride outbox_pending_idx and the failed one ride
// outbox_failed_idx (added in Step 2), both index-only.
//
// oldest_pending_age counts from occurred_at of the oldest *pending* row.
// Published and failed rows must not count, or one long-parked row would make a
// healthy relay look permanently stuck. coalesce covers the empty table, where
// min() is NULL — without it an idle relay would report an age of decades.
const outboxStats = `
SELECT
    (SELECT count(*) FROM accounts.outbox
      WHERE status = 'pending')                                     AS backlog,
    (SELECT coalesce(extract(epoch FROM now() - min(occurred_at)), 0)
       FROM accounts.outbox
      WHERE status = 'pending')                                     AS oldest_pending_age_s,
    (SELECT count(*) FROM accounts.outbox
      WHERE status = 'failed')                                      AS failed_rows,
    (SELECT count(*) FROM accounts.outbox
      WHERE status = 'pending' AND attempts >= $1)                  AS stuck_rows`

var _ application.OutboxStatsReader = (*OutboxStatsReader)(nil)
