//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

// Spec §11.2: claim by state, in id order, and only what is due.
func TestClaimPending_TakesWhatIsDueInIDOrder(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	first := due(t, pool, "ada")
	second := due(t, pool, "grace")
	notYet := seed(t, pool, row{Aggregate: "alan", Attempts: 2, AvailableIn: time.Hour})
	alreadyDone := seed(t, pool, row{Aggregate: "linus", Status: "published"})
	parked := seed(t, pool, row{Aggregate: "ken", Status: "failed", Attempts: 1})

	uow := NewPublishUnitOfWork(pool)
	var claimed []application.PendingMessage
	err := uow.Do(ctx, func(w application.PublishWork) error {
		var err error
		claimed, err = w.Outbox.ClaimPending(ctx, 10)
		return err
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if len(claimed) != 2 {
		t.Fatalf("claimed %d rows, want 2 (ids %d and %d); got %+v",
			len(claimed), first, second, claimedIDs(claimed))
	}
	if claimed[0].ID != first || claimed[1].ID != second {
		t.Errorf("claimed %v, want [%d %d] — id is the ordering authority (§8.1)",
			claimedIDs(claimed), first, second)
	}
	for _, unwanted := range []struct {
		id  int64
		why string
	}{
		{notYet, "available_at is in the future"},
		{alreadyDone, "status is published"},
		{parked, "status is failed"},
	} {
		for _, got := range claimed {
			if got.ID == unwanted.id {
				t.Errorf("claimed row %d, which should be invisible: %s", unwanted.id, unwanted.why)
			}
		}
	}

	// Every column the relay routes and publishes on came back.
	row := claimed[0]
	if row.AggregateType != "User" || row.AggregateID != "ada" {
		t.Errorf("routing = %q/%q, want User/ada", row.AggregateType, row.AggregateID)
	}
	if row.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want 1", row.SchemaVersion)
	}
	if len(row.Payload) == 0 {
		t.Error("payload is empty")
	}
	if row.Headers["correlation_id"] != "corr-ada" {
		t.Errorf("headers = %v, want the stored correlation_id", row.Headers)
	}
	if row.OccurredAt.IsZero() {
		t.Error("occurred_at is zero; oldest_pending_age would be meaningless")
	}
}

// Spec §11.2 on FOR UPDATE SKIP LOCKED: "mutual exclusion per row, no blocking
// between relays". This is D7's safety half — two relays never publish one row
// concurrently — and it is the only reason a hot standby is safe at all.
func TestClaimPending_TwoTransactionsClaimDisjointRows(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	for _, name := range []string{"ada", "grace", "alan", "linus"} {
		due(t, pool, name)
	}

	// Two transactions on two connections, the second claiming while the first
	// still holds its rows.
	firstTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first: %v", err)
	}
	defer func() { _ = firstTx.Rollback(ctx) }()

	firstBatch, err := NewRelayOutbox(firstTx).ClaimPending(ctx, 2)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	secondTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin second: %v", err)
	}
	defer func() { _ = secondTx.Rollback(ctx) }()

	secondBatch, err := NewRelayOutbox(secondTx).ClaimPending(ctx, 2)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if len(firstBatch) != 2 || len(secondBatch) != 2 {
		t.Fatalf("batches = %v and %v, want two rows each", claimedIDs(firstBatch), claimedIDs(secondBatch))
	}
	held := map[int64]bool{}
	for _, row := range firstBatch {
		held[row.ID] = true
	}
	for _, row := range secondBatch {
		if held[row.ID] {
			t.Errorf("row %d was claimed by both transactions; SKIP LOCKED is not "+
				"doing its job and two relays would publish it twice", row.ID)
		}
	}
}

// Spec §11.2: "automatic lease release: a crashed relay's locks are dropped by
// the database on disconnect. There is no stuck claim and no reaper to write."
// pg_terminate_backend is the closest thing to killing a relay mid-pass.
func TestClaimPending_LocksReleaseWhenTheRelayDies(t *testing.T) {
	dsn, pool := pgtest.Accounts(t)
	ctx := context.Background()
	due(t, pool, "ada")

	// A connection of its own, so terminating it cannot take out the pool.
	doomed, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	var pid int
	if err := doomed.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatalf("backend pid: %v", err)
	}
	tx, err := doomed.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	claimed, err := NewRelayOutbox(tx).ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d rows, want 1", len(claimed))
	}

	// While that transaction holds the lock, nobody else can have the row.
	if got := claimIn(t, pool, 10); len(got) != 0 {
		t.Fatalf("claimed %v while another transaction holds the lock", got)
	}

	// Kill the relay.
	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	_ = doomed.Close(ctx)

	// The row is claimable again, with no reaper having run.
	//
	// Polled rather than asserted once: pg_terminate_backend signals the backend
	// and returns, so the locks are dropped a moment later when it actually dies.
	// A single immediate claim is a race that passes on a fast machine and fails
	// on a loaded one.
	var got []int64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got = claimIn(t, pool, 10); len(got) == 1 {
			return
		}
	}
	t.Errorf("claimed %v after the holder died, want 1 row — PostgreSQL is "+
		"supposed to release the locks with the connection", got)
}

func TestMarkPublished_RecordsTheAckAndClearsTheLastError(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	// A row that failed transiently before and is now succeeding.
	id := seed(t, pool, row{Aggregate: "ada", Attempts: 3})
	pgtest.MustExec(t, pool, `UPDATE accounts.outbox SET last_error = 'NOT_ENOUGH_REPLICAS' WHERE id = $1`, id)

	inTx(t, pool, func(w application.PublishWork) error {
		return w.Outbox.MarkPublished(ctx, id)
	})

	var (
		status      string
		publishedAt *time.Time
		lastError   *string
		attempts    int
	)
	err := pool.QueryRow(ctx, `
SELECT status, published_at, last_error, attempts FROM accounts.outbox WHERE id = $1`, id).
		Scan(&status, &publishedAt, &lastError, &attempts)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "published" {
		t.Errorf("status = %q, want published", status)
	}
	if publishedAt == nil {
		t.Error("published_at is NULL; the purge of §13.2 keys off it")
	}
	if lastError != nil {
		t.Errorf("last_error = %q, want NULL — a published row with a stale error "+
			"reads as a problem during an incident", *lastError)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 left alone", attempts)
	}
}

// D8, at the SQL level: a transient failure moves available_at and bumps
// attempts, and the row is still pending. There is no status change here, and
// that absence is the decision.
func TestReschedule_BumpsAttemptsAndLeavesTheRowPending(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	id := seed(t, pool, row{Aggregate: "ada", Attempts: 4})

	availableAt := time.Now().UTC().Add(16 * time.Second).Truncate(time.Millisecond)
	inTx(t, pool, func(w application.PublishWork) error {
		return w.Outbox.Reschedule(ctx, id, availableAt, "NOT_ENOUGH_REPLICAS")
	})

	var (
		status    string
		attempts  int
		available time.Time
		lastError string
	)
	err := pool.QueryRow(ctx, `
SELECT status, attempts, available_at, last_error FROM accounts.outbox WHERE id = $1`, id).
		Scan(&status, &attempts, &available, &lastError)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "pending" {
		t.Errorf("status = %q, want pending — only a permanent error reaches failed (D8)", status)
	}
	if attempts != 5 {
		t.Errorf("attempts = %d, want 5", attempts)
	}
	if !available.Equal(availableAt) {
		t.Errorf("available_at = %v, want %v", available, availableAt)
	}
	if lastError != "NOT_ENOUGH_REPLICAS" {
		t.Errorf("last_error = %q, want the reason", lastError)
	}
}

func TestMarkFailed_ParksTheRowWithoutBurningAnAttempt(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	id := seed(t, pool, row{Aggregate: "ada", Attempts: 2})

	inTx(t, pool, func(w application.PublishWork) error {
		return w.Outbox.MarkFailed(ctx, id, "RECORD_LIST_TOO_LARGE")
	})

	var (
		status    string
		attempts  int
		lastError string
	)
	err := pool.QueryRow(ctx, `
SELECT status, attempts, last_error FROM accounts.outbox WHERE id = $1`, id).
		Scan(&status, &attempts, &lastError)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "failed" {
		t.Errorf("status = %q, want failed", status)
	}
	// attempts drives backoff, and a failed row is never retried, so there is
	// nothing for another attempt to schedule. Spec §12.1's permanent row says
	// "status = 'failed', record last_error" and nothing about attempts.
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 left alone", attempts)
	}
	if lastError != "RECORD_LIST_TOO_LARGE" {
		t.Errorf("last_error = %q, want the reason a human will read", lastError)
	}
}

// A mark that hits no row means the pass is publishing messages it cannot
// account for. Better a failed transaction than a committed lie.
func TestMarkPublished_FailsWhenTheRowIsGone(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	err := NewPublishUnitOfWork(pool).Do(ctx, func(w application.PublishWork) error {
		return w.Outbox.MarkPublished(ctx, 999_999)
	})
	if err == nil {
		t.Fatal("MarkPublished on a nonexistent row succeeded")
	}
}

// The pass line of §13.3, from one query.
func TestOutboxStatsReader_CountsTheTableOnThePool(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	seed(t, pool, row{Aggregate: "ada", OccurredAgo: 5 * time.Second}) // oldest pending
	seed(t, pool, row{Aggregate: "grace"})
	seed(t, pool, row{Aggregate: "alan", Attempts: 12, AvailableIn: time.Hour,
		OccurredAgo: 2 * time.Second}) // stuck, still retrying
	seed(t, pool, row{Aggregate: "linus", Status: "failed", Attempts: 1})
	seed(t, pool, row{Aggregate: "ken", Status: "published"})

	// On the pool, with no transaction in sight — which is the point of the type
	// and the reason a failure here can no longer roll back a published batch.
	stats, err := NewOutboxStatsReader(pool).Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if stats.Backlog != 3 {
		t.Errorf("backlog = %d, want 3 — every pending row, due or not", stats.Backlog)
	}
	if stats.FailedRows != 1 {
		t.Errorf("failed_rows = %d, want 1", stats.FailedRows)
	}
	// attempts 12 >= maxAttempts 10, and the row is still pending: counted, and
	// still retrying. That is what makes RELAY_MAX_ATTEMPTS an alert threshold.
	if stats.StuckRows != 1 {
		t.Errorf("stuck_rows = %d, want 1", stats.StuckRows)
	}
	// The oldest *pending* row occurred 5s ago. Published and failed rows must
	// not count, or a long-published row would make a healthy relay look stuck.
	if stats.OldestPendingAge < 4*time.Second || stats.OldestPendingAge > 30*time.Second {
		t.Errorf("oldest_pending_age = %v, want about 5s", stats.OldestPendingAge)
	}
}

// An empty table must not report an age of "the epoch". min() over no rows is
// NULL, and a NULL that scanned as a zero time would produce an
// oldest_pending_age of fifty-odd years on every idle pass.
func TestOutboxStatsReader_AnEmptyOutboxHasNoOldestRow(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	stats, err := NewOutboxStatsReader(pool).Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if stats.Backlog != 0 || stats.OldestPendingAge != 0 {
		t.Errorf("stats = %+v, want a zeroed backlog and age", stats)
	}
}

// Spec §13.2's delete, and what it must leave alone.
func TestPurgePublished_DeletesOnlyPublishedRowsPastRetention(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	old := seed(t, pool, row{Aggregate: "ada", Status: "published", PublishedAgo: 48 * time.Hour})
	recent := seed(t, pool, row{Aggregate: "grace", Status: "published"})
	parked := seed(t, pool, row{Aggregate: "alan", Status: "failed", Attempts: 3})
	waiting := seed(t, pool, row{Aggregate: "linus"})

	deleted, err := NewOutboxPurger(pool).
		PurgePublished(ctx, 24*time.Hour, 1000)
	if err != nil {
		t.Fatalf("PurgePublished: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}

	// The row past retention is the one that had to go. A count of 1 alone does
	// not say *which* row was deleted, and deleting the wrong one would satisfy
	// it just as well.
	var stillThere bool
	if err := pool.QueryRow(ctx,
		`SELECT exists(SELECT 1 FROM accounts.outbox WHERE id = $1)`, old).Scan(&stillThere); err != nil {
		t.Fatalf("exists %d: %v", old, err)
	}
	if stillThere {
		t.Errorf("row %d survived; it was published 48h ago against a 24h retention", old)
	}

	for _, survivor := range []struct {
		id  int64
		why string
	}{
		{recent, "published inside the retention window"},
		{parked, "failed rows are never purged automatically"},
		{waiting, "still pending"},
	} {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT exists(SELECT 1 FROM accounts.outbox WHERE id = $1)`, survivor.id).Scan(&exists); err != nil {
			t.Fatalf("exists %d: %v", survivor.id, err)
		}
		if !exists {
			t.Errorf("row %d was deleted but should not have been: %s", survivor.id, survivor.why)
		}
	}
}

// The bound is what makes the purge safe, so it is asserted rather than assumed.
func TestPurgePublished_RespectsTheBatchLimit(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c", "d", "e"} {
		seed(t, pool, row{Aggregate: name, Status: "published", PublishedAgo: 48 * time.Hour})
	}

	// Only the bound, and only once: what needs a database here is that LIMIT
	// reaches the DELETE at all. That the caller repeats until a page comes back
	// short is the use case's property and is already asserted against a fake in
	// TestPurgePublished_RepeatsUntilAShortPage — re-implementing the loop here
	// would put the same logic in two places and give this test a second,
	// unrelated way to fail.
	deleted, err := NewOutboxPurger(pool).PurgePublished(ctx, 24*time.Hour, 2)
	if err != nil {
		t.Fatalf("PurgePublished: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted = %d, want 2 — the limit is not being applied, and an "+
			"unbounded delete is the long lock §13.2 exists to avoid", deleted)
	}
	var left int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM accounts.outbox`).Scan(&left); err != nil {
		t.Fatalf("count: %v", err)
	}
	if left != 3 {
		t.Errorf("rows left = %d, want 3", left)
	}
}

// --- helpers -----------------------------------------------------------------

// row is what a seeded outbox row differs from the ordinary case by. Its zero
// value is the ordinary case: pending, available now, occurred a second ago.
//
// An options struct rather than six positional parameters, because
// seed(t, pool, "alan", "pending", 12, time.Hour, 2*time.Second) is unreadable at
// the call site and its two adjacent durations can be swapped without the
// compiler noticing.
type row struct {
	Aggregate    string
	Status       string // "" means pending
	Attempts     int
	AvailableIn  time.Duration // relative to now; negative means already due
	OccurredAgo  time.Duration // 0 means one second ago
	PublishedAgo time.Duration // only meaningful with Status "published"
}

// seed inserts one outbox row and returns its id.
//
// The content columns come from envelope(), the same helper the producer's own
// tests use, so there is one definition of what a valid row looks like. The
// relay's columns — status, attempts, available_at, published_at — are written
// directly, which nothing in production does because the relay owns them; that is
// the point, since it lets a test reach a state that would otherwise take a
// broker outage.
func seed(t *testing.T, pool *pgxpool.Pool, spec row) int64 {
	t.Helper()
	env := envelope(t, spec.Aggregate,
		`{"specversion":"1.0","data":{"user_id":"`+spec.Aggregate+`"}}`)
	// envelope() spells one correlation_id for every row it builds. Per-aggregate
	// here, so that asserting a claimed row's headers proves the relay read
	// *that* row's rather than any row's.
	env.Headers = map[string]string{"correlation_id": "corr-" + spec.Aggregate}

	status := spec.Status
	if status == "" {
		status = "pending"
	}
	occurredAgo := spec.OccurredAgo
	if occurredAgo == 0 {
		occurredAgo = time.Second
	}
	var publishedAt any
	if spec.Status == "published" {
		publishedAt = time.Now().Add(-spec.PublishedAgo)
	}

	var id int64
	err := pool.QueryRow(context.Background(), `
INSERT INTO accounts.outbox
    (event_id, aggregate_type, aggregate_id, event_type, schema_version,
     payload, headers, occurred_at, status, attempts, available_at, published_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now() - $8::interval, $9, $10,
        now() + $11::interval, $12)
RETURNING id`,
		env.EventID, env.AggregateType, env.AggregateID, env.EventType,
		env.SchemaVersion, env.Payload, env.Headers,
		occurredAgo, status, spec.Attempts, spec.AvailableIn, publishedAt,
	).Scan(&id)
	if err != nil {
		t.Fatalf("seed %s: %v", spec.Aggregate, err)
	}
	return id
}

// due is the ordinary case, named for how often it is the only thing a test wants.
func due(t *testing.T, pool *pgxpool.Pool, aggregateID string) int64 {
	t.Helper()
	return seed(t, pool, row{Aggregate: aggregateID})
}

func claimedIDs(claimed []application.PendingMessage) []int64 {
	out := make([]int64, 0, len(claimed))
	for _, c := range claimed {
		out = append(out, c.ID)
	}
	return out
}

// inTx runs fn in a relay transaction and fails the test if it does not commit.
func inTx(t *testing.T, pool *pgxpool.Pool, fn func(application.PublishWork) error) {
	t.Helper()
	if err := NewPublishUnitOfWork(pool).Do(context.Background(), fn); err != nil {
		t.Fatalf("transaction: %v", err)
	}
}

// claimIn claims in a transaction that is immediately rolled back, so the caller
// can ask "what is claimable right now" without keeping the rows.
func claimIn(t *testing.T, pool *pgxpool.Pool, limit int) []int64 {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := NewRelayOutbox(tx).ClaimPending(ctx, limit)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return claimedIDs(rows)
}
