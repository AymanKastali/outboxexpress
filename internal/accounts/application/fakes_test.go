package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// fakeUOW runs fn immediately and records what happened. It models the two
// outcomes the use case can distinguish: fn failed, or the transaction
// committed.
type fakeUOW struct {
	calls     int
	committed bool
	lastMeta  Metadata
	repo      *fakeUserRepo
	commitErr error
}

func (f *fakeUOW) Do(ctx context.Context, meta Metadata, fn func(Work) error) error {
	f.calls++
	f.lastMeta = meta
	if err := fn(Work{Users: f.repo}); err != nil {
		return err
	}
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = true
	return nil
}

type fakeUserRepo struct {
	saved []*domain.User
	err   error
}

func (f *fakeUserRepo) Save(ctx context.Context, u *domain.User) error {
	if f.err != nil {
		return f.err
	}
	f.saved = append(f.saved, u)
	return nil
}

type fakeWakeup struct{ calls int }

func (f *fakeWakeup) Notify(ctx context.Context) { f.calls++ }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// movableClock is a clock a test can advance, so that "the pass took ten seconds"
// is expressible. fixedClock stays for the tests where nothing may move.
//
// A pointer receiver, because advancing it has to be visible to the use case
// holding it — a value receiver would give the use case a copy and the test a
// silent no-op.
type movableClock struct{ now time.Time }

func (c *movableClock) Now() time.Time { return c.now }

func (c *movableClock) advance(d time.Duration) { c.now = c.now.Add(d) }

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

var errIDs = errors.New("no entropy")

// --- the relay's fakes -------------------------------------------------------

// outboxCall records one call so a test can assert on the *sequence* of writes,
// not just the final state. The order matters here more than usual: publishing
// out of id order breaks the per-aggregate ordering guarantee of §12.4, and a
// mark that lands before its ack would be the lost-message bug of §7.
type outboxCall struct {
	op          string // "claim", "published", "reschedule", "failed"
	id          int64
	availableAt time.Time
	lastError   string
}

type fakeOutbox struct {
	pending []PendingMessage
	calls   []outboxCall
}

func (f *fakeOutbox) ClaimPending(_ context.Context, limit int) ([]PendingMessage, error) {
	f.calls = append(f.calls, outboxCall{op: "claim"})
	if limit < len(f.pending) {
		return f.pending[:limit], nil
	}
	return f.pending, nil
}

func (f *fakeOutbox) MarkPublished(_ context.Context, id int64) error {
	f.calls = append(f.calls, outboxCall{op: "published", id: id})
	return nil
}

func (f *fakeOutbox) Reschedule(_ context.Context, id int64, availableAt time.Time, lastError string) error {
	f.calls = append(f.calls, outboxCall{
		op: "reschedule", id: id, availableAt: availableAt, lastError: lastError})
	return nil
}

func (f *fakeOutbox) MarkFailed(_ context.Context, id int64, lastError string) error {
	f.calls = append(f.calls, outboxCall{op: "failed", id: id, lastError: lastError})
	return nil
}

// ops is the call sequence, for asserting on order.
func (f *fakeOutbox) ops() []string {
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.op)
	}
	return out
}

// callsOf returns every call of one kind, so a test can say "MarkFailed was
// never called" without scanning.
func (f *fakeOutbox) callsOf(op string) []outboxCall {
	var out []outboxCall
	for _, c := range f.calls {
		if c.op == op {
			out = append(out, c)
		}
	}
	return out
}

// fakeStatsReader is separate from fakeOutbox for the same reason the port is
// separate from the repository: it is not on the pass's transaction, so a test
// can fail it without that failure having any way to reach the marks.
type fakeStatsReader struct {
	stats OutboxStats
	err   error
	reads int
}

func (f *fakeStatsReader) Read(_ context.Context, _ int) (OutboxStats, error) {
	f.reads++
	if f.err != nil {
		return OutboxStats{}, f.err
	}
	return f.stats, nil
}

// fakePublishUOW commits by returning nil and rolls back by returning fn's
// error unwrapped — the contract platformpg.WithTx implements, so that a test of
// the use case and the real transaction agree about what an error means.
type fakePublishUOW struct {
	outbox    *fakeOutbox
	commits   int
	rollbacks int

	// beforeCommit runs at the moment fn has returned and the transaction is
	// about to close. It is how a test observes what happened *inside* the
	// boundary, which is the only way to assert that the stats read is outside
	// it — a call count taken afterwards cannot tell the two apart.
	beforeCommit func()
}

func (f *fakePublishUOW) Do(ctx context.Context, fn func(PublishWork) error) error {
	if err := fn(PublishWork{Outbox: f.outbox}); err != nil {
		f.rollbacks++
		return err
	}
	if f.beforeCommit != nil {
		f.beforeCommit()
	}
	f.commits++
	return nil
}

// fakeSchedule returns a constant and records what it was asked about.
//
// The real arithmetic lives in platform/backoff and is asserted there; a pass
// test that used it would be re-asserting §12.1 from the wrong layer. What this
// layer owns is *which* number it passes in — the attempts recorded before this
// failure — so askedFor is the assertable half.
type fakeSchedule struct {
	delay    time.Duration
	askedFor []int
}

func (f *fakeSchedule) After(attempts int) time.Duration {
	f.askedFor = append(f.askedFor, attempts)
	return f.delay
}

// fakePublisher answers per topic-and-key so a test can make exactly one row
// fail. It records what it was asked to publish, which is how the header set of
// §9.2 is asserted without a broker.
type fakePublisher struct {
	errByEventID map[string]error
	published    []messaging.Message

	// takes, when set, runs before each answer. It is how a test says "this
	// produce spent ten seconds before it failed", which is the case that
	// separates a backoff measured from the failure from one measured from the
	// start of the pass.
	takes func()
}

func (f *fakePublisher) Publish(_ context.Context, msg messaging.Message) error {
	if f.takes != nil {
		f.takes()
	}
	if err := f.errByEventID[msg.EventID]; err != nil {
		return err
	}
	f.published = append(f.published, msg)
	return nil
}

// eventIDs is the publish order, which must be id order.
func (f *fakePublisher) eventIDs() []string {
	out := make([]string, 0, len(f.published))
	for _, m := range f.published {
		out = append(out, m.EventID)
	}
	return out
}
