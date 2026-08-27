package application

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

const testTopic = "accounts.user.v1"

func TestPublishPendingBatch_PublishesEveryClaimedRowInIDOrder(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0), row(2, 0), row(3, 0)}, 100)

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if res.Claimed != 3 || res.Published != 3 {
		t.Errorf("claimed=%d published=%d, want 3 and 3", res.Claimed, res.Published)
	}
	if len(res.Transient) != 0 || len(res.Permanent) != 0 {
		t.Errorf("failures = %v / %v, want none", res.Transient, res.Permanent)
	}

	// id is the ordering authority (spec §8.1) and per-aggregate ordering is a
	// stated guarantee (§12.4). A pass that publishes out of order forfeits it
	// while every count still looks right, so order is what this asserts.
	want := []string{row(1, 0).EventID.String(), row(2, 0).EventID.String(), row(3, 0).EventID.String()}
	if got := f.publisher.eventIDs(); !slices.Equal(got, want) {
		t.Errorf("publish order = %v, want %v", got, want)
	}

	// Every mark follows its own publish, and nothing else touches the
	// transaction — the stats read is not on it.
	wantOps := []string{"claim", "published", "published", "published"}
	if got := f.outbox.ops(); !slices.Equal(got, wantOps) {
		t.Errorf("calls = %v, want %v", got, wantOps)
	}
	if f.uow.commits != 1 {
		t.Errorf("commits = %d, want 1", f.uow.commits)
	}
}

// Spec §9.2's header table, asserted where it is composed. This is the reason the
// contract headers are built in the application layer and not in the Kafka
// adapter: the set a consumer can rely on is checkable without a broker.
func TestPublishPendingBatch_ComposesTheHeadersOfTheKafkaMapping(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0)}, 100)

	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	msg := f.publisher.published[0]
	want := map[string]string{
		messaging.HeaderEventID:       row(1, 0).EventID.String(),
		messaging.HeaderEventType:     "com.outboxexpress.accounts.user.registered",
		messaging.HeaderSpecVersion:   "1.0",
		messaging.HeaderSchemaVersion: "1",
		messaging.HeaderContentType:   "application/json",
		messaging.HeaderCorrelationID: "corr-1", // carried from the row
	}
	for key, wantValue := range want {
		if got := msg.Headers[key]; got != wantValue {
			t.Errorf("header %s = %q, want %q", key, got, wantValue)
		}
	}
	if len(msg.Headers) != len(want) {
		t.Errorf("headers = %v, want exactly %d entries", msg.Headers, len(want))
	}

	// The rest of the mapping: key is the partition key that buys ordering, and
	// the payload goes through untouched because the relay is a dumb pipe.
	if msg.Topic != testTopic {
		t.Errorf("topic = %q, want %q", msg.Topic, testTopic)
	}
	if msg.Key != "user-1" {
		t.Errorf("key = %q, want the aggregate id", msg.Key)
	}
	if string(msg.Payload) != `{"specversion":"1.0","id":"x"}` {
		t.Errorf("payload = %q, want the row's bytes verbatim", msg.Payload)
	}
}

// A derived header must win over a stored one. Spec §8.1 says only
// correlation_id and traceparent are stored, but a contract that depends on that
// staying true for the lifetime of the table is a contract with a hostage.
func TestPublishPendingBatch_AStoredHeaderCannotShadowAContractHeader(t *testing.T) {
	poisoned := row(1, 0)
	poisoned.Headers = map[string]string{messaging.HeaderEventID: "not-the-event-id"}
	f := newPass(t, []PendingMessage{poisoned}, 100)

	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := f.publisher.published[0].Headers[messaging.HeaderEventID]; got != poisoned.EventID.String() {
		t.Errorf("ce_id = %q, want the row's event_id %q", got, poisoned.EventID)
	}
}

// Spec §12.1: a transient error is "not this message's fault". Break the batch,
// because every remaining row is about to fail the same way and trying them all
// would burn a whole batch's attempts on one outage.
func TestPublishPendingBatch_ATransientFailureStopsTheBatchAndReschedules(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0), row(2, 3), row(3, 0)}, 100)
	f.publisher.errByEventID[row(2, 3).EventID.String()] =
		fmt.Errorf("%w: NOT_ENOUGH_REPLICAS", ErrTransient)

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if res.Claimed != 3 || res.Published != 1 {
		t.Errorf("claimed=%d published=%d, want 3 and 1", res.Claimed, res.Published)
	}
	if len(res.Transient) != 1 || res.Transient[0].OutboxID != 2 {
		t.Fatalf("transient = %v, want one failure on row 2", res.Transient)
	}

	// Row 3 was claimed and never attempted: that is the batch breaking.
	if got := f.publisher.eventIDs(); !slices.Equal(got, []string{row(1, 0).EventID.String()}) {
		t.Errorf("published = %v, want only row 1 — row 3 must not be attempted", got)
	}

	// available_at = the pass's start plus whatever the schedule said, and the
	// schedule is consulted with the attempts the row had *before* this failure.
	// That off-by-one is the whole reason RELAY_BACKOFF_BASE means what its name
	// says, and it is this layer's to get right — the arithmetic itself is
	// asserted in platform/backoff.
	if got := f.schedule.askedFor; !slices.Equal(got, []int{3}) {
		t.Errorf("schedule asked about attempts %v, want [3] — the pre-failure count", got)
	}
	reschedules := f.outbox.callsOf("reschedule")
	if len(reschedules) != 1 {
		t.Fatalf("reschedule calls = %d, want 1", len(reschedules))
	}
	if want := f.now.Add(8 * time.Second); !reschedules[0].availableAt.Equal(want) {
		t.Errorf("available_at = %v, want %v", reschedules[0].availableAt, want)
	}
	if reschedules[0].lastError == "" {
		t.Error("last_error is empty; the row would be parked with no reason to read")
	}

	// The pass still commits. Rolling back would discard row 1's mark and
	// republish an event that Kafka already durably has.
	if f.uow.commits != 1 {
		t.Errorf("commits = %d, want 1 — a transient failure is not a failed pass", f.uow.commits)
	}
	// And stats are still read, so a stalling relay is still visible.
	if f.stats.reads != 1 {
		t.Error("stats were not read on a pass that broke early")
	}
}

// Spec §12.1: a permanent error is this message's fault. Park it and keep
// draining — one poison message must not stop the queue.
func TestPublishPendingBatch_APermanentFailureParksTheRowAndKeepsGoing(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0), row(2, 0), row(3, 0)}, 100)
	f.publisher.errByEventID[row(2, 0).EventID.String()] =
		fmt.Errorf("%w: RECORD_LIST_TOO_LARGE", ErrPermanent)

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if res.Published != 2 {
		t.Errorf("published = %d, want 2 — the batch must not stop", res.Published)
	}
	if len(res.Permanent) != 1 || res.Permanent[0].OutboxID != 2 {
		t.Fatalf("permanent = %v, want one failure on row 2", res.Permanent)
	}
	// The worker raises the alert §12.1 asks for, so the result has to carry
	// enough to raise it with: which row, which event, and why.
	if res.Permanent[0].EventID != row(2, 0).EventID {
		t.Errorf("failure event id = %v, want %v", res.Permanent[0].EventID, row(2, 0).EventID)
	}
	if res.Permanent[0].Reason == "" {
		t.Error("failure reason is empty; the alert would say nothing")
	}

	wantOps := []string{"claim", "published", "failed", "published"}
	if got := f.outbox.ops(); !slices.Equal(got, wantOps) {
		t.Errorf("calls = %v, want %v", got, wantOps)
	}
}

// D8, and the headline property of §12.1: an hour-long broker outage must not
// push a backlog to 'failed'. attempts drives backoff and nothing else, so a row
// with attempts far past RELAY_MAX_ATTEMPTS is still rescheduled, never parked.
func TestPublishPendingBatch_ATransientFailureNeverReachesFailed(t *testing.T) {
	for _, attempts := range []int{0, 9, 10, 11, 500} {
		f := newPass(t, []PendingMessage{row(1, attempts)}, 100)
		f.publisher.errByEventID[row(1, attempts).EventID.String()] =
			fmt.Errorf("%w: LEADER_NOT_AVAILABLE", ErrTransient)

		if _, err := f.uc.Execute(context.Background()); err != nil {
			t.Fatalf("attempts=%d: Execute: %v", attempts, err)
		}
		if failed := f.outbox.callsOf("failed"); len(failed) != 0 {
			t.Errorf("attempts=%d: MarkFailed called %d times; RELAY_MAX_ATTEMPTS is an "+
				"alert threshold, not a state transition", attempts, len(failed))
		}
		if len(f.outbox.callsOf("reschedule")) != 1 {
			t.Errorf("attempts=%d: the row was not rescheduled", attempts)
		}
	}
}

// Spec §9.2 says an unmapped aggregate type is permanent "so the branch is
// reachable". This is that branch, reached through the use case: the row parks
// and the pass carries on.
func TestPublishPendingBatch_AnUnmappedAggregateTypeParksTheRow(t *testing.T) {
	unroutable := row(2, 0)
	unroutable.AggregateType = "Invoice"
	f := newPass(t, []PendingMessage{row(1, 0), unroutable, row(3, 0)}, 100)

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Published != 2 || len(res.Permanent) != 1 {
		t.Errorf("published=%d permanent=%d, want 2 and 1", res.Published, len(res.Permanent))
	}
	if len(f.publisher.published) != 2 {
		t.Errorf("the unroutable row reached the publisher; routing must fail before produce")
	}
}

// An adapter whose error wraps neither class has a bug. The safe response is to
// abandon the pass: the transaction rolls back, every row stays pending, and
// nothing has been marked on a guess. Marking on an unclassified error is how a
// message gets lost.
func TestPublishPendingBatch_AnUnclassifiedErrorAbandonsThePass(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0), row(2, 0)}, 100)
	boom := errors.New("an adapter that forgot to classify")
	f.publisher.errByEventID[row(1, 0).EventID.String()] = boom

	res, err := f.uc.Execute(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the adapter's error", err)
	}
	// The result still describes what happened. It has to: on a rolled-back pass
	// Published is the count of messages the broker durably accepted and whose
	// marks were just undone, which is the number of duplicates the next pass
	// will produce. Here the first row failed before any ack, so it is zero — and
	// the claim is still reported, because the rows were claimed.
	if res.Claimed != 2 || res.Published != 0 {
		t.Errorf("claimed=%d published=%d, want 2 and 0", res.Claimed, res.Published)
	}
	if res.Stats != (OutboxStats{}) || res.StatsErr != nil {
		t.Errorf("stats = %+v / %v, want neither on a pass that rolled back",
			res.Stats, res.StatsErr)
	}
	if f.uow.commits != 0 || f.uow.rollbacks != 1 {
		t.Errorf("commits=%d rollbacks=%d, want 0 and 1", f.uow.commits, f.uow.rollbacks)
	}
	for _, op := range f.outbox.ops() {
		if op == "published" || op == "failed" || op == "reschedule" {
			t.Errorf("the pass called %s before giving up; nothing may be marked on an "+
				"error it does not understand", op)
		}
	}
}

// Every field spec §13.3 asks the relay to log comes out of the result struct
// rather than a counter incremented mid-loop (§6.1). This is what makes each one
// assertable at all.
func TestPublishPendingBatch_ReportsTheStatsOfThePass(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0)}, 100)
	f.stats.stats = OutboxStats{
		Backlog:          42,
		OldestPendingAge: 1500 * time.Millisecond,
		FailedRows:       2,
		StuckRows:        1,
	}

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Stats != f.stats.stats {
		t.Errorf("stats = %+v, want %+v", res.Stats, f.stats.stats)
	}
	if res.StatsErr != nil {
		t.Errorf("StatsErr = %v, want nil", res.StatsErr)
	}
}

// An empty outbox is the common case and must still be cheap and still be
// observable: no publish, one stats read, one commit.
func TestPublishPendingBatch_AnEmptyOutboxStillReportsTheBacklog(t *testing.T) {
	f := newPass(t, nil, 100)
	f.stats.stats = OutboxStats{Backlog: 0, FailedRows: 3}

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Claimed != 0 || res.Published != 0 {
		t.Errorf("claimed=%d published=%d, want 0 and 0", res.Claimed, res.Published)
	}
	if res.Stats.FailedRows != 3 {
		t.Errorf("failed_rows = %d, want 3 — an idle relay is exactly when a parked "+
			"row needs to still be visible", res.Stats.FailedRows)
	}
	if got := f.outbox.ops(); !slices.Equal(got, []string{"claim"}) {
		t.Errorf("calls = %v, want a claim and nothing else", got)
	}
}

// Full is what the worker uses to decide not to sleep, and it takes no argument:
// Execute stamps the batch size it actually used, so the loop cannot be
// configured to disagree with the pass about what full means.
func TestPublishResult_FullMeansThereIsCertainlyMore(t *testing.T) {
	if !(PublishResult{Claimed: 100, BatchSize: 100}).Full() {
		t.Error("a batch that came back full must report Full")
	}
	if (PublishResult{Claimed: 99, BatchSize: 100}).Full() {
		t.Error("a batch with room left must not report Full")
	}
	// A zero batch size means nothing was asked for, which is not "full".
	if (PublishResult{}).Full() {
		t.Error("a zeroed result must not report Full")
	}
}

// Execute stamps it, so a result handed to the worker always carries it.
func TestPublishPendingBatch_StampsTheBatchSizeItUsed(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0)}, 100)

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", res.BatchSize)
	}
}

// The headline property of moving the stats read off the transaction: counting
// rows can no longer undo delivery.
//
// This test used to assert the opposite, and the argument for it was real — a
// pass that reports a zero backlog it never read is indistinguishable from a
// healthy idle relay. But the price was that a statement_timeout on the one query
// whose cost grows with the backlog would abort the transaction, and the commit
// that marks a durably-acked batch would come back as a rollback. Every message
// in that batch published twice, because a number could not be read. §13.1 draws
// this line for the wakeup: "an optimisation, never the delivery path."
//
// The observability concern is answered without paying that: the result carries
// the error, and the pass line says so instead of reporting zeroes.
func TestPublishPendingBatch_AStatsFailureCannotUndoDelivery(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0), row(2, 0)}, 100)
	boom := errors.New("canceling statement due to statement timeout")
	f.stats.err = boom

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute = %v; the delivery half of this pass succeeded", err)
	}
	if f.uow.commits != 1 {
		t.Errorf("commits = %d, want 1 — the marks for two acked messages must "+
			"survive a failure to count rows", f.uow.commits)
	}
	if res.Published != 2 {
		t.Errorf("published = %d, want 2", res.Published)
	}
	if !errors.Is(res.StatsErr, boom) {
		t.Errorf("StatsErr = %v, want the reader's error — the pass line has to be "+
			"able to say the backlog is unknown rather than report it as zero", res.StatsErr)
	}
	if res.Stats != (OutboxStats{}) {
		t.Errorf("stats = %+v, want the zero value alongside StatsErr", res.Stats)
	}
}

// Stats are read after the commit, not inside it. Read inside, they described a
// state that had not happened yet and might never — a backlog that excluded rows
// the pass had marked, in a pass that then rolled back.
func TestPublishPendingBatch_StatsAreReadAfterTheCommit(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0)}, 100)
	f.uow.beforeCommit = func() {
		if f.stats.reads != 0 {
			t.Errorf("the stats read happened inside the transaction (%d reads before "+
				"commit); it must not be able to abort it", f.stats.reads)
		}
	}

	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if f.stats.reads != 1 {
		t.Errorf("stats reads = %d, want 1 after the commit", f.stats.reads)
	}
}

// A pass that rolls back must not read stats at all: there is nothing to report
// and the numbers would describe a table the pass just stopped changing.
func TestPublishPendingBatch_ARolledBackPassReadsNoStats(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0)}, 100)
	f.publisher.errByEventID[row(1, 0).EventID.String()] = errors.New("unclassified")

	if _, err := f.uc.Execute(context.Background()); err == nil {
		t.Fatal("Execute returned nil on an unclassified error")
	}
	if f.stats.reads != 0 {
		t.Errorf("stats reads = %d, want 0 on a pass that rolled back", f.stats.reads)
	}
}

// §12.1 is literal: "available_at = now() + backoff(attempts)". now() is the
// moment of the failure, not the moment the pass began, and the two differ by
// however long the pass has been running. A transient publish failure is usually
// a produce that timed out, so the gap is the whole request timeout — measuring
// from the pass's start would subtract that from every backoff and can write an
// available_at that is already in the past by the time it is committed.
//
// A fixed clock cannot catch this, which is why the produce here takes time.
func TestPublishPendingBatch_TheBackoffRunsFromTheFailureNotThePassStart(t *testing.T) {
	const requestTimeout = 10 * time.Second

	f := newPass(t, []PendingMessage{row(1, 3)}, 100)
	// What a broker outage looks like from in here: the produce holds for the
	// request timeout and then fails transiently.
	f.publisher.takes = func() { f.clock.advance(requestTimeout) }
	f.publisher.errByEventID[row(1, 3).EventID.String()] =
		fmt.Errorf("%w: REQUEST_TIMED_OUT", ErrTransient)

	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	reschedules := f.outbox.callsOf("reschedule")
	if len(reschedules) != 1 {
		t.Fatalf("reschedule calls = %d, want 1", len(reschedules))
	}
	want := f.now.Add(requestTimeout + 8*time.Second)
	if got := reschedules[0].availableAt; !got.Equal(want) {
		t.Errorf("available_at = %v, want %v — the ten seconds the produce spent "+
			"failing must not be deducted from the backoff", got, want)
	}
	// And the row must not come back due immediately, which is what the earlier
	// reading would have produced: start + 8s is two seconds in the past by now.
	if !reschedules[0].availableAt.After(f.clock.Now()) {
		t.Errorf("available_at = %v is not in the future at %v; the row is due again "+
			"at once and the backoff bought nothing", reschedules[0].availableAt, f.clock.Now())
	}
	// batch_ms is the transaction's duration, so it sees those ten seconds too.
	if res.Duration != requestTimeout {
		t.Errorf("batch_ms source = %v, want %v", res.Duration, requestTimeout)
	}
}

// A pass can fail after the broker has durably accepted some of its messages —
// a mark that fails, a commit that fails, a SIGTERM past the drain. Those rows go
// back to pending and will be published again, and Published is the only place
// that count exists. Returning a zeroed result would make the one moment the
// relay knowingly creates duplicates the one moment it reports nothing.
func TestPublishPendingBatch_AFailedPassStillReportsWhatItPublished(t *testing.T) {
	f := newPass(t, []PendingMessage{row(1, 0), row(2, 0), row(3, 0)}, 100)
	// Rows 1 and 2 are acked; row 3's error is unclassified, which abandons the
	// pass and rolls both marks back.
	f.publisher.errByEventID[row(3, 0).EventID.String()] = errors.New("unclassified")

	res, err := f.uc.Execute(context.Background())
	if err == nil {
		t.Fatal("Execute returned nil on an unclassified error")
	}
	if res.Published != 2 {
		t.Errorf("published = %d, want 2 — two messages are on the broker and their "+
			"marks just rolled back; that is the duplicate count and nothing else "+
			"records it", res.Published)
	}
	if res.Claimed != 3 {
		t.Errorf("claimed = %d, want 3", res.Claimed)
	}
	if f.uow.rollbacks != 1 {
		t.Errorf("rollbacks = %d, want 1", f.uow.rollbacks)
	}
}

// row builds one claimed outbox row. Its event id is derived from its outbox id
// so that a failing assertion names something a reader can find.
func row(id int64, attempts int) PendingMessage {
	return PendingMessage{
		ID:       id,
		Attempts: attempts,
		Envelope: Envelope{
			EventID:       uuid.MustParse(fmt.Sprintf("00000000-0000-7000-8000-%012d", id)),
			AggregateType: "User",
			AggregateID:   "user-" + strconv.FormatInt(id, 10),
			EventType:     "com.outboxexpress.accounts.user.registered",
			SchemaVersion: 1,
			Payload:       []byte(`{"specversion":"1.0","id":"x"}`),
			Headers:       map[string]string{messaging.HeaderCorrelationID: "corr-" + strconv.FormatInt(id, 10)},
			OccurredAt:    time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
		},
	}
}

type passFixture struct {
	uc        *PublishPendingBatch
	outbox    *fakeOutbox
	stats     *fakeStatsReader
	uow       *fakePublishUOW
	publisher *fakePublisher
	schedule  *fakeSchedule
	clock     *movableClock
	now       time.Time
}

func newPass(t *testing.T, rows []PendingMessage, batchSize int) *passFixture {
	t.Helper()
	now := time.Date(2026, 8, 26, 10, 20, 0, 0, time.UTC)
	outbox := &fakeOutbox{pending: rows}
	uow := &fakePublishUOW{outbox: outbox}
	publisher := &fakePublisher{errByEventID: map[string]error{}}
	schedule := &fakeSchedule{delay: 8 * time.Second}
	clk := &movableClock{now: now}
	stats := &fakeStatsReader{}

	uc := NewPublishPendingBatch(uow, publisher, stats, Topics{"User": testTopic}, clk,
		PublishPolicy{
			BatchSize:   batchSize,
			Schedule:    schedule,
			MaxAttempts: 10,
		})
	return &passFixture{
		uc: uc, outbox: outbox, stats: stats, uow: uow,
		publisher: publisher, schedule: schedule, clock: clk, now: now,
	}
}
