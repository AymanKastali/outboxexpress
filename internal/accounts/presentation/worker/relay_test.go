package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// Spec §6.3: the worker "picks the next sleep from PublishResult". Each branch is
// a decision with a cost if it is wrong, so each one is named.
func TestRelay_NextWait(t *testing.T) {
	failure := application.Failure{OutboxID: 1, EventID: uuid.New(), Reason: "NOT_ENOUGH_REPLICAS"}

	// BatchSize on every case: Execute stamps it, and Full() reads it off the
	// result rather than the policy so the loop cannot disagree with the pass
	// about what a full batch was. A result without it is one the relay never sees.
	tests := []struct {
		name string
		res  application.PublishResult
		want time.Duration
		why  string
	}{
		{
			name: "a full batch",
			res:  application.PublishResult{Claimed: 100, Published: 100, BatchSize: 100},
			want: 0,
			why:  "there is certainly more; sleeping on a full batch is how a backlog stops draining",
		},
		{
			name: "some work, room left",
			res:  application.PublishResult{Claimed: 4, Published: 4, BatchSize: 100},
			want: 50 * time.Millisecond,
			why:  "work is arriving; stay responsive",
		},
		{
			name: "a transient failure",
			res: application.PublishResult{Claimed: 3, Published: 1, BatchSize: 100,
				Transient: []application.Failure{failure}},
			want: 2 * time.Second,
			why:  "the broker is unwell and the rows carry their own available_at; hammering it helps nobody",
		},
		{
			name: "nothing at all",
			res:  application.PublishResult{BatchSize: 100},
			want: 50 * time.Millisecond,
			why:  "start at IdleMin and grow from there",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			relay := newRelay(t, &fakePass{}, &fakeWaiter{}, &bytes.Buffer{})
			if got := relay.nextWait(tc.res); got != tc.want {
				t.Errorf("nextWait = %v, want %v: %s", got, tc.want, tc.why)
			}
		})
	}
}

// An idle relay must not poll at IdleMin forever, and must not jump straight to
// IdleMax either: it doubles, so a quiet system costs little and a busy one is
// still picked up quickly.
func TestRelay_IdleWaitGrowsTowardTheMaximum(t *testing.T) {
	relay := newRelay(t, &fakePass{}, &fakeWaiter{}, &bytes.Buffer{})
	idle := application.PublishResult{}

	want := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		2 * time.Second, // capped
		2 * time.Second,
	}
	for i, expected := range want {
		if got := relay.nextWait(idle); got != expected {
			t.Fatalf("idle wait %d = %v, want %v", i, got, expected)
		}
	}

	// And it resets the moment there is work again, so one busy moment does not
	// have to wait out a backoff earned while the system was quiet.
	if got := relay.nextWait(application.PublishResult{Claimed: 1, Published: 1}); got != 50*time.Millisecond {
		t.Errorf("wait after work = %v, want IdleMin", got)
	}
	if got := relay.nextWait(idle); got != 50*time.Millisecond {
		t.Errorf("idle wait after a reset = %v, want IdleMin", got)
	}
}

// The pass line of spec §13.3, field by field. This test is the table.
func TestRelay_LogsOnePassLineWithEveryFieldOperationsNeeds(t *testing.T) {
	var out bytes.Buffer
	pass := &fakePass{results: []application.PublishResult{{
		Claimed:   7,
		Published: 5,
		Transient: []application.Failure{{OutboxID: 11, EventID: uuid.New(), Reason: "NOT_ENOUGH_REPLICAS"}},
		Permanent: []application.Failure{{OutboxID: 12, EventID: uuid.New(), Reason: "RECORD_LIST_TOO_LARGE"}},
		Stats: application.PassStats{
			Backlog:          42,
			OldestPendingAge: 1500 * time.Millisecond,
			FailedRows:       3,
			StuckRows:        1,
		},
		Duration: 87 * time.Millisecond,
	}}}
	pass.stopAfter = 1

	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel
	// notify_queue_usage is the one field that does not come from the pass: §13.1
	// reads it from the connection that listens, so it arrives through the waiter.
	if err := newRelay(t, pass, &fakeWaiter{usage: 0.11}, &out).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	line := findLine(t, out.String(), "relay pass")
	want := map[string]any{
		"claimed":               float64(7),
		"published":             float64(5),
		"transient_failures":    float64(1),
		"permanent_failures":    float64(1),
		"backlog":               float64(42),
		"oldest_pending_age_ms": float64(1500),
		"failed_rows":           float64(3),
		"stuck_rows":            float64(1),
		"batch_ms":              float64(87),
		"notify_queue_usage":    0.11,
	}
	for field, wantValue := range want {
		got, ok := line[field]
		if !ok {
			t.Errorf("the pass line has no %q; spec §13.3 lists it, and there is no "+
				"metrics system to find it in instead", field)
			continue
		}
		if got != wantValue {
			t.Errorf("%s = %v, want %v", field, got, wantValue)
		}
	}
	if line["level"] != "INFO" {
		t.Errorf("level = %v, want INFO on a pass that did work", line["level"])
	}
}

// A relay that found nothing logs at DEBUG. An idle relay logging at INFO
// several times a second is a relay whose log is useless during the incident it
// exists for.
func TestRelay_AnIdlePassLogsAtDebug(t *testing.T) {
	var out bytes.Buffer
	pass := &fakePass{results: []application.PublishResult{{}}, stopAfter: 1}
	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel

	if err := newRelay(t, pass, &fakeWaiter{}, &out).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if line := findLine(t, out.String(), "relay pass"); line["level"] != "DEBUG" {
		t.Errorf("level = %v, want DEBUG on an idle pass", line["level"])
	}
}

// A wakeup resets the idle ladder: a relay that had backed off to two seconds
// during a quiet night must not make the first registration of the morning wait
// out the rest of that backoff.
func TestRelay_AWakeupResetsTheIdleBackoff(t *testing.T) {
	relay := newRelay(t, &fakePass{}, &fakeWaiter{woken: true}, &bytes.Buffer{})
	idle := application.PublishResult{}

	// Climb the ladder to the cap.
	for range 8 {
		relay.nextWait(idle)
	}
	if relay.idle != 2*time.Second {
		t.Fatalf("idle = %v, want it to have reached IdleMax first", relay.idle)
	}

	// One wait during which the waiter reports a wakeup.
	relay.wait(context.Background(), 10*time.Millisecond)

	if relay.idle != 50*time.Millisecond {
		t.Errorf("idle = %v after a wakeup, want IdleMin — otherwise the first "+
			"registration after a quiet night waits out a backoff it did not earn",
			relay.idle)
	}
}

// With RELAY_USE_NOTIFY=false there is no waiter at all. The loop must still
// sleep, and must not report a notify queue usage of zero for a mechanism it is
// not using — a zero would read as "healthy" on a dashboard.
func TestRelay_WithNoWaiterItStillSleepsAndReportsNoUsage(t *testing.T) {
	var out bytes.Buffer
	pass := &fakePass{results: []application.PublishResult{{Claimed: 1, Published: 1}}, stopAfter: 1}
	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel

	// nil Waiter, which is what wakeupFor returns for RELAY_USE_NOTIFY=false.
	if err := newRelay(t, pass, nil, &out).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	line := findLine(t, out.String(), "relay pass")
	if _, present := line["notify_queue_usage"]; present {
		t.Error("the pass line reports notify_queue_usage with no listener; absent " +
			"is the honest answer and zero is the misleading one")
	}
}

// A waiter that cannot report its own health is not a failure either.
func TestRelay_AWaiterWithNoReadingOmitsTheField(t *testing.T) {
	var out bytes.Buffer
	pass := &fakePass{results: []application.PublishResult{{Claimed: 1}}, stopAfter: 1}
	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel

	if err := newRelay(t, pass, &fakeWaiter{noUsage: true}, &out).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if line := findLine(t, out.String(), "relay pass"); line["notify_queue_usage"] != nil {
		t.Error("a waiter with no reading still put a number on the pass line")
	}
}

// §12.1 asks for an alert on a permanent failure, and §6.1 keeps the use case
// from raising it. So it is raised here, with the ids an operator needs to find
// the row.
func TestRelay_RaisesAnAlertPerParkedRow(t *testing.T) {
	var out bytes.Buffer
	eventID := uuid.New()
	pass := &fakePass{results: []application.PublishResult{{
		Claimed:   1,
		Permanent: []application.Failure{{OutboxID: 99, EventID: eventID, Reason: "RECORD_LIST_TOO_LARGE"}},
	}}}
	pass.stopAfter = 1
	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel

	if err := newRelay(t, pass, &fakeWaiter{}, &out).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logged := out.String()
	if !strings.Contains(logged, `"level":"ERROR"`) {
		t.Error("a parked row produced no ERROR line; nobody would look at it")
	}
	if !strings.Contains(logged, "99") || !strings.Contains(logged, eventID.String()) {
		t.Errorf("the alert names neither the outbox id nor the event id:\n%s", logged)
	}
	if !strings.Contains(logged, "RECORD_LIST_TOO_LARGE") {
		t.Error("the alert does not say why")
	}
}

// §13.1: "pg_notification_queue_usage() is read and logged on every relay pass
// and warned on above 25%." Past that point the queue is filling, and when it
// fills it is POST /users that starts failing.
func TestRelay_WarnsWhenTheNotifyQueueIsFilling(t *testing.T) {
	for _, tc := range []struct {
		usage    float64
		wantWarn bool
	}{
		{usage: 0.10, wantWarn: false},
		{usage: 0.25, wantWarn: false},
		{usage: 0.26, wantWarn: true},
		{usage: 0.90, wantWarn: true},
	} {
		var out bytes.Buffer
		pass := &fakePass{results: []application.PublishResult{{}}, stopAfter: 1}
		ctx, cancel := context.WithCancel(context.Background())
		pass.stop = cancel

		if err := newRelay(t, pass, &fakeWaiter{usage: tc.usage}, &out).Run(ctx); err != nil {
			t.Fatalf("usage=%v: Run: %v", tc.usage, err)
		}
		warned := strings.Contains(out.String(), "notify queue is filling")
		if warned != tc.wantWarn {
			t.Errorf("usage=%v: warned=%v, want %v", tc.usage, warned, tc.wantWarn)
		}
	}
}

// The bug this shape exists to avoid. Listener.Wait returns *immediately* when it
// fails, so a loop that treated its return as "the wait happened" would spin at
// full speed against the database for as long as the listener is broken.
func TestRelay_DoesNotSpinWhenTheWakeupFails(t *testing.T) {
	var out bytes.Buffer
	wake := &fakeWaiter{err: errors.New("listen connection refused")}
	pass := &fakePass{}

	// A real wait, briefly: the point is that the loop takes about as long as it
	// was told to even though the waiter returned at once.
	log := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	relay := NewRelay(pass, wake, func() float64 { return 1 }, log, RelayPolicy{
		IdleMin: 80 * time.Millisecond,
		IdleMax: 80 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := relay.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 300ms of 80ms waits is a handful of passes. A spin would be thousands.
	if passes := pass.calls; passes > 12 {
		t.Errorf("%d passes in 300ms; the loop is spinning because the wakeup "+
			"returned early", passes)
	}
	if !strings.Contains(out.String(), "wakeup") {
		t.Error("the broken wakeup was never mentioned in the log")
	}
}

// A cancelled context is how this loop is asked to stop, not a failure.
func TestRelay_StopsCleanlyWhenTheContextEnds(t *testing.T) {
	var out bytes.Buffer
	pass := &fakePass{results: []application.PublishResult{{}, {}, {}}, stopAfter: 3}
	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel

	if err := newRelay(t, pass, &fakeWaiter{}, &out).Run(ctx); err != nil {
		t.Errorf("Run = %v, want nil on a clean shutdown", err)
	}
	if !strings.Contains(out.String(), "relay stopped") {
		t.Error("the relay stopped without saying so")
	}
}

// A pass that fails has committed nothing, so every row it claimed is still
// pending. The relay is the thing that must not give up: it logs, backs off the
// long way, and tries again.
func TestRelay_KeepsGoingAfterAFailedPass(t *testing.T) {
	var out bytes.Buffer
	wake := &fakeWaiter{}
	pass := &fakePass{err: errors.New("connection reset"), stopAfter: 3}

	ctx, cancel := context.WithCancel(context.Background())
	pass.stop = cancel

	log := slog.New(slog.NewJSONHandler(&out, nil))
	relay := NewRelay(pass, wake, func() float64 { return 1 }, log, RelayPolicy{
		IdleMin: 10 * time.Millisecond,
		IdleMax: 60 * time.Millisecond,
	})
	if err := relay.Run(ctx); err != nil {
		t.Errorf("Run = %v, want nil — a failing pass is not a failing relay", err)
	}
	if pass.calls < 3 {
		t.Errorf("%d passes; the relay gave up after a failure", pass.calls)
	}
	if !strings.Contains(out.String(), `"level":"ERROR"`) {
		t.Error("a failed pass produced no ERROR line")
	}
	// It must back off the long way, not retry at IdleMin against a database
	// that is already unhappy.
	if len(wake.waits) == 0 || wake.waits[0] != 60*time.Millisecond {
		t.Errorf("wait after a failed pass = %v, want IdleMax", wake.waits)
	}
}

// newRelay wires a relay with a jitter factor of 1, so that every expectation in
// this file is an exact duration rather than a range.
func newRelay(t *testing.T, pass *fakePass, wake Waiter, out *bytes.Buffer) *Relay {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return NewRelay(pass, wake, func() float64 { return 1 }, log, RelayPolicy{
		IdleMin: 50 * time.Millisecond,
		IdleMax: 2 * time.Second,
	})
}

// fakePass answers with a script of results and then stops the loop by
// cancelling, so a test never has to sleep to observe a fixed number of passes.
//
// stopAfter is separate from len(results) because the failure tests script no
// results at all and still need the loop to end after a known number of passes.
type fakePass struct {
	results   []application.PublishResult
	err       error
	stopAfter int
	calls     int
	stop      context.CancelFunc
}

// A plain int, not an atomic: Run is called synchronously from the test and every
// read of calls happens after it returns. An atomic here would tell the next
// reader this loop is concurrent, which it is not.
func (f *fakePass) Execute(context.Context) (application.PublishResult, error) {
	f.calls++
	n := f.calls
	if f.stop != nil && n >= f.stopAfter {
		defer f.stop()
	}
	if f.err != nil {
		return application.PublishResult{}, f.err
	}
	if n <= len(f.results) {
		return f.results[n-1], nil
	}
	return application.PublishResult{}, nil
}

// fakeWaiter records what it was asked to wait for and answers instantly, so the
// loop's timing decisions are assertable without any of them elapsing.
type fakeWaiter struct {
	waits   []time.Duration
	woken   bool
	err     error
	usage   float64
	noUsage bool // true to report no reading, as a listener that never connected does
}

func (f *fakeWaiter) Wait(_ context.Context, timeout time.Duration) (bool, error) {
	f.waits = append(f.waits, timeout)
	return f.woken, f.err
}

func (f *fakeWaiter) Usage(context.Context) (float64, bool) {
	return f.usage, !f.noUsage
}

// findLine returns the first JSON log line whose msg matches.
func findLine(t *testing.T, logged, msg string) map[string]any {
	t.Helper()
	for _, raw := range strings.Split(strings.TrimSpace(logged), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", raw, err)
		}
		if line["msg"] == msg {
			return line
		}
	}
	t.Fatalf("no log line with msg=%q in:\n%s", msg, logged)
	return nil
}
