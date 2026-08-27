package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

func TestPurger_LogsWhatItDeleted(t *testing.T) {
	var out bytes.Buffer
	run := &fakePurgeRun{res: application.PurgeResult{Deleted: 2240, Passes: 3}, stopAfter: 1}
	ctx, cancel := context.WithCancel(context.Background())
	run.stop = cancel

	log := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := NewPurger(run, log, time.Millisecond).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logged := out.String()
	if !strings.Contains(logged, "2240") || !strings.Contains(logged, `"passes":3`) {
		t.Errorf("the purge line does not report what it did:\n%s", logged)
	}
}

// A failing purge is not a failing process. Rows that were not deleted are still
// published and still purgeable next tick.
func TestPurger_KeepsGoingAfterAFailure(t *testing.T) {
	var out bytes.Buffer
	// The fake stops the loop on its second call, so this test costs one tick
	// rather than waiting out a fixed deadline (spec §15: no sleeps for
	// synchronisation).
	run := &fakePurgeRun{err: errors.New("connection reset"), stopAfter: 2}
	ctx, cancel := context.WithCancel(context.Background())
	run.stop = cancel

	log := slog.New(slog.NewJSONHandler(&out, nil))
	if err := NewPurger(run, log, time.Millisecond).Run(ctx); err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
	if run.calls < 2 {
		t.Errorf("%d runs; the purger gave up after the first failure", run.calls)
	}
	if !strings.Contains(out.String(), `"level":"ERROR"`) {
		t.Error("a failed purge produced no ERROR line")
	}
}

// The first purge must not wait out a whole interval. With OUTBOX_RETENTION of
// 24h and PURGE_INTERVAL of a minute that would not matter, but a process that
// does nothing for its first tick is a process nobody can smoke-test.
func TestPurger_RunsOnceBeforeTheFirstTick(t *testing.T) {
	run := &fakePurgeRun{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	log := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	if err := NewPurger(run, log, time.Hour).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.calls != 1 {
		t.Errorf("%d runs in 50ms with a one-hour interval, want exactly 1", run.calls)
	}
}

// stopAfter is how these tests end: the fake cancels the context once it has been
// called enough times, so a test observes a known number of runs without waiting
// out a wall-clock deadline. calls is a plain int because Run is called
// synchronously and every read of it happens after Run returns.
type fakePurgeRun struct {
	res       application.PurgeResult
	err       error
	stopAfter int
	calls     int
	stop      context.CancelFunc
}

func (f *fakePurgeRun) Execute(context.Context) (application.PurgeResult, error) {
	f.calls++
	if f.stop != nil && f.calls >= f.stopAfter {
		defer f.stop()
	}
	return f.res, f.err
}
