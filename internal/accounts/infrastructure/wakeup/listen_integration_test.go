//go:build integration

package wakeup_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

// slog.New(slog.DiscardHandler) inline rather than a local helper: it is what the
// three existing tests in this package and in presentation/http already use, and
// a text handler formatting into io.Discard still does the formatting.

// The two halves of §13.1 against one database: the api notifies, the relay is
// woken. This is where the two sides' agreement about the channel name is
// actually checked rather than assumed from a shared constant.
func TestListener_IsWokenByTheNotifier(t *testing.T) {
	dsn, pool := pgtest.Accounts(t)
	ctx := context.Background()

	listener := wakeup.NewListener(dsn, slog.New(slog.DiscardHandler))
	defer func() { _ = listener.Close(ctx) }()

	// LISTEN has to be outstanding before the NOTIFY: notifications are not
	// durable and are not delivered to a listener that was disconnected when
	// they were sent (§13.1). Waiting once with a tiny timeout is how the
	// connection gets established without a sleep.
	if woken, err := listener.Wait(ctx, 50*time.Millisecond); err != nil || woken {
		t.Fatalf("priming Wait: woken=%v err=%v, want false and nil", woken, err)
	}

	wakeup.NewNotifier(pool, slog.New(slog.DiscardHandler)).Notify(ctx)

	woken, err := listener.Wait(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !woken {
		t.Error("Wait reported no wakeup after a NOTIFY on the same channel; the " +
			"relay would only ever find rows by polling")
	}
}

// The ordinary case, and the one that happens on almost every pass of an idle
// relay: nothing arrived. It must be cheap, it must not be an error, and it must
// take about as long as it was asked to.
func TestListener_ReportsNotWokenOnTimeout(t *testing.T) {
	dsn, _ := pgtest.Accounts(t)
	ctx := context.Background()

	listener := wakeup.NewListener(dsn, slog.New(slog.DiscardHandler))
	defer func() { _ = listener.Close(ctx) }()

	start := time.Now()
	woken, err := listener.Wait(ctx, 200*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Wait: %v, want a timeout to be reported as not-woken, not as an error", err)
	}
	if woken {
		t.Error("Wait reported a wakeup nobody sent")
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("Wait returned after %v, want about 200ms — a wait that returns "+
			"early turns the pass loop into a spin", elapsed)
	}
}

// The fact the whole design rests on: a timed-out wait leaves the connection
// listening. If it did not, every idle pass would pay for a reconnect and a
// LISTEN, and a notification sent between the two would be lost.
func TestListener_KeepsListeningAcrossATimeout(t *testing.T) {
	dsn, pool := pgtest.Accounts(t)
	ctx := context.Background()

	listener := wakeup.NewListener(dsn, slog.New(slog.DiscardHandler))
	defer func() { _ = listener.Close(ctx) }()

	for range 3 {
		if woken, err := listener.Wait(ctx, 50*time.Millisecond); err != nil || woken {
			t.Fatalf("idle Wait: woken=%v err=%v", woken, err)
		}
	}

	wakeup.NewNotifier(pool, slog.New(slog.DiscardHandler)).Notify(ctx)

	woken, err := listener.Wait(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !woken {
		t.Error("three timeouts silently dropped the subscription")
	}
}

// Shutdown is not a failure. The relay's context ending mid-wait is how the
// process is asked to stop, and a Wait that reported that as an error would make
// every clean shutdown log a warning.
func TestListener_ACancelledContextIsNotAnError(t *testing.T) {
	dsn, _ := pgtest.Accounts(t)

	listener := wakeup.NewListener(dsn, slog.New(slog.DiscardHandler))
	defer func() { _ = listener.Close(context.Background()) }()

	// Establish the connection first, so the cancellation is observed in the
	// wait rather than in the dial.
	if _, err := listener.Wait(context.Background(), 50*time.Millisecond); err != nil {
		t.Fatalf("priming Wait: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	woken, err := listener.Wait(ctx, 5*time.Second)
	if err != nil {
		t.Errorf("Wait: %v, want a cancelled context to be reported as not-woken", err)
	}
	if woken {
		t.Error("Wait reported a wakeup on a cancelled context")
	}
}

// A dead connection degrades to "not woken, and here is why" — never to a fatal
// error, because this is an optimisation and never the delivery path (§13.1).
func TestListener_ADeadConnectionIsReportedAndRecoverable(t *testing.T) {
	dsn, pool := pgtest.Accounts(t)
	ctx := context.Background()

	listener := wakeup.NewListener(dsn, slog.New(slog.DiscardHandler))
	defer func() { _ = listener.Close(ctx) }()

	if _, err := listener.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("priming Wait: %v", err)
	}

	// Kill the listening backend from outside, the way a database restart would.
	if _, err := pool.Exec(ctx, `
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
 WHERE query LIKE 'LISTEN %' AND pid <> pg_backend_pid()`); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	// The next wait notices. Which call notices depends on when the connection
	// is discovered to be gone, so either an error now or a clean recovery is
	// acceptable — what is not acceptable is staying silently deaf forever.
	if _, err := listener.Wait(ctx, 500*time.Millisecond); err != nil {
		t.Logf("first wait after the kill reported: %v", err)
	}

	if _, err := listener.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("the listener did not rebuild its connection: %v", err)
	}
	wakeup.NewNotifier(pool, slog.New(slog.DiscardHandler)).Notify(ctx)

	woken, err := listener.Wait(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !woken {
		t.Error("the listener never recovered; the relay would poll forever after " +
			"one database restart")
	}
}
