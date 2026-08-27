//go:build integration

package wakeup

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func TestNotifier_DeliversToAListener(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	listener, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+ChannelOutboxNew); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	n := NewNotifier(pool, slog.New(slog.DiscardHandler))
	n.Notify(ctx)

	notification, err := listener.Conn().WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("WaitForNotification: %v", err)
	}
	if notification.Channel != ChannelOutboxNew {
		t.Errorf("channel = %q, want %q", notification.Channel, ChannelOutboxNew)
	}
}

func TestNotifier_SwallowsFailure(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	n := NewNotifier(pool, slog.New(slog.DiscardHandler))

	// A cancelled context makes the Exec fail. Notify must return quietly: a
	// failed wakeup costs latency, and a registration that has already committed
	// must not be reported as failed because an optimisation did not fire.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n.Notify(ctx)
}
