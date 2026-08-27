package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

func mustRegister(t *testing.T, email string) *domain.User {
	t.Helper()
	u, err := domain.Register(uuid.New(), email, "Ada Lovelace",
		time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return u
}

func TestTracker_DrainsInTrackingOrder(t *testing.T) {
	a := mustRegister(t, "a@example.com")
	b := mustRegister(t, "b@example.com")

	tr := newTracker()
	tr.track(a)
	tr.track(b)

	events := tr.drain()
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	if events[0].AggregateID() != a.ID().String() {
		t.Errorf("first event is %s, want %s — tracking order is publish order",
			events[0].AggregateID(), a.ID())
	}
	if events[1].AggregateID() != b.ID().String() {
		t.Errorf("second event is %s, want %s", events[1].AggregateID(), b.ID())
	}
}

func TestTracker_DrainIsIdempotent(t *testing.T) {
	tr := newTracker()
	tr.track(mustRegister(t, "a@example.com"))

	if got := len(tr.drain()); got != 1 {
		t.Fatalf("first drain = %d, want 1", got)
	}
	if got := len(tr.drain()); got != 0 {
		t.Fatalf("second drain = %d, want 0", got)
	}
}

// This is a test of PullEvents' drain semantics, which is what makes tracking
// idempotent without a membership check.
func TestTracker_TrackingTheSameAggregateTwiceDoesNotDuplicateItsEvents(t *testing.T) {
	u := mustRegister(t, "a@example.com")
	tr := newTracker()
	tr.track(u)
	tr.track(u)

	if got := len(tr.drain()); got != 1 {
		t.Fatalf("drain = %d events, want 1 — a repository that writes an "+
			"aggregate twice in one transaction must not publish its event twice", got)
	}
}

func TestTracker_EmptyDrain(t *testing.T) {
	if got := newTracker().drain(); len(got) != 0 {
		t.Fatalf("drain = %d, want 0", len(got))
	}
}
