package domain

import "testing"

func TestEventRecorder_ZeroValueIsUsable(t *testing.T) {
	var r eventRecorder
	if got := r.PullEvents(); got != nil {
		t.Fatalf("an unused recorder drained %d events, want nil", len(got))
	}
}

func TestEventRecorder_KeepsTheOrderEventsWereRecordedIn(t *testing.T) {
	var r eventRecorder
	first := UserRegistered{DisplayName: mustName(t, "first")}
	second := UserRegistered{DisplayName: mustName(t, "second")}
	r.record(first)
	r.record(second)

	events := r.PullEvents()
	if len(events) != 2 {
		t.Fatalf("drained %d events, want 2", len(events))
	}
	if events[0].(UserRegistered).DisplayName.String() != "first" ||
		events[1].(UserRegistered).DisplayName.String() != "second" {
		t.Error("order must be the order they were recorded in — the outbox's " +
			"ordering guarantee starts here, not at the relay")
	}
}

func TestEventRecorder_DrainsOnce(t *testing.T) {
	var r eventRecorder
	r.record(UserRegistered{})
	if got := len(r.PullEvents()); got != 1 {
		t.Fatalf("first drain returned %d, want 1", got)
	}
	if got := len(r.PullEvents()); got != 0 {
		t.Fatalf("second drain returned %d, want 0 — a re-emitting aggregate "+
			"turns one state change into two published events", got)
	}
}
