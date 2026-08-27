package domain

// eventRecorder is the recording half of an aggregate root, kept in one type so
// that no aggregate carries a bare slice it could append to, read twice, or
// forget to drain.
//
// Every aggregate in this context embeds it. Its zero value is ready to use, so
// a constructor need not initialise anything, and the slice stays unexported —
// the only ways to move an event in or out are record and PullEvents.
//
// It is unexported and lives in this package rather than in a shared kernel:
// the notifications context is a separate model and gets its own recorder if it
// needs one. Fifteen duplicated lines cost less than a type two bounded
// contexts must agree on (spec §6.3).
type eventRecorder struct {
	events []Event
}

// record appends one event. Aggregates call this only after their invariants
// have accepted the change — an event is a record of something that happened,
// so recording one that was refused would be a lie the outbox then publishes.
func (r *eventRecorder) record(e Event) { r.events = append(r.events, e) }

// PullEvents returns the recorded events and forgets them. Draining is what
// makes a second commit of the same aggregate emit nothing: an aggregate that
// re-emitted would turn one state change into two published events.
func (r *eventRecorder) PullEvents() []Event {
	events := r.events
	r.events = nil
	return events
}
