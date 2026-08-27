package postgres

import "github.com/AymanKastali/outboxexpress/internal/accounts/domain"

// aggregate is what this layer needs from a domain object: the ability to be
// drained. It is named for the domain concept the tracker collects, not for a
// mechanism — the comments below say "aggregate" and so should the code.
//
// It is not a duplicate of the domain's eventRecorder. The recorder is the
// concrete half: it holds the slice and appends to it, and an aggregate embeds
// it. This is the consuming half: the one method infrastructure calls. Declaring
// it here rather than exporting it from the domain means draining is something
// this layer asks for, not something every aggregate has to announce about
// itself — and Go asks for no declaration on the aggregate's side either way.
type aggregate interface {
	PullEvents() []domain.Event
}

// tracker remembers which aggregates a transaction touched, so that the unit of
// work can drain their events at commit time without the use case listing them.
// This is the mechanism behind spec §5: the outbox insert is not something a
// caller remembers to do, it is something the persistence layer does because the
// caller persisted an aggregate.
type tracker struct {
	aggregates []aggregate
}

func newTracker() *tracker { return &tracker{} }

// track records an aggregate. Repositories call it after a successful write, and
// only after: an aggregate whose INSERT failed has not changed any state, so it
// has nothing to announce.
//
// There is no de-duplication here, and none is needed: PullEvents drains, so an
// aggregate recorded twice yields its events on the first pull and nothing on
// the second.
func (t *tracker) track(a aggregate) {
	t.aggregates = append(t.aggregates, a)
}

// drain pulls every tracked aggregate's events, in the order they were tracked,
// and forgets them. Ordering matters: these become outbox rows, and outbox row
// order is publish order.
func (t *tracker) drain() []domain.Event {
	events := make([]domain.Event, 0, len(t.aggregates))
	for _, a := range t.aggregates {
		events = append(events, a.PullEvents()...)
	}
	t.aggregates = nil
	return events
}
