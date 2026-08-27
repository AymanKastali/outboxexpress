package postgres

import "github.com/AymanKastali/outboxexpress/internal/accounts/domain"

// tracker remembers which aggregates a transaction touched, so that the unit of
// work can drain their events at commit time without the use case listing them.
// This is the mechanism behind spec §5: the outbox insert is not something a
// caller remembers to do, it is something the persistence layer does because the
// caller persisted an aggregate.
type tracker struct {
	sources []domain.EventSource
}

func newTracker() *tracker { return &tracker{} }

// track records an aggregate. Repositories call it after a successful write, and
// only after: an aggregate whose INSERT failed has not changed any state, so it
// has nothing to announce.
//
// There is no de-duplication here, and none is needed: PullEvents drains, so an
// aggregate recorded twice yields its events on the first pull and nothing on
// the second.
func (t *tracker) track(s domain.EventSource) {
	t.sources = append(t.sources, s)
}

// drain pulls every tracked aggregate's events, in the order they were tracked,
// and forgets them. Ordering matters: these become outbox rows, and outbox row
// order is publish order.
func (t *tracker) drain() []domain.Event {
	events := make([]domain.Event, 0, len(t.sources))
	for _, s := range t.sources {
		events = append(events, s.PullEvents()...)
	}
	t.sources = nil
	return events
}
