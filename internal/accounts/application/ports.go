// Package application holds the accounts use cases and the technical ports they
// need. It imports the domain and nothing concrete: no SQL, no HTTP, no Kafka,
// no logger (spec §6.1).
package application

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

// Clock and IDGen exist so that a use case is deterministic under test. Reading
// the wall clock or generating a UUID is I/O against the machine.
type Clock interface {
	Now() time.Time
}

type IDGen interface {
	New() (uuid.UUID, error)
}

// Wakeup shortens the relay's polling latency. It has no error return by
// design: this is an optimisation and never the delivery path, so there is no
// failure a caller could sensibly handle (spec §11.1 rule 3, §13.1).
type Wakeup interface {
	Notify(ctx context.Context)
}

// Metadata is the ambient context of a command — where it came from, and which
// trace it belongs to. It travels into the envelope so the trace survives the
// asynchronous hop (spec §9.2).
type Metadata struct {
	CorrelationID string
	Traceparent   string
}

// Work is the set of repositories bound to one transaction. The infrastructure
// constructs it; a use case can only reach persistence through it, which is what
// makes an untransacted write unexpressible.
//
// Do not add an outbox to this struct. The moment Work exposes one, a use case
// can append to it by hand, and the invariant this whole design exists to make
// structural becomes a rule people have to remember again — with no failing test
// to notice, because the arch test walks imports and this would be an
// intra-package reference. A future use case that genuinely needs a different
// transaction boundary (the relay's claim-publish-mark pass, for instance)
// declares its own boundary port and its own work set.
type Work struct {
	Users domain.UserRepository
}

// UnitOfWork is the transaction boundary. Everything fn does commits together or
// not at all — including the outbox rows, which fn never mentions: the
// implementation drains the aggregates' events and appends them inside the same
// transaction (spec §5, §6.4).
type UnitOfWork interface {
	Do(ctx context.Context, meta Metadata, fn func(Work) error) error
}

// Envelope is one outbox row's worth of content: the routing columns the relay
// reads, and the already-serialised message it must not read.
type Envelope struct {
	EventID       uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	SchemaVersion int
	Payload       []byte
	Headers       map[string]string
	OccurredAt    time.Time
}

// EnvelopeFactory maps domain events onto the message contract. This is an
// application concern: the domain must not know what CloudEvents is, and the
// infrastructure must not decide what a message means.
type EnvelopeFactory interface {
	From(events []domain.Event, meta Metadata) ([]Envelope, error)
}

// OutboxAppender appends envelopes to the outbox. Unlike UserRepository it is
// declared here rather than in the domain, because an outbox is not a domain
// concept — no domain expert has heard of one (spec D5).
//
// It is named for the role, not for the table. The relay's needs — claim, mark
// published, mark failed — are a different role with a different consumer, and
// Plan 2 declares its own port rather than widening this one into a fat
// interface with two disjoint users.
type OutboxAppender interface {
	Append(ctx context.Context, envelopes []Envelope) error
}
