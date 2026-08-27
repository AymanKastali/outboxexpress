package application

import (
	"context"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

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
