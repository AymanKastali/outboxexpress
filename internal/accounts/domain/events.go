package domain

import (
	"time"

	"github.com/google/uuid"
)

// Event is what an aggregate emits when its state changes. Routing fields are
// methods rather than payload data because the relay routes on them and must
// never parse business content (spec §8.1): AggregateType selects the topic,
// AggregateID becomes the partition key.
//
// There is deliberately no SchemaVersion here. A wire schema version is message
// contract, not domain vocabulary — no domain expert has an opinion about it,
// nothing routes on it, and one domain event type returning one fixed version
// would make §9.3's dual-publish migration (one state change emitted as two
// schema versions in a single transaction) inexpressible. The application layer
// names the version alongside the schema it belongs to.
type Event interface {
	EventType() string
	AggregateType() string
	AggregateID() string
	OccurredAt() time.Time
}

const (
	// AggregateTypeUser selects the topic at publish time.
	AggregateTypeUser = "User"

	// EventTypeUserRegistered is reverse-DNS and versionless: a breaking change
	// becomes a new schema_version or a new type, never a redefinition of this
	// one (spec §9.3).
	EventTypeUserRegistered = "com.outboxexpress.accounts.user.registered"
)

// UserRegistered carries the state a consumer needs as of the moment it
// occurred, plus the aggregate version, so that no consumer has to call back
// into accounts (event-carried state transfer, spec §9.1).
type UserRegistered struct {
	UserID       uuid.UUID
	Email        Email
	DisplayName  DisplayName
	Version      int
	RegisteredAt time.Time
}

func (e UserRegistered) EventType() string     { return EventTypeUserRegistered }
func (e UserRegistered) AggregateType() string { return AggregateTypeUser }
func (e UserRegistered) AggregateID() string   { return e.UserID.String() }
func (e UserRegistered) OccurredAt() time.Time { return e.RegisteredAt }
