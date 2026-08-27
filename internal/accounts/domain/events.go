package domain

import (
	"time"

	"github.com/google/uuid"
)

// Event is what an aggregate emits when its state changes. Routing fields are
// methods rather than payload data because the relay routes on them and must
// never parse business content (spec §8.1).
type Event interface {
	EventType() string
	AggregateType() string
	AggregateID() string
	SchemaVersion() int
	OccurredAt() time.Time
}

// EventSource is anything the unit of work can drain during a commit. Keeping
// this in the domain is what lets the persistence layer collect events without
// the domain knowing an outbox exists (spec §5, §6.4).
type EventSource interface {
	PullEvents() []Event
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
	Email        string
	DisplayName  string
	Version      int
	RegisteredAt time.Time
}

func (e UserRegistered) EventType() string     { return EventTypeUserRegistered }
func (e UserRegistered) AggregateType() string { return AggregateTypeUser }
func (e UserRegistered) AggregateID() string   { return e.UserID.String() }
func (e UserRegistered) SchemaVersion() int    { return 1 }
func (e UserRegistered) OccurredAt() time.Time { return e.RegisteredAt }
