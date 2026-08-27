// Package domain is the accounts context's business model: the User aggregate,
// its invariants, and the events it records when those invariants let a change
// through.
//
// It imports the standard library and github.com/google/uuid, and nothing else
// — no driver, no struct tags, no framework (spec §6.1). That restriction is
// enforced by internal/arch, not merely intended. The rule it serves: an
// aggregate records that something happened; it never decides how that fact
// reaches a broker. Register appends UserRegistered, and the unit of work one
// layer out is what turns that into an outbox row.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// User is the aggregate. Its invariants are checked in one place — the
// constructor — so no code path can produce a User that violates them.
//
// The fields are unexported and read through accessors. Exported fields would
// make that claim true of construction and false of the lifetime: any package
// could assign u.Email = Email{} and put the aggregate in a state NewEmail
// exists to make unreachable. An aggregate that cannot be reached around is the
// point of the pattern; accessors are the price Go charges for it.
type User struct {
	id          uuid.UUID
	email       Email
	displayName DisplayName
	version     int
	createdAt   time.Time

	// Embedded, not a field: the aggregate records events, it does not own a
	// collection of them. Promotion is also what gives User its PullEvents.
	eventRecorder
}

func (u *User) ID() uuid.UUID            { return u.id }
func (u *User) Email() Email             { return u.email }
func (u *User) DisplayName() DisplayName { return u.displayName }
func (u *User) CreatedAt() time.Time     { return u.createdAt }

// Version is the aggregate's version, not the message schema's. It is 1 at
// registration and there is nothing yet to increment it, because User has no
// mutating operation — Register is its only transition.
//
// It exists for two reasons. Consumers receive it in UserRegistered and can
// discard an out-of-order update without calling back into accounts (§9.1). And
// when the first mutating use case arrives, this is the column an optimistic
// UPDATE ... WHERE version = $n checks and increments; Save becomes an upsert
// that fails on a stale read rather than overwriting a concurrent change.
// Until then it is carried, not enforced.
func (u *User) Version() int { return u.version }

// Register creates a user and records the fact that it happened. It performs no
// I/O: the identifier and the current time arrive as values so that this
// function is deterministic and the layer above owns both sources of
// nondeterminism.
//
// now is stored as given. Converting it here would be the domain quietly
// disagreeing with its caller about what time it is; the application layer
// orchestrates, and its Clock port is where "timestamps are UTC" is promised.
func Register(id uuid.UUID, email, displayName string, now time.Time) (*User, error) {
	if id == uuid.Nil {
		return nil, ErrInvalidID
	}
	addr, err := NewEmail(email)
	if err != nil {
		return nil, err
	}
	name, err := NewDisplayName(displayName)
	if err != nil {
		return nil, err
	}

	u := &User{
		id:          id,
		email:       addr,
		displayName: name,
		version:     1,
		createdAt:   now,
	}
	u.record(UserRegistered{
		UserID:       u.id,
		Email:        u.email,
		DisplayName:  u.displayName,
		Version:      u.version,
		RegisteredAt: u.createdAt,
	})
	return u, nil
}
