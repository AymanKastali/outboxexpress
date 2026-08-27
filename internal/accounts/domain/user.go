package domain

import (
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	maxEmailLength       = 320 // RFC 3696 local part + domain
	maxDisplayNameLength = 200
)

// User is the aggregate. Its invariants are checked in one place — the
// constructor — so no code path can produce a User that violates them.
type User struct {
	ID          uuid.UUID
	Email       string
	DisplayName string
	Version     int
	CreatedAt   time.Time

	events []Event
}

// Register creates a user and records the fact that it happened. It performs no
// I/O: the identifier and the current time arrive as values so that this
// function is deterministic and the layer above owns both sources of
// nondeterminism.
func Register(id uuid.UUID, email, displayName string, now time.Time) (*User, error) {
	if id == uuid.Nil {
		return nil, ErrInvalidID
	}
	addr, err := normaliseEmail(email)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(displayName)
	if name == "" || len(name) > maxDisplayNameLength {
		return nil, ErrInvalidDisplayName
	}

	u := &User{
		ID:          id,
		Email:       addr,
		DisplayName: name,
		Version:     1,
		CreatedAt:   now.UTC(),
	}
	u.record(UserRegistered{
		UserID:       u.ID,
		Email:        u.Email,
		DisplayName:  u.DisplayName,
		Version:      u.Version,
		RegisteredAt: u.CreatedAt,
	})
	return u, nil
}

// PullEvents returns the recorded events and forgets them. Draining is what
// makes a second commit of the same aggregate emit nothing: an aggregate that
// re-emitted would turn one state change into two published events.
func (u *User) PullEvents() []Event {
	events := u.events
	u.events = nil
	return events
}

func (u *User) record(e Event) { u.events = append(u.events, e) }

var _ EventSource = (*User)(nil)

// normaliseEmail lowercases and trims, then requires a bare address. The
// display-name form ("Ada <ada@example.com>") parses but is not an address, and
// storing it would make the email column mean two different things.
func normaliseEmail(raw string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" || len(s) > maxEmailLength {
		return "", ErrInvalidEmail
	}
	parsed, err := mail.ParseAddress(s)
	if err != nil {
		return "", ErrInvalidEmail
	}
	if parsed.Name != "" || parsed.Address != s {
		return "", ErrInvalidEmail
	}
	// ParseAddress has already established that there is an "@"; Cut says so in
	// the code rather than relying on an index that would panic if it had not.
	_, domainPart, ok := strings.Cut(s, "@")
	if !ok || !strings.Contains(domainPart, ".") {
		return "", ErrInvalidEmail
	}
	return s, nil
}
