package domain

import (
	"net/mail"
	"strings"
)

// maxEmailLength is RFC 3696's local part plus domain.
const maxEmailLength = 320

// Email is a validated address. The value is unexported and there is exactly
// one constructor, so an Email that exists is an Email that passed validation —
// no call site can hold an unchecked one, and no future code path can build a
// User around a string nobody looked at.
//
// It normalises on the way in, which makes the type comparable by value: two
// addresses differing only in case or surrounding space are ==, without every
// caller remembering to fold them first. That is also what makes the database's
// UNIQUE constraint mean what the domain means.
type Email struct {
	value string
}

// NewEmail normalises and validates one address, rejecting anything that is not
// a bare addr-spec.
func NewEmail(raw string) (Email, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" || len(s) > maxEmailLength {
		return Email{}, ErrInvalidEmail
	}
	parsed, err := mail.ParseAddress(s)
	if err != nil {
		return Email{}, ErrInvalidEmail
	}
	// Requiring the parse to be a no-op is what rejects everything that is a
	// valid address *expression* without being a bare address: the display-name
	// form ("Ada <ada@example.com>"), and the angle-bracket form. Storing either
	// would make the email column mean two different things. Testing
	// parsed.Name as well would add nothing — a display name is exactly what
	// makes parsed.Address differ from the input.
	if parsed.Address != s {
		return Email{}, ErrInvalidEmail
	}
	// ParseAddress has already established that there is an "@"; Cut says so in
	// the code rather than relying on an index that would panic if it had not.
	_, domainPart, ok := strings.Cut(s, "@")
	if !ok || !strings.Contains(domainPart, ".") {
		return Email{}, ErrInvalidEmail
	}
	return Email{value: s}, nil
}

// String returns the normalised address. It is what an adapter writes to a
// column or a payload; the domain hands out the string, never the type's
// innards.
func (e Email) String() string { return e.value }
