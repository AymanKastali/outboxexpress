package domain

import (
	"strings"
	"unicode/utf8"
)

// maxDisplayNameRunes is a limit on characters, not bytes. The column is TEXT
// with no length constraint, so this bound exists only because the domain says
// a display name is short — and a rule stated in characters should be counted
// in characters.
const maxDisplayNameRunes = 200

// DisplayName is a validated human-facing name. Like Email, the value is
// unexported behind one constructor: a DisplayName that exists is one that was
// checked, so no code path can put an empty or unbounded name on a User.
//
// Trimming happens here rather than at the caller, which is what makes
// "  Ada  " and "Ada" the same name — the type owns its normal form.
type DisplayName struct {
	value string
}

// NewDisplayName trims surrounding space, then requires a non-empty name within
// the length bound.
func NewDisplayName(raw string) (DisplayName, error) {
	s := strings.TrimSpace(raw)
	if s == "" || utf8.RuneCountInString(s) > maxDisplayNameRunes {
		return DisplayName{}, ErrInvalidDisplayName
	}
	return DisplayName{value: s}, nil
}

// String returns the trimmed name, for an adapter to write to a column or a
// payload.
func (n DisplayName) String() string { return n.value }
