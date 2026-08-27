package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestNewDisplayName_Trims(t *testing.T) {
	n, err := NewDisplayName("  Ada Lovelace\t")
	if err != nil {
		t.Fatalf("NewDisplayName: %v", err)
	}
	if n.String() != "Ada Lovelace" {
		t.Errorf("String() = %q, want %q", n, "Ada Lovelace")
	}
}

func TestNewDisplayName_Rejects(t *testing.T) {
	tests := map[string]string{
		"empty":    "",
		"blank":    " \t\n ",
		"too long": strings.Repeat("a", maxDisplayNameRunes+1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			n, err := NewDisplayName(raw)
			if !errors.Is(err, ErrInvalidDisplayName) {
				t.Fatalf("NewDisplayName(%q…) = %q, %v; want ErrInvalidDisplayName", name, n, err)
			}
			if n != (DisplayName{}) {
				t.Errorf("a rejected name must not yield a usable DisplayName, got %q", n)
			}
		})
	}
}

// The limit counts characters, not bytes. len() would reject a name a third of
// the allowed length as soon as it stopped being ASCII.
func TestNewDisplayName_LimitsRunesNotBytes(t *testing.T) {
	name := strings.Repeat("字", maxDisplayNameRunes)
	if len(name) <= maxDisplayNameRunes {
		t.Fatalf("test is not exercising multi-byte runes: %d bytes", len(name))
	}
	if _, err := NewDisplayName(name); err != nil {
		t.Errorf("a %d-character name was rejected: %v", maxDisplayNameRunes, err)
	}
}

func TestDisplayName_ZeroValueIsEmpty(t *testing.T) {
	var n DisplayName
	if n.String() != "" {
		t.Errorf("zero DisplayName = %q, want empty", n)
	}
}
