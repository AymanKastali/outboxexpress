package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestNewEmail_Normalises(t *testing.T) {
	tests := map[string]string{
		"already normal": "ada@example.com",
		"upper case":     "ADA@Example.COM",
		"padded":         "  ada@example.com\t",
		"both":           "  ADA@EXAMPLE.COM  ",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			e, err := NewEmail(raw)
			if err != nil {
				t.Fatalf("NewEmail(%q): %v", raw, err)
			}
			if e.String() != "ada@example.com" {
				t.Errorf("String() = %q, want %q", e.String(), "ada@example.com")
			}
		})
	}
}

func TestNewEmail_Rejects(t *testing.T) {
	tests := map[string]string{
		"empty":             "",
		"blank":             "   ",
		"no at":             "ada.example.com",
		"no domain dot":     "ada@localhost",
		"display-name form": "Ada <ada@example.com>",
		"two addresses":     "ada@example.com, grace@example.com",
		"overlong":          strings.Repeat("a", maxEmailLength) + "@example.com",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			e, err := NewEmail(raw)
			if !errors.Is(err, ErrInvalidEmail) {
				t.Fatalf("NewEmail(%q) = %q, %v; want ErrInvalidEmail", raw, e, err)
			}
			if e != (Email{}) {
				t.Errorf("a rejected address must not yield a usable Email, got %q", e)
			}
		})
	}
}

// The point of the type: two addresses that differ only in case or padding are
// the same address, and == says so without anyone remembering to normalise.
func TestEmail_IsComparableByValue(t *testing.T) {
	a, err := NewEmail("ADA@example.com")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewEmail(" ada@EXAMPLE.com ")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("%q != %q, but they are the same address", a, b)
	}
}

// A zero Email cannot be mistaken for a valid one.
func TestEmail_ZeroValueIsEmpty(t *testing.T) {
	var e Email
	if e.String() != "" {
		t.Errorf("zero Email = %q, want empty", e.String())
	}
}
