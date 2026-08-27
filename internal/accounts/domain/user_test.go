package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testID  = uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1")
	testNow = time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC)
)

func TestRegister_NormalisesEmailAndTrimsName(t *testing.T) {
	u, err := Register(testID, "  ADA@Example.COM ", "  Ada Lovelace  ", testNow)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if u.Email().String() != "ada@example.com" {
		t.Errorf("Email = %q, want %q", u.Email(), "ada@example.com")
	}
	if u.DisplayName().String() != "Ada Lovelace" {
		t.Errorf("DisplayName = %q, want %q", u.DisplayName(), "Ada Lovelace")
	}
	if u.Version() != 1 {
		t.Errorf("Version = %d, want 1", u.Version())
	}
	if !u.CreatedAt().Equal(testNow) {
		t.Errorf("CreatedAt = %v, want %v", u.CreatedAt(), testNow)
	}
}

func TestRegister_EmitsExactlyOneEvent(t *testing.T) {
	u, err := Register(testID, "ada@example.com", "Ada Lovelace", testNow)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	events := u.PullEvents()
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	got, ok := events[0].(UserRegistered)
	if !ok {
		t.Fatalf("event type = %T, want UserRegistered", events[0])
	}
	want := UserRegistered{
		UserID:       testID,
		Email:        mustEmail(t, "ada@example.com"),
		DisplayName:  mustName(t, "Ada Lovelace"),
		Version:      1,
		RegisteredAt: testNow,
	}
	if got != want {
		t.Errorf("event = %+v, want %+v", got, want)
	}
}

func TestUserRegistered_RoutingFields(t *testing.T) {
	e := UserRegistered{UserID: testID, RegisteredAt: testNow}
	if e.EventType() != "com.outboxexpress.accounts.user.registered" {
		t.Errorf("EventType = %q", e.EventType())
	}
	if e.AggregateType() != "User" {
		t.Errorf("AggregateType = %q", e.AggregateType())
	}
	if e.AggregateID() != testID.String() {
		t.Errorf("AggregateID = %q, want %q", e.AggregateID(), testID)
	}
	if !e.OccurredAt().Equal(testNow) {
		t.Errorf("OccurredAt = %v, want %v", e.OccurredAt(), testNow)
	}
}

func TestRegister_Rejects(t *testing.T) {
	tests := []struct {
		name        string
		id          uuid.UUID
		email       string
		displayName string
		want        error
	}{
		// One row per rule Register owns. What makes an address or a name
		// invalid is enumerated by the types that decide it, in email_test.go
		// and display_name_test.go; repeating those tables here would mean a
		// new rule has to be added in two places.
		{"a bad email is the type's refusal, passed through", testID, "not-an-address", "Ada", ErrInvalidEmail},
		{"a bad display name likewise", testID, "ada@example.com", "   ", ErrInvalidDisplayName},
		{"the nil id is Register's own rule", uuid.Nil, "ada@example.com", "Ada", ErrInvalidID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Register(tc.id, tc.email, tc.displayName, testNow)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if u != nil {
				t.Error("a rejected registration must not return a user")
			}
		})
	}
}

func mustEmail(t *testing.T, raw string) Email {
	t.Helper()
	e, err := NewEmail(raw)
	if err != nil {
		t.Fatalf("NewEmail(%q): %v", raw, err)
	}
	return e
}

func mustName(t *testing.T, raw string) DisplayName {
	t.Helper()
	n, err := NewDisplayName(raw)
	if err != nil {
		t.Fatalf("NewDisplayName(%q): %v", raw, err)
	}
	return n
}

// Register stores the instant it is handed, unchanged. Normalising here would
// mean the domain silently disagreeing with its caller about what time it is;
// the Clock port promises UTC, and the layer that owns the clock keeps that
// promise.
func TestRegister_KeepsTheTimeItIsGiven(t *testing.T) {
	zone := time.FixedZone("UTC+7", 7*60*60)
	at := time.Date(2026, 8, 26, 17, 15, 30, 0, zone)

	u, err := Register(testID, "ada@example.com", "Ada Lovelace", at)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !u.CreatedAt().Equal(at) {
		t.Errorf("CreatedAt = %v, want the same instant as %v", u.CreatedAt(), at)
	}
	if u.CreatedAt().Location() != zone {
		t.Errorf("CreatedAt location = %v, want %v — Register must not convert",
			u.CreatedAt().Location(), zone)
	}
}
