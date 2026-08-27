package domain

import (
	"errors"
	"strings"
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
	if u.Email != "ada@example.com" {
		t.Errorf("Email = %q, want %q", u.Email, "ada@example.com")
	}
	if u.DisplayName != "Ada Lovelace" {
		t.Errorf("DisplayName = %q, want %q", u.DisplayName, "Ada Lovelace")
	}
	if u.Version != 1 {
		t.Errorf("Version = %d, want 1", u.Version)
	}
	if !u.CreatedAt.Equal(testNow) {
		t.Errorf("CreatedAt = %v, want %v", u.CreatedAt, testNow)
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
		Email:        "ada@example.com",
		DisplayName:  "Ada Lovelace",
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
	if e.SchemaVersion() != 1 {
		t.Errorf("SchemaVersion = %d, want 1", e.SchemaVersion())
	}
	if !e.OccurredAt().Equal(testNow) {
		t.Errorf("OccurredAt = %v, want %v", e.OccurredAt(), testNow)
	}
}

func TestPullEvents_DrainsOnce(t *testing.T) {
	u, err := Register(testID, "ada@example.com", "Ada Lovelace", testNow)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := len(u.PullEvents()); got != 1 {
		t.Fatalf("first pull = %d events, want 1", got)
	}
	if got := len(u.PullEvents()); got != 0 {
		t.Fatalf("second pull = %d events, want 0 — a drained aggregate must not "+
			"re-emit, or one commit would append the same outbox row twice", got)
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
		{"empty email", testID, "", "Ada", ErrInvalidEmail},
		{"blank email", testID, "   ", "Ada", ErrInvalidEmail},
		{"no at sign", testID, "ada.example.com", "Ada", ErrInvalidEmail},
		{"no domain", testID, "ada@", "Ada", ErrInvalidEmail},
		{"no local part", testID, "@example.com", "Ada", ErrInvalidEmail},
		{"display name form", testID, "Ada <ada@example.com>", "Ada", ErrInvalidEmail},
		{"two addresses", testID, "a@b.com, c@d.com", "Ada", ErrInvalidEmail},
		{"empty display name", testID, "ada@example.com", "", ErrInvalidDisplayName},
		{"blank display name", testID, "ada@example.com", "   ", ErrInvalidDisplayName},
		{"nil id", uuid.Nil, "ada@example.com", "Ada", ErrInvalidID},
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

func TestRegister_RejectsOverlongInput(t *testing.T) {
	long := strings.Repeat("a", 400)
	if _, err := Register(testID, long+"@example.com", "Ada", testNow); !errors.Is(err, ErrInvalidEmail) {
		t.Errorf("overlong email: err = %v, want ErrInvalidEmail", err)
	}
	if _, err := Register(testID, "ada@example.com", long, testNow); !errors.Is(err, ErrInvalidDisplayName) {
		t.Errorf("overlong display name: err = %v, want ErrInvalidDisplayName", err)
	}
}
