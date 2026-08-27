package application

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

type seqIDs struct{ n uint64 }

func (s *seqIDs) New() (uuid.UUID, error) {
	s.n++
	var id uuid.UUID
	id[0] = 0xAA
	id[15] = byte(s.n)
	return id, nil
}

type unmappedEvent struct{}

func (unmappedEvent) EventType() string     { return "com.outboxexpress.accounts.user.forgotten" }
func (unmappedEvent) AggregateType() string { return "User" }
func (unmappedEvent) AggregateID() string   { return "x" }
func (unmappedEvent) OccurredAt() time.Time { return time.Unix(0, 0).UTC() }

func TestCloudEventFactory_ProducesTheExactWireFormat(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})

	event := registered(t,
		uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1"),
		"ada@example.com", "Ada Lovelace",
		time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC))
	meta := Metadata{
		CorrelationID: "corr-7",
		Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	envs, err := f.From([]domain.Event{event}, meta)
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("len(envs) = %d, want 1", len(envs))
	}
	got := envs[0]

	if got.EventID.String() != "aa000000-0000-0000-0000-000000000001" {
		t.Errorf("EventID = %s", got.EventID)
	}
	if got.AggregateType != "User" || got.AggregateID != event.UserID.String() {
		t.Errorf("routing columns = %q/%q", got.AggregateType, got.AggregateID)
	}
	if got.EventType != domain.EventTypeUserRegistered {
		t.Errorf("EventType = %q", got.EventType)
	}
	if got.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d", got.SchemaVersion)
	}
	if !got.OccurredAt.Equal(event.RegisteredAt) {
		t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, event.RegisteredAt)
	}
	if got.Headers["correlation_id"] != "corr-7" {
		t.Errorf("correlation_id = %q", got.Headers["correlation_id"])
	}
	if got.Headers["traceparent"] != meta.Traceparent {
		t.Errorf("traceparent = %q", got.Headers["traceparent"])
	}
	if _, ok := got.Headers["ce_id"]; ok {
		t.Error("ce_* headers are composed by the relay from the row's columns, not stored")
	}

	const want = `{"specversion":"1.0","id":"aa000000-0000-0000-0000-000000000001",` +
		`"type":"com.outboxexpress.accounts.user.registered","source":"/services/accounts",` +
		`"subject":"9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1","time":"2026-08-26T10:15:30.100Z",` +
		`"dataschema":"https://schemas.outboxexpress.dev/accounts/user.registered/1.json",` +
		`"datacontenttype":"application/json",` +
		`"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",` +
		`"data":{"user_id":"9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1","email":"ada@example.com",` +
		`"display_name":"Ada Lovelace","version":1,"registered_at":"2026-08-26T10:15:30.100Z"}}`

	if string(got.Payload) != want {
		t.Errorf("payload mismatch\n got: %s\nwant: %s", got.Payload, want)
	}
}

func TestCloudEventFactory_OmitsTraceparentWhenAbsent(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	envs, err := f.From([]domain.Event{registered(t,
		uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1"),
		"ada@example.com", "Ada Lovelace",
		time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
	)}, Metadata{})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(envs[0].Payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := decoded["traceparent"]; ok {
		t.Error("traceparent must be omitted rather than sent empty")
	}
	if _, ok := envs[0].Headers["traceparent"]; ok {
		t.Error("an empty traceparent must not become a header")
	}
}

func TestCloudEventFactory_MintsOneIDPerEvent(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	at := time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC)
	envs, err := f.From([]domain.Event{
		registered(t, uuid.New(), "a@b.com", "A", at),
		registered(t, uuid.New(), "c@d.com", "C", at),
	}, Metadata{})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if envs[0].EventID == envs[1].EventID {
		t.Fatal("two events shared one event_id; the consumer would deduplicate one away")
	}
}

func TestCloudEventFactory_RejectsUnmappedEvent(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	_, err := f.From([]domain.Event{unmappedEvent{}}, Metadata{})
	if !errors.Is(err, ErrUnmappedEvent) {
		t.Fatalf("err = %v, want ErrUnmappedEvent", err)
	}
}

func TestCloudEventFactory_EmptyInput(t *testing.T) {
	f := NewCloudEventFactory(&seqIDs{})
	envs, err := f.From(nil, Metadata{})
	if err != nil {
		t.Fatalf("From: %v", err)
	}
	if len(envs) != 0 {
		t.Fatalf("len(envs) = %d, want 0", len(envs))
	}
}

// registered builds the event the way production does — through the aggregate's
// constructor — rather than by filling a UserRegistered literal. That keeps the
// test honest about what the domain actually emits, and it means the value
// objects' constructors are not re-wrapped in per-package must-helpers.
func registered(t *testing.T, id uuid.UUID, email, name string, at time.Time) domain.UserRegistered {
	t.Helper()
	u, err := domain.Register(id, email, name, at)
	if err != nil {
		t.Fatalf("Register(%q, %q): %v", email, name, err)
	}
	events := u.PullEvents()
	if len(events) != 1 {
		t.Fatalf("Register emitted %d events, want 1", len(events))
	}
	event, ok := events[0].(domain.UserRegistered)
	if !ok {
		t.Fatalf("Register emitted %T, want domain.UserRegistered", events[0])
	}
	return event
}
