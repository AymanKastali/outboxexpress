//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func envelope(t *testing.T, aggregateID string, payload string) application.Envelope {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return application.Envelope{
		EventID:       id,
		AggregateType: "User",
		AggregateID:   aggregateID,
		EventType:     "com.outboxexpress.accounts.user.registered",
		SchemaVersion: 1,
		Payload:       []byte(payload),
		Headers:       map[string]string{"correlation_id": "corr-1"},
		OccurredAt:    time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
	}
}

func TestOutboxRepository_Append(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	a := envelope(t, "user-a", `{"specversion":"1.0","data":{"email":"a@b.com"}}`)
	b := envelope(t, "user-b", `{"specversion":"1.0","data":{"email":"c@d.com"}}`)

	if err := repo.Append(ctx, []application.Envelope{a, b}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, schema_version,
		       payload::text, headers::text, occurred_at, status, attempts,
		       available_at <= now() AS due, last_error, published_at
		  FROM accounts.outbox
		 ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		id            int64
		eventID       uuid.UUID
		aggType       string
		aggID         string
		eventType     string
		schemaVersion int
		payload       string
		headers       string
		occurredAt    time.Time
		status        string
		attempts      int
		due           bool
		lastError     *string
		publishedAt   *time.Time
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.eventID, &r.aggType, &r.aggID, &r.eventType,
			&r.schemaVersion, &r.payload, &r.headers, &r.occurredAt, &r.status,
			&r.attempts, &r.due, &r.lastError, &r.publishedAt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(got))
	}
	if got[0].id >= got[1].id {
		t.Errorf("ids %d, %d are not ascending — BIGSERIAL is the ordering authority", got[0].id, got[1].id)
	}
	if got[0].eventID != a.EventID || got[1].eventID != b.EventID {
		t.Error("event ids were not stored as given; the relay must never mint one")
	}
	if got[0].aggID != "user-a" || got[1].aggID != "user-b" {
		t.Errorf("aggregate ids = %q, %q", got[0].aggID, got[1].aggID)
	}
	// Append writes eight columns and leaves the relay's five to their defaults.
	// Asserting that once is enough — it is a property of the INSERT, not of the
	// row — and it keeps this test from breaking when Plan 2 touches those columns.
	if first := got[0]; first.status != "pending" || first.attempts != 0 || !first.due ||
		first.lastError != nil || first.publishedAt != nil || first.schemaVersion != 1 {
		t.Errorf("relay bookkeeping was not left at its defaults: %+v", first)
	}

	// JSONB normalises: it sorts object keys and drops insignificant whitespace,
	// so the stored bytes are not the factory's bytes. Compare meaning, not
	// bytes. The byte-exact contract test lives in the application layer, where
	// the bytes are still the ones that will be published.
	assertSameJSON(t, got[0].payload, string(a.Payload))
	assertSameJSON(t, got[0].headers, `{"correlation_id":"corr-1"}`)

	if !got[0].occurredAt.Equal(a.OccurredAt) {
		t.Errorf("occurred_at = %v, want %v", got[0].occurredAt, a.OccurredAt)
	}
}

func TestOutboxRepository_Append_RejectsADuplicateEventID(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	repo := NewOutboxRepository(pool)

	e := envelope(t, "user-a", `{"specversion":"1.0"}`)
	if err := repo.Append(ctx, []application.Envelope{e}); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if err := repo.Append(ctx, []application.Envelope{e}); err == nil {
		t.Fatal("expected the event_id UNIQUE constraint to reject the duplicate")
	}
}

func TestOutboxRepository_Append_EmptyIsANoOp(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	if err := NewOutboxRepository(pool).Append(ctx, nil); err != nil {
		t.Fatalf("Append(nil): %v", err)
	}
	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 0 {
		t.Fatalf("outbox = %d, want 0", outbox)
	}
}

func assertSameJSON(t *testing.T, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	// Both sides are now trees of map[string]any / []any / float64 / string, which
	// DeepEqual compares directly — no need to re-marshal just to sort keys.
	if !reflect.DeepEqual(g, w) {
		t.Errorf("json mismatch\n got: %s\nwant: %s", got, want)
	}
}
