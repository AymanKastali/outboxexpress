//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

var uowNow = time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC)

// The frozen clock lives here, in the test that needs it, not in the clock
// package. Two lines beat an exported type that production code never calls.
type frozenClock struct{ at time.Time }

func (c frozenClock) Now() time.Time { return c.at }

type countingWakeup struct{ calls int }

func (w *countingWakeup) Notify(ctx context.Context) { w.calls++ }

type failingFactory struct{ err error }

func (f failingFactory) From([]domain.Event, application.Metadata) ([]application.Envelope, error) {
	return nil, f.err
}

type failingAppender struct{ err error }

func (f failingAppender) Append(context.Context, []application.Envelope) error { return f.err }

func newUOW(pool *pgxpool.Pool) *UnitOfWork {
	return NewUnitOfWork(pool, application.NewCloudEventFactory(ids.UUIDv7{}))
}

// The invariant. A use case that never mentions the outbox produces an outbox
// row, in the same transaction, because it persisted an aggregate (spec §5).
func TestUnitOfWork_RegistrationWritesUserAndOutboxRowTogether(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	wake := &countingWakeup{}
	uc := application.NewRegisterUser(newUOW(pool), frozenClock{at: uowNow}, ids.UUIDv7{}, wake)

	res, err := uc.Execute(ctx, application.RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
		Meta:        application.Metadata{CorrelationID: "corr-1", Traceparent: "tp-1"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	users, outbox := pgtest.CountAccounts(t, pool)
	if users != 1 || outbox != 1 {
		t.Fatalf("users = %d, outbox = %d; want 1 and 1", users, outbox)
	}

	var (
		eventID       string
		aggregateID   string
		aggregateType string
		eventType     string
		status        string
		payload       string
		headers       string
		occurredAt    time.Time
	)
	err = pool.QueryRow(ctx, `
		SELECT event_id::text, aggregate_id, aggregate_type, event_type, status,
		       payload::text, headers::text, occurred_at
		  FROM accounts.outbox`).Scan(&eventID, &aggregateID, &aggregateType,
		&eventType, &status, &payload, &headers, &occurredAt)
	if err != nil {
		t.Fatalf("select outbox: %v", err)
	}

	if aggregateID != res.UserID.String() {
		t.Errorf("aggregate_id = %s, want the user id %s — this is the partition key",
			aggregateID, res.UserID)
	}
	if aggregateType != domain.AggregateTypeUser {
		t.Errorf("aggregate_type = %q, want %q", aggregateType, domain.AggregateTypeUser)
	}
	if eventType != domain.EventTypeUserRegistered {
		t.Errorf("event_type = %q", eventType)
	}
	if status != "pending" {
		t.Errorf("status = %q, want pending — nothing has published it yet", status)
	}
	if !occurredAt.UTC().Equal(uowNow) {
		t.Errorf("occurred_at = %v, want the injected clock's time %v", occurredAt.UTC(), uowNow)
	}

	var envelope map[string]any
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if envelope["id"] != eventID {
		t.Errorf("payload id %v != event_id column %s — the column and the "+
			"envelope must carry the same deduplication key", envelope["id"], eventID)
	}
	if envelope["subject"] != res.UserID.String() {
		t.Errorf("subject = %v, want %s", envelope["subject"], res.UserID)
	}
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is %T, want an object", envelope["data"])
	}
	if data["email"] != "ada@example.com" {
		t.Errorf("data.email = %v", data["email"])
	}

	var hdrs map[string]string
	if err := json.Unmarshal([]byte(headers), &hdrs); err != nil {
		t.Fatalf("headers is not JSON: %v", err)
	}
	if hdrs["correlation_id"] != "corr-1" || hdrs["traceparent"] != "tp-1" {
		t.Errorf("headers = %v", hdrs)
	}

	if wake.calls != 1 {
		t.Errorf("wakeup.calls = %d, want 1", wake.calls)
	}
}

// The dual-write problem, refused. If the business write fails there is no
// event, and if the event cannot be written there is no user.
func TestUnitOfWork_NeitherRowSurvivesAFailure(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	t.Run("the work fails", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		boom := errors.New("boom")
		err := newUOW(pool).Do(ctx, application.Metadata{}, func(w application.Work) error {
			if err := w.Users.Insert(ctx, mustRegister(t, "ada@example.com")); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0", users, outbox)
		}
	})

	t.Run("the envelope cannot be built", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		boom := errors.New("no mapping")
		uow := NewUnitOfWork(pool, failingFactory{err: boom})
		err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
			return w.Users.Insert(ctx, mustRegister(t, "ada@example.com"))
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the factory error", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0 — a message that "+
				"cannot be built must take the business write down with it", users, outbox)
		}
	})

	t.Run("the outbox append fails", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		boom := errors.New("outbox is unwritable")
		uow := newUOW(pool)
		uow.outbox = func(platformpg.Queryer) application.OutboxAppender {
			return failingAppender{err: boom}
		}
		err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
			return w.Users.Insert(ctx, mustRegister(t, "ada@example.com"))
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the appender error", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0 — this is the dual "+
				"write refused in its purest form: the event could not be "+
				"recorded, so the state change must not have happened either",
				users, outbox)
		}
	})

	t.Run("the email is taken", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		uc := application.NewRegisterUser(newUOW(pool), frozenClock{at: uowNow}, ids.UUIDv7{}, &countingWakeup{})
		cmd := application.RegisterUserCommand{Email: "ada@example.com", DisplayName: "Ada Lovelace"}

		if _, err := uc.Execute(ctx, cmd); err != nil {
			t.Fatalf("first Execute: %v", err)
		}
		if _, err := uc.Execute(ctx, cmd); !errors.Is(err, domain.ErrEmailTaken) {
			t.Fatalf("second Execute: err = %v, want ErrEmailTaken", err)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 1 || outbox != 1 {
			t.Fatalf("users = %d, outbox = %d; want 1 and 1 — the rejected "+
				"registration must leave nothing behind", users, outbox)
		}
	})
}

func TestUnitOfWork_TwoAggregatesInOneTransactionProduceTwoRowsInOrder(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	first := mustRegister(t, "ada@example.com")
	second := mustRegister(t, "grace@example.com")

	err := newUOW(pool).Do(ctx, application.Metadata{}, func(w application.Work) error {
		if err := w.Users.Insert(ctx, first); err != nil {
			return err
		}
		return w.Users.Insert(ctx, second)
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	rows, err := pool.Query(ctx, `SELECT aggregate_id FROM accounts.outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var order []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		order = append(order, id)
	}
	if len(order) != 2 {
		t.Fatalf("len = %d, want 2", len(order))
	}
	if order[0] != first.ID.String() || order[1] != second.ID.String() {
		t.Errorf("order = %v, want [%s %s] — id order is publish order",
			order, first.ID, second.ID)
	}
}

func TestUnitOfWork_CommitsWorkThatEmitsNothing(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	err := newUOW(pool).Do(ctx, application.Metadata{}, func(w application.Work) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 0 {
		t.Fatalf("outbox = %d, want 0", outbox)
	}
}

// The limitation of the structural approach, stated as a test: events are
// collected from aggregates the repositories were given, so SQL that goes around
// a repository changes state and announces nothing. That is a real cost of §5's
// design and the reason raw UPDATEs do not belong in a use case.
func TestUnitOfWork_AWriteThatBypassesTheRepositoryEmitsNothing(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	uow := newUOW(pool)
	user := mustRegister(t, "ada@example.com")

	if err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
		return w.Users.Insert(ctx, user)
	}); err != nil {
		t.Fatalf("first Do: %v", err)
	}

	// A second transaction that changes the row with raw SQL rather than through
	// the repository. No aggregate is tracked, so no event is collected.
	if err := uow.Do(ctx, application.Metadata{}, func(w application.Work) error {
		_, err := pool.Exec(ctx, `UPDATE accounts.users SET display_name = 'Ada L.' WHERE id = $1`, user.ID)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("second Do: %v", err)
	}

	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 1 {
		t.Fatalf("outbox = %d, want 1", outbox)
	}
}
