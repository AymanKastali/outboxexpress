//go:build integration

package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	accountspg "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/postgres"
	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	httpapi "github.com/AymanKastali/outboxexpress/internal/accounts/presentation/http"
	"github.com/AymanKastali/outboxexpress/internal/platform/clock"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

// The whole write path, wired as cmd/api wires it, against a real PostgreSQL.
func TestWritePath(t *testing.T) {
	_, pool := pgtest.Accounts(t)

	generator := ids.UUIDv7{}
	envelopes := application.NewCloudEventFactory(generator)
	useCase := application.NewRegisterUser(
		accountspg.NewUnitOfWork(pool, envelopes),
		clock.System{},
		generator,
		wakeup.NewNotifier(pool, wakeup.ChannelOutboxNew, slog.New(slog.DiscardHandler)),
	)
	srv := httptest.NewServer(httpapi.NewRouter(
		httpapi.NewHandler(useCase, generator, slog.New(slog.DiscardHandler))))
	defer srv.Close()

	// The body is read and closed before returning, so the client can reuse one
	// connection instead of opening one per request.
	type response struct {
		status   int
		location string
		body     string
	}
	post := func(t *testing.T, body string) response {
		t.Helper()
		resp, err := http.Post(srv.URL+"/users", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /users: %v", err)
		}
		defer resp.Body.Close()
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		return response{
			status:   resp.StatusCode,
			location: resp.Header.Get("Location"),
			body:     string(payload),
		}
	}

	// What only this altitude can prove: the wiring exists and speaks HTTP. The
	// column-to-envelope contract is asserted once, against the database, in
	// TestUnitOfWork_RegistrationWritesUserAndOutboxRowTogether — repeating it
	// here would break three tests in three packages for one envelope change.
	t.Run("a registration leaves exactly one pending event", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		resp := post(t, `{"email":"ada@example.com","display_name":"Ada Lovelace"}`)
		if resp.status != http.StatusCreated {
			t.Fatalf("status = %d, want 201", resp.status)
		}
		var created struct {
			UserID string `json:"user_id"`
		}
		if err := json.Unmarshal([]byte(resp.body), &created); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if want := "/users/" + created.UserID; resp.location != want {
			t.Errorf("Location = %q, want %q", resp.location, want)
		}

		users, outbox := pgtest.CountAccounts(t, pool)
		if users != 1 || outbox != 1 {
			t.Fatalf("users = %d, outbox = %d; want 1 and 1", users, outbox)
		}

		var status string
		if err := pool.QueryRow(context.Background(),
			`SELECT status FROM accounts.outbox`).Scan(&status); err != nil {
			t.Fatalf("select: %v", err)
		}
		if status != "pending" {
			t.Errorf("status = %q, want pending — no relay exists yet, and that is "+
				"precisely why the row must be safe where it is", status)
		}
	})

	t.Run("a duplicate email changes nothing", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		if resp := post(t, `{"email":"ada@example.com","display_name":"Ada"}`); resp.status != http.StatusCreated {
			t.Fatalf("first status = %d, want 201", resp.status)
		}
		if resp := post(t, `{"email":"ADA@example.com","display_name":"Ada Again"}`); resp.status != http.StatusConflict {
			t.Fatalf("second status = %d, want 409 — email normalisation must make "+
				"these the same address", resp.status)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 1 || outbox != 1 {
			t.Fatalf("users = %d, outbox = %d; want 1 and 1", users, outbox)
		}
	})

	t.Run("a rejected registration writes nothing", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		if resp := post(t, `{"email":"not-an-address","display_name":"Ada"}`); resp.status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.status)
		}
		if users, outbox := pgtest.CountAccounts(t, pool); users != 0 || outbox != 0 {
			t.Fatalf("users = %d, outbox = %d; want 0 and 0", users, outbox)
		}
	})

	t.Run("many registrations produce exactly as many events", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		const n = 50
		for i := 0; i < n; i++ {
			body := `{"email":"user` + strconv.Itoa(i) + `@example.com","display_name":"User"}`
			if resp := post(t, body); resp.status != http.StatusCreated {
				t.Fatalf("registration %d: status = %d", i, resp.status)
			}
		}
		users, outbox := pgtest.CountAccounts(t, pool)
		if users != n || outbox != n {
			t.Fatalf("users = %d, outbox = %d; want %d each — one event per state "+
				"change, no more and no fewer", users, outbox, n)
		}
		var distinct int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(DISTINCT event_id) FROM accounts.outbox`).Scan(&distinct); err != nil {
			t.Fatalf("count distinct: %v", err)
		}
		if distinct != n {
			t.Fatalf("distinct event_id = %d, want %d", distinct, n)
		}
	})
}
