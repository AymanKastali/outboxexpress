//go:build integration

package migrations_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
	"github.com/AymanKastali/outboxexpress/migrations"
)

func TestAccountsSchema(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	t.Run("tables exist", func(t *testing.T) {
		for _, table := range []string{"users", "outbox"} {
			var exists bool
			err := pool.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables
				                 WHERE table_schema = 'accounts' AND table_name = $1)`,
				table).Scan(&exists)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if !exists {
				t.Errorf("accounts.%s missing", table)
			}
		}
	})

	t.Run("partial index on pending rows", func(t *testing.T) {
		var def string
		err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes
			  WHERE schemaname = 'accounts' AND indexname = 'outbox_pending_idx'`).Scan(&def)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		for _, want := range []string{"available_at", "id", "WHERE"} {
			if !strings.Contains(def, want) {
				t.Errorf("indexdef %q missing %q", def, want)
			}
		}
	})

	t.Run("status is constrained", func(t *testing.T) {
		_, err := pool.Exec(ctx,
			`INSERT INTO accounts.outbox
			   (event_id, aggregate_type, aggregate_id, event_type, payload, status)
			 VALUES (gen_random_uuid(), 'User', 'x', 'y', '{}', 'nonsense')`)
		if err == nil {
			t.Fatal("expected outbox_status_check to reject status 'nonsense'")
		}
	})

	t.Run("Ready agrees with the embedded latest", func(t *testing.T) {
		latest, err := migrations.Latest(migrations.Accounts)
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if err := migrations.Ready(ctx, pool, latest); err != nil {
			t.Fatalf("Ready(%d) = %v, want nil", latest, err)
		}
		if err := migrations.Ready(ctx, pool, latest+1); !errors.Is(err, migrations.ErrSchemaBehind) {
			t.Fatalf("Ready(%d) = %v, want ErrSchemaBehind", latest+1, err)
		}
	})
}

func TestNotificationsSchema(t *testing.T) {
	_, pool := pgtest.Notifications(t)
	ctx := context.Background()

	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
		                 WHERE table_schema = 'notifications' AND table_name = 'inbox')`).Scan(&exists)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !exists {
		t.Error("notifications.inbox missing")
	}
}
