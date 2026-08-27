//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

func TestNewPool_ConnectsAndPings(t *testing.T) {
	dsn, _ := pgtest.Accounts(t)
	ctx := context.Background()

	pool, err := platformpg.NewPool(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestWithTx(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()

	t.Run("commits", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		err := platformpg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO accounts.users (id, email, display_name)
			                        VALUES (gen_random_uuid(), 'a@example.com', 'A')`)
			return err
		})
		if err != nil {
			t.Fatalf("WithTx: %v", err)
		}
		if users, _ := pgtest.CountAccounts(t, pool); users != 1 {
			t.Fatalf("users = %d, want 1", users)
		}
	})

	t.Run("rolls back and returns fn's error unwrapped", func(t *testing.T) {
		pgtest.TruncateAccounts(t, pool)
		sentinel := errors.New("sentinel")
		err := platformpg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `INSERT INTO accounts.users (id, email, display_name)
			                           VALUES (gen_random_uuid(), 'a@example.com', 'A')`); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want the sentinel — wrapping it here is how a 409 becomes a 500", err)
		}
		if users, _ := pgtest.CountAccounts(t, pool); users != 0 {
			t.Fatalf("users = %d, want 0", users)
		}
	})
}
