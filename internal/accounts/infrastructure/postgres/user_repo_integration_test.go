//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

func TestUserRepository_Save(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	tr := newTracker()
	repo := newUserRepository(pool, tr)

	user := mustRegister(t, "ada@example.com")
	if err := repo.Save(ctx, user); err != nil {
		t.Fatalf("Save: %v", err)
	}

	var (
		email, displayName string
		version            int
		createdAt          time.Time
	)
	err := pool.QueryRow(ctx,
		`SELECT email, display_name, version, created_at FROM accounts.users WHERE id = $1`,
		user.ID()).Scan(&email, &displayName, &version, &createdAt)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if email != "ada@example.com" || displayName != "Ada Lovelace" || version != 1 {
		t.Errorf("row = %q/%q/%d", email, displayName, version)
	}
	if !createdAt.UTC().Equal(user.CreatedAt()) {
		t.Errorf("created_at = %v, want %v", createdAt.UTC(), user.CreatedAt())
	}
	if got := len(tr.drain()); got != 1 {
		t.Errorf("tracker holds %d events after a successful save, want 1", got)
	}
}

func TestUserRepository_Save_DuplicateEmailBecomesADomainError(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	ctx := context.Background()
	tr := newTracker()
	repo := newUserRepository(pool, tr)

	if err := repo.Save(ctx, mustRegister(t, "ada@example.com")); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	tr.drain()

	err := repo.Save(ctx, mustRegister(t, "ada@example.com"))
	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("err = %v, want domain.ErrEmailTaken", err)
	}
	if got := len(tr.drain()); got != 0 {
		t.Errorf("tracker holds %d events after a failed save, want 0 — a state "+
			"change that did not happen must not be announced", got)
	}
}
