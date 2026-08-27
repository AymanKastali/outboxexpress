package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

const insertUser = `
INSERT INTO accounts.users (id, email, display_name, version, created_at)
VALUES ($1, $2, $3, $4, $5)`

// uniqueViolation is PostgreSQL's SQLSTATE for a unique constraint breach.
const uniqueViolation = "23505"

// emailUniqueConstraint is the constraint name PostgreSQL generates for the
// UNIQUE on accounts.users.email. Matching the name rather than just the
// SQLSTATE means a future unique index on this table cannot be silently
// misreported as a taken email.
const emailUniqueConstraint = "users_email_key"

type userRepository struct {
	q       platformpg.Queryer
	tracker *tracker
}

func newUserRepository(q platformpg.Queryer, tr *tracker) *userRepository {
	return &userRepository{q: q, tracker: tr}
}

func (r *userRepository) Insert(ctx context.Context, u *domain.User) error {
	_, err := r.q.Exec(ctx, insertUser, u.ID, u.Email, u.DisplayName, u.Version, u.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) &&
			pgErr.Code == uniqueViolation &&
			pgErr.ConstraintName == emailUniqueConstraint {
			// The domain owns the rule; the database is where it can actually be
			// enforced under concurrency. Translating here keeps the rule's
			// vocabulary in the domain and its enforcement where it belongs.
			return fmt.Errorf("%w: %s", domain.ErrEmailTaken, u.Email)
		}
		return fmt.Errorf("postgres: insert user %s: %w", u.ID, err)
	}
	// Only after a successful write. The unit of work will drain this aggregate's
	// events into the outbox inside the same transaction.
	r.tracker.track(u)
	return nil
}

var _ domain.UserRepository = (*userRepository)(nil)
