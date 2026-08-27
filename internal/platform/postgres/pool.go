package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	if maxConns <= 0 {
		return nil, fmt.Errorf("postgres: maxConns must be positive, got %d", maxConns)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	// A warm floor, so that the first registration after an idle period does not
	// pay TCP connect, startup and auth inside a user-facing request. NewPool
	// already blocks on Ping, so this work happens where it belongs.
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

// WithTx runs fn inside a transaction, committing if it returns nil.
//
// It exists because three non-obvious rules have to be got right every time, and
// this plan is the first of four places that need them:
//
//  1. Rollback after a successful Commit is a no-op in pgx, so the deferred
//     rollback needs no "did we commit?" flag.
//  2. Begin and Commit errors are wrapped — they are this layer's failures.
//  3. fn's error is returned **unwrapped**, so that a caller several layers up
//     can still errors.Is it against a domain sentinel. Wrapping it here is how
//     a 409 Conflict silently becomes a 500.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}
