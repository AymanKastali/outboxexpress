package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, poolCfg PoolConfig) (*pgxpool.Pool, error) {
	if err := poolCfg.validate(); err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(poolCfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	cfg.MaxConns = poolCfg.MaxConns
	cfg.MinConns = poolCfg.MinConns
	cfg.MaxConnLifetime = poolCfg.MaxConnLifetime
	cfg.MaxConnIdleTime = poolCfg.MaxConnIdleTime
	cfg.HealthCheckPeriod = poolCfg.HealthCheckPeriod

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
