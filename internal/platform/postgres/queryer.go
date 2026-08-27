// Package postgres holds the pgx plumbing every context shares: pool
// construction, the small interface that lets a repository work equally against
// a pool or a transaction, and the transaction boundary.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Queryer is what a repository needs. Both *pgxpool.Pool and pgx.Tx satisfy it,
// which is the whole trick behind the unit of work: the same repository code is
// transaction-bound or not depending only on what it was handed, and the
// application layer is never handed a pool.
type Queryer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}
