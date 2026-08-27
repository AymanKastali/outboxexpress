// Package pgtest gives an integration test a real PostgreSQL 18.6, migrated and
// empty.
//
// One container per test binary, holding both databases — the same shape
// deploy/docker-compose.yml deploys. A container per test would add a boot and a
// goose run to every one of them, and would buy no isolation that TRUNCATE does
// not already give.
//
// This file carries no build tag on purpose. A package whose every file is
// excluded by a constraint makes `go build ./...` fail with "build constraints
// exclude all Go files", so the tag goes on the tests that use pgtest, never on
// pgtest itself. Importing "testing" outside a test file is the price; nothing
// in cmd/ imports this package, and the architecture test would say so if it did.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
	"github.com/AymanKastali/outboxexpress/migrations"
)

const (
	image = "postgres:18.6"

	// poolMaxConns is per database, and tests within a binary run sequentially
	// unless they opt into t.Parallel.
	poolMaxConns = 4
)

// The container is started once and never terminated explicitly: testcontainers'
// reaper removes it when the test binary exits, which is the only moment at
// which terminating it is safe.
var (
	instance = sync.OnceValues(startInstance)

	accountsDB = sync.OnceValues(func() (database, error) {
		return open(migrations.Accounts, "oe_accounts")
	})
	notificationsDB = sync.OnceValues(func() (database, error) {
		return open(migrations.Notifications, "oe_notifications")
	})
)

type database struct {
	dsn  string
	pool *pgxpool.Pool
}

// Accounts returns a DSN and pool for a migrated, empty oe_accounts.
func Accounts(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	d, err := accountsDB()
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	TruncateAccounts(t, d.pool)
	return d.dsn, d.pool
}

// Notifications is the same for oe_notifications.
func Notifications(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	d, err := notificationsDB()
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	Truncate(t, d.pool,
		"notifications.outbox", "notifications.notifications", "notifications.inbox")
	return d.dsn, d.pool
}

// TruncateAccounts empties the accounts tables, resetting the outbox sequence so
// that row ids are predictable per test.
func TruncateAccounts(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	Truncate(t, pool, "accounts.outbox", "accounts.users")
}

// Truncate empties the named tables in one statement. The table list is a
// parameter because it is the only thing that varies between contexts.
func Truncate(t *testing.T, pool *pgxpool.Pool, tables ...string) {
	t.Helper()
	if len(tables) == 0 {
		return
	}
	stmt := "TRUNCATE " + strings.Join(tables, ", ") + " RESTART IDENTITY"
	if _, err := pool.Exec(context.Background(), stmt); err != nil {
		t.Fatalf("pgtest: truncate: %v", err)
	}
}

// CountAccounts is the assertion every write-path test starts from: how many
// users, and how many outbox rows.
func CountAccounts(t *testing.T, pool *pgxpool.Pool) (users, outbox int) {
	t.Helper()
	ctx := context.Background()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts.users`).Scan(&users); err != nil {
		t.Fatalf("pgtest: count users: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts.outbox`).Scan(&outbox); err != nil {
		t.Fatalf("pgtest: count outbox: %v", err)
	}
	return users, outbox
}

func startInstance() (*tcpostgres.PostgresContainer, error) {
	ctx := context.Background()

	// BasicWaitStrategies is the module's own strategy: the "ready to accept
	// connections" log twice (initdb restarts the server) *and* a listening-port
	// check. Hand-writing only the log wait replaces this rather than extending
	// it, and the module documents that dropping the port check is what makes
	// these tests flaky on macOS and Windows.
	ctr, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("oe_accounts"),
		tcpostgres.WithUsername("oe"),
		tcpostgres.WithPassword("oe"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("pgtest: start %s: %w", image, err)
	}

	// The second database, exactly as deploy/postgres-init.sh creates it.
	adminDSN, err := dsnFor(ctx, ctr, "oe_accounts")
	if err != nil {
		return nil, err
	}
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return nil, fmt.Errorf("pgtest: open admin connection: %w", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE oe_notifications`); err != nil {
		return nil, fmt.Errorf("pgtest: create oe_notifications: %w", err)
	}
	return ctr, nil
}

func open(c migrations.Context, database_ string) (database, error) {
	ctr, err := instance()
	if err != nil {
		return database{}, err
	}
	ctx := context.Background()

	dsn, err := dsnFor(ctx, ctr, database_)
	if err != nil {
		return database{}, err
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return database{}, fmt.Errorf("pgtest: open %s: %w", database_, err)
	}
	err = migrations.Apply(db, c)
	closeErr := db.Close()
	if err != nil {
		return database{}, fmt.Errorf("pgtest: migrate %s: %w", database_, err)
	}
	if closeErr != nil {
		return database{}, fmt.Errorf("pgtest: close migration connection: %w", closeErr)
	}

	// Through the project's own constructor, not pgxpool.New: a test suite that
	// builds its pools differently from production leaves NewPool and
	// DefaultPoolConfig unexercised by every integration test that needs a pool.
	pool, err := platformpg.NewPool(ctx, platformpg.DefaultPoolConfig(dsn, poolMaxConns))
	if err != nil {
		return database{}, fmt.Errorf("pgtest: pool for %s: %w", database_, err)
	}
	return database{dsn: dsn, pool: pool}, nil
}

func dsnFor(ctx context.Context, ctr *tcpostgres.PostgresContainer, database_ string) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("pgtest: host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", fmt.Errorf("pgtest: mapped port: %w", err)
	}
	return fmt.Sprintf("postgres://oe:oe@%s:%s/%s?sslmode=disable",
		host, port.Port(), database_), nil
}
