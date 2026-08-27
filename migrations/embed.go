// Package migrations holds the SQL schema of both bounded contexts and applies
// it. Each context has its own directory and its own goose_db_version table in
// its own database, so the two schema histories are independent (spec §8.3).
//
// Migration is a separate step, never something a service does at startup: with
// four processes and any replica count, startup migration is a race.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/pressly/goose/v3"

	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

//go:embed accounts/*.sql notifications/*.sql
var files embed.FS

// Context names a bounded context, which here means one database.
type Context string

const (
	Accounts      Context = "accounts"
	Notifications Context = "notifications"
)

var (
	ErrUnknownContext = errors.New("migrations: unknown context")

	// ErrSchemaBehind means the database is at an older migration than the
	// binary expects. A process in that state reports itself unready rather than
	// running against a schema it does not understand (spec §8.3, §13.4).
	ErrSchemaBehind = errors.New("migrations: schema is behind the expected version")
)

// Contexts lists every context in a stable order, for commands that act on all
// of them.
func Contexts() []Context { return []Context{Accounts, Notifications} }

// Parse turns a command-line value into a Context. It is exported so that
// cmd/migrate validates the flag with this package's own rule rather than
// repeating the membership check.
func Parse(name string) (Context, error) {
	c := Context(name)
	if !slices.Contains(Contexts(), c) {
		return "", fmt.Errorf("%w: %q", ErrUnknownContext, name)
	}
	return c, nil
}

// FS returns the migrations of one context, rooted so that goose sees the .sql
// files at ".".
func FS(c Context) (fs.FS, error) {
	parsed, err := Parse(string(c))
	if err != nil {
		return nil, err
	}
	return fs.Sub(files, string(parsed))
}

// Latest is the highest migration version embedded for a context. It is derived
// from the filenames rather than declared in a constant, because a constant is
// something a future migration forgets to bump.
//
// The version is parsed by goose.NumericComponent — the same parser that will
// run these files — so this package cannot disagree with goose about what a
// valid migration filename is. The prefixes are zero-padded, so lexical order is
// numeric order and the newest is last. fs.Glob already returns names sorted,
// and the explicit sort below is kept anyway — it costs nothing on a handful of
// filenames and it means this function does not depend on that guarantee.
func Latest(c Context) (int64, error) {
	f, err := FS(c)
	if err != nil {
		return 0, err
	}
	names, err := fs.Glob(f, "*.sql")
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		return 0, fmt.Errorf("migrations: no .sql files embedded for %s", c)
	}
	slices.Sort(names)
	newest := names[len(names)-1]
	v, err := goose.NumericComponent(newest)
	if err != nil {
		return 0, fmt.Errorf("migrations: %s: %w", newest, err)
	}
	return v, nil
}

// Apply brings one context's database up to Latest.
func Apply(db *sql.DB, c Context) error {
	f, err := FS(c)
	if err != nil {
		return err
	}
	goose.SetBaseFS(f)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("migrations: dialect: %w", err)
	}
	if err := goose.Up(db, "."); err != nil {
		return fmt.Errorf("migrations: up %s: %w", c, err)
	}
	return nil
}

// Version is the currently applied version of the database behind db.
func Version(db *sql.DB) (int64, error) {
	if err := goose.SetDialect("postgres"); err != nil {
		return 0, fmt.Errorf("migrations: dialect: %w", err)
	}
	v, err := goose.GetDBVersion(db)
	if err != nil {
		return 0, fmt.Errorf("migrations: version: %w", err)
	}
	return v, nil
}

// Ready reports whether the database behind q has at least the expected
// migration applied. It takes a Queryer because its caller is a readiness probe
// holding a pgx pool, not a *sql.DB, which is why it cannot simply call
// goose.GetDBVersion.
//
// max(version_id) and goose.GetDBVersion disagree after a down migration —
// goose honours is_applied, this does not. That is acceptable for "is the schema
// at least as new as my binary expects" and acceptable for nothing else.
func Ready(ctx context.Context, q platformpg.Queryer, expected int64) error {
	query := fmt.Sprintf(`SELECT coalesce(max(version_id), 0) FROM %s`, goose.TableName())
	var applied int64
	if err := q.QueryRow(ctx, query).Scan(&applied); err != nil {
		return fmt.Errorf("migrations: read schema version: %w", err)
	}
	if applied < expected {
		return fmt.Errorf("%w: applied %d, expected %d", ErrSchemaBehind, applied, expected)
	}
	return nil
}
