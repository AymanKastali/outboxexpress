// Command migrate applies the schema of one bounded context, or of all of them.
// It is a separate binary because no service may migrate at startup (spec §8.3).
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/AymanKastali/outboxexpress/internal/platform/config"
	"github.com/AymanKastali/outboxexpress/internal/platform/logging"
	"github.com/AymanKastali/outboxexpress/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	contextFlag := flag.String("context", "all", "bounded context: accounts, notifications or all")
	action := flag.String("action", "up", "action: up or version")
	flag.Parse()

	cfg, err := config.LoadMigrate(os.Getenv)
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	targets, err := selected(*contextFlag)
	if err != nil {
		return err
	}

	for _, c := range targets {
		dsn, err := dsnFor(cfg, c)
		if err != nil {
			return err
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return fmt.Errorf("open %s: %w", c, err)
		}
		err = act(db, c, *action, log)
		closeErr := db.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("close %s: %w", c, closeErr)
		}
	}
	return nil
}

func act(db *sql.DB, c migrations.Context, action string, log *slog.Logger) error {
	switch action {
	case "up":
		if err := migrations.Apply(db, c); err != nil {
			return err
		}
		v, err := migrations.Version(db)
		if err != nil {
			return err
		}
		log.Info("migrated", "context", string(c), "version", v)
		return nil
	case "version":
		v, err := migrations.Version(db)
		if err != nil {
			return err
		}
		latest, err := migrations.Latest(c)
		if err != nil {
			return err
		}
		log.Info("version", "context", string(c), "applied", v, "latest", latest)
		return nil
	default:
		return fmt.Errorf("unknown action %q (want up or version)", action)
	}
}

func selected(name string) ([]migrations.Context, error) {
	if name == "all" {
		return migrations.Contexts(), nil
	}
	c, err := migrations.Parse(name)
	if err != nil {
		return nil, err
	}
	return []migrations.Context{c}, nil
}

func dsnFor(cfg config.Migrate, c migrations.Context) (string, error) {
	switch c {
	case migrations.Accounts:
		return cfg.AccountsDatabaseURL, nil
	case migrations.Notifications:
		return cfg.NotificationsDatabaseURL, nil
	default:
		return "", fmt.Errorf("%w: %q", migrations.ErrUnknownContext, c)
	}
}
