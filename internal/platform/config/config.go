// Package config parses process configuration from the environment, once, at
// startup. A process that starts with invalid configuration is a process that
// fails later and further from the cause, so every Load function refuses to
// return a partially valid struct.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

var (
	ErrMissing = errors.New("config: required variable is empty")
	ErrInvalid = errors.New("config: invalid value")
)

// API is the configuration of the accounts HTTP process (spec §14).
type API struct {
	AccountsDatabaseURL string
	HTTPAddr            string
	AdminAddr           string
	LogLevel            slog.Level
}

// Migrate is the configuration of the schema migration command.
type Migrate struct {
	AccountsDatabaseURL      string
	NotificationsDatabaseURL string
	LogLevel                 slog.Level
}

func LoadAPI(getenv func(string) string) (API, error) {
	level, err := logLevel(getenv)
	if err != nil {
		return API{}, err
	}
	cfg := API{
		AccountsDatabaseURL: value(getenv, "ACCOUNTS_DATABASE_URL", ""),
		HTTPAddr:            value(getenv, "HTTP_ADDR", ":8080"),
		AdminAddr:           value(getenv, "ADMIN_ADDR", "127.0.0.1:8081"),
		LogLevel:            level,
	}
	if err := required("ACCOUNTS_DATABASE_URL", cfg.AccountsDatabaseURL); err != nil {
		return API{}, err
	}
	return cfg, nil
}

func LoadMigrate(getenv func(string) string) (Migrate, error) {
	level, err := logLevel(getenv)
	if err != nil {
		return Migrate{}, err
	}
	cfg := Migrate{
		AccountsDatabaseURL:      value(getenv, "ACCOUNTS_DATABASE_URL", ""),
		NotificationsDatabaseURL: value(getenv, "NOTIFICATIONS_DATABASE_URL", ""),
		LogLevel:                 level,
	}
	if err := required("ACCOUNTS_DATABASE_URL", cfg.AccountsDatabaseURL); err != nil {
		return Migrate{}, err
	}
	if err := required("NOTIFICATIONS_DATABASE_URL", cfg.NotificationsDatabaseURL); err != nil {
		return Migrate{}, err
	}
	return cfg, nil
}

func value(getenv func(string) string, key, fallback string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return fallback
}

// required takes an already-trimmed value from value(), so a second TrimSpace
// here could never change the answer.
func required(key, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s", ErrMissing, key)
	}
	return nil
}

// logLevel parses LOG_LEVEL with slog's own parser. The set of valid level
// names is a fact the standard library owns; repeating it here would be a
// second source of truth that can drift, and slog's version accepts offsets
// like "info+2" for free. Returning a slog.Level rather than a string means the
// value is parsed once, at startup, and cannot fail again later.
func logLevel(getenv func(string) string) (slog.Level, error) {
	raw := value(getenv, "LOG_LEVEL", "info")
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("%w: LOG_LEVEL=%q: %w", ErrInvalid, raw, err)
	}
	return level, nil
}
