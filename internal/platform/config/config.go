// Package config parses process configuration from the environment, once, at
// startup. A process that starts with invalid configuration is a process that
// fails later and further from the cause, so every Load function refuses to
// return a partially valid struct.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
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

// Relay is the configuration of the accounts publishing process (spec §14).
type Relay struct {
	AccountsDatabaseURL string
	KafkaBrokers        []string
	KafkaTopic          string
	AdminAddr           string
	LogLevel            slog.Level

	BatchSize   int
	IdleMin     time.Duration
	IdleMax     time.Duration
	BackoffBase time.Duration
	BackoffCap  time.Duration

	// MaxAttempts is an alert threshold, not a state transition (spec §12.1,
	// D8). A row past it is counted in the pass line's stuck_rows and keeps
	// retrying; whether it is genuinely poison is a judgement a human makes with
	// last_error in front of them.
	MaxAttempts int

	UseNotify bool

	PurgeInterval   time.Duration
	OutboxRetention time.Duration
	PurgeBatch      int
}

// LoadRelay parses the relay's environment.
//
// Unlike LoadAPI it collects every problem before returning, because there are
// fourteen variables here and telling an operator about them one restart at a
// time is a bad trade. LoadAPI keeps its early returns: with two required
// variables the difference is invisible.
func LoadRelay(getenv func(string) string) (Relay, error) {
	var problems []error
	fail := func(err error) {
		if err != nil {
			problems = append(problems, err)
		}
	}

	level, err := logLevel(getenv)
	fail(err)

	batchSize, err := parsed(getenv, "RELAY_BATCH_SIZE", 100, strconv.Atoi)
	fail(err)
	idleMin, err := parsed(getenv, "RELAY_IDLE_MIN", 50*time.Millisecond, time.ParseDuration)
	fail(err)
	idleMax, err := parsed(getenv, "RELAY_IDLE_MAX", 2*time.Second, time.ParseDuration)
	fail(err)
	backoffBase, err := parsed(getenv, "RELAY_BACKOFF_BASE", time.Second, time.ParseDuration)
	fail(err)
	backoffCap, err := parsed(getenv, "RELAY_BACKOFF_CAP", 5*time.Minute, time.ParseDuration)
	fail(err)
	maxAttempts, err := parsed(getenv, "RELAY_MAX_ATTEMPTS", 10, strconv.Atoi)
	fail(err)
	useNotify, err := parsed(getenv, "RELAY_USE_NOTIFY", true, strconv.ParseBool)
	fail(err)
	purgeInterval, err := parsed(getenv, "PURGE_INTERVAL", time.Minute, time.ParseDuration)
	fail(err)
	outboxRetention, err := parsed(getenv, "OUTBOX_RETENTION", 24*time.Hour, time.ParseDuration)
	fail(err)
	purgeBatch, err := parsed(getenv, "PURGE_BATCH", 1000, strconv.Atoi)
	fail(err)

	cfg := Relay{
		AccountsDatabaseURL: value(getenv, "ACCOUNTS_DATABASE_URL", ""),
		KafkaBrokers:        list(getenv, "KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:          value(getenv, "KAFKA_TOPIC", "accounts.user.v1"),
		// Loopback, not a wildcard: there is no authentication on this listener
		// and the bind address is the only reason that is acceptable (§13.5).
		AdminAddr:       value(getenv, "ADMIN_ADDR", "127.0.0.1:8082"),
		LogLevel:        level,
		BatchSize:       batchSize,
		IdleMin:         idleMin,
		IdleMax:         idleMax,
		BackoffBase:     backoffBase,
		BackoffCap:      backoffCap,
		MaxAttempts:     maxAttempts,
		UseNotify:       useNotify,
		PurgeInterval:   purgeInterval,
		OutboxRetention: outboxRetention,
		PurgeBatch:      purgeBatch,
	}

	fail(required("ACCOUNTS_DATABASE_URL", cfg.AccountsDatabaseURL))
	if len(cfg.KafkaBrokers) == 0 {
		fail(fmt.Errorf("%w: KAFKA_BROKERS", ErrMissing))
	}
	fail(required("KAFKA_TOPIC", cfg.KafkaTopic))
	fail(positive("RELAY_BATCH_SIZE", cfg.BatchSize))
	fail(positive("RELAY_MAX_ATTEMPTS", cfg.MaxAttempts))
	fail(positive("PURGE_BATCH", cfg.PurgeBatch))
	fail(positive("RELAY_IDLE_MIN", cfg.IdleMin))
	fail(positive("RELAY_BACKOFF_BASE", cfg.BackoffBase))
	fail(positive("PURGE_INTERVAL", cfg.PurgeInterval))
	fail(positive("OUTBOX_RETENTION", cfg.OutboxRetention))

	// The comparisons are only checked once both operands are themselves valid,
	// so that a zeroed configuration reports the variables that are wrong rather
	// than also reporting that zero is less than zero.
	if cfg.IdleMin > 0 && cfg.IdleMax > 0 && cfg.IdleMax < cfg.IdleMin {
		fail(fmt.Errorf("%w: RELAY_IDLE_MAX=%v is below RELAY_IDLE_MIN=%v",
			ErrInvalid, cfg.IdleMax, cfg.IdleMin))
	}
	if cfg.BackoffBase > 0 && cfg.BackoffCap > 0 && cfg.BackoffCap < cfg.BackoffBase {
		fail(fmt.Errorf("%w: RELAY_BACKOFF_CAP=%v is below RELAY_BACKOFF_BASE=%v",
			ErrInvalid, cfg.BackoffCap, cfg.BackoffBase))
	}

	if err := errors.Join(problems...); err != nil {
		return Relay{}, err
	}
	return cfg, nil
}

// list splits a comma-separated variable, trimming each element. Empty elements
// are dropped rather than passed on, because "a:9092,,b:9092" is a typo and an
// empty broker address is a connection attempt against nothing.
func list(getenv func(string) string, key, fallback string) []string {
	raw := value(getenv, key, fallback)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// parsed reads a variable, falls back when it is unset, and wraps a parse failure
// so that the message names the variable and the value an operator actually typed.
//
// One generic helper rather than one per type: integer, duration and boolean
// differ only in their parse function, and the error format is the thing
// TestLoadRelay_RejectsUnparseableValues asserts on — worth having in one place
// rather than three.
func parsed[T any](getenv func(string) string, key string, fallback T,
	parse func(string) (T, error)) (T, error) {
	raw := value(getenv, key, "")
	if raw == "" {
		return fallback, nil
	}
	v, err := parse(raw)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("%w: %s=%q: %w", ErrInvalid, key, raw, err)
	}
	return v, nil
}

// positive covers both the counts and the durations. A Duration is an int64, so
// one constraint fits both and %v prints each the way its own type wants.
func positive[T int | time.Duration](key string, v T) error {
	if v <= 0 {
		return fmt.Errorf("%w: %s=%v must be positive", ErrInvalid, key, v)
	}
	return nil
}
