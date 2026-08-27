package postgres

import (
	"errors"
	"fmt"
	"time"
)

// PoolConfig is this project's pgxpool tuning.
//
// The values live in a struct rather than in NewPool's body so that every
// setting the pool runs with is nameable and overridable at the call site — a
// relay that holds one long-lived connection wants different lifetimes from an
// API sized for concurrent requests, and neither should have to fork NewPool to
// say so.
//
// NewPool applies no defaults to a field left at zero. That is deliberate: a
// pool quietly running with tuning nobody wrote down is exactly the failure the
// struct exists to prevent. Start from DefaultPoolConfig and override.
type PoolConfig struct {
	DSN string

	// MaxConns caps the pool. It has no default here because it is the one
	// setting each process must size for itself.
	MaxConns int32

	// MinConns is a warm floor, so that the first registration after an idle
	// period does not pay TCP connect, startup and auth inside a user-facing
	// request. NewPool already blocks on Ping, so this work happens where it
	// belongs.
	MinConns int32

	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// DefaultPoolConfig returns the tuning every process in this project starts
// from. maxConns is the only tuning value taken as a parameter, because it is
// the one that legitimately differs between processes — the API sizes for
// concurrent requests, the relay holds far fewer. Override any other field on
// the returned struct.
func DefaultPoolConfig(dsn string, maxConns int32) PoolConfig {
	return PoolConfig{
		DSN:               dsn,
		MaxConns:          maxConns,
		MinConns:          2,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	}
}

// validate reports every problem at once. Configuration errors surface at
// startup, where fixing three of them should take one restart, not three.
func (c PoolConfig) validate() error {
	var errs []error
	if c.DSN == "" {
		errs = append(errs, errors.New("DSN is empty"))
	}
	if c.MaxConns <= 0 {
		errs = append(errs, fmt.Errorf("MaxConns must be positive, got %d", c.MaxConns))
	}
	if c.MinConns < 0 {
		errs = append(errs, fmt.Errorf("MinConns must not be negative, got %d", c.MinConns))
	}
	if c.MaxConns > 0 && c.MinConns > c.MaxConns {
		errs = append(errs, fmt.Errorf("MinConns %d exceeds MaxConns %d", c.MinConns, c.MaxConns))
	}
	if c.MaxConnLifetime <= 0 {
		errs = append(errs, fmt.Errorf("MaxConnLifetime must be positive, got %s", c.MaxConnLifetime))
	}
	if c.MaxConnIdleTime <= 0 {
		errs = append(errs, fmt.Errorf("MaxConnIdleTime must be positive, got %s", c.MaxConnIdleTime))
	}
	// An idle timeout longer than the lifetime can never fire, which makes it a
	// setting that reads as configured but does nothing.
	if c.MaxConnLifetime > 0 && c.MaxConnIdleTime > c.MaxConnLifetime {
		errs = append(errs, fmt.Errorf("MaxConnIdleTime %s exceeds MaxConnLifetime %s",
			c.MaxConnIdleTime, c.MaxConnLifetime))
	}
	if c.HealthCheckPeriod <= 0 {
		errs = append(errs, fmt.Errorf("HealthCheckPeriod must be positive, got %s", c.HealthCheckPeriod))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("postgres: pool config: %w", errors.Join(errs...))
}
