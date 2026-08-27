package postgres

import (
	"strings"
	"testing"
	"time"
)

const testDSN = "postgres://oe:oe@localhost:5432/oe_accounts"

func TestDefaultPoolConfig_IsUsableAsIs(t *testing.T) {
	cfg := DefaultPoolConfig(testDSN, 4)
	if err := cfg.validate(); err != nil {
		t.Fatalf("the defaults must be valid without further tuning: %v", err)
	}
	if cfg.MaxConns != 4 {
		t.Errorf("MaxConns = %d, want the caller's 4", cfg.MaxConns)
	}
	if cfg.MinConns > cfg.MaxConns {
		t.Errorf("MinConns %d exceeds MaxConns %d", cfg.MinConns, cfg.MaxConns)
	}
}

func TestPoolConfig_Validate(t *testing.T) {
	tests := map[string]func(*PoolConfig){
		"empty dsn":                func(c *PoolConfig) { c.DSN = "" },
		"non-positive maxConns":    func(c *PoolConfig) { c.MaxConns = 0 },
		"negative minConns":        func(c *PoolConfig) { c.MinConns = -1 },
		"minConns above max":       func(c *PoolConfig) { c.MinConns = c.MaxConns + 1 },
		"zero conn lifetime":       func(c *PoolConfig) { c.MaxConnLifetime = 0 },
		"zero idle time":           func(c *PoolConfig) { c.MaxConnIdleTime = 0 },
		"zero health check":        func(c *PoolConfig) { c.HealthCheckPeriod = 0 },
		"idle time above lifetime": func(c *PoolConfig) { c.MaxConnIdleTime = c.MaxConnLifetime + time.Second },
	}

	for name, breakIt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultPoolConfig(testDSN, 4)
			breakIt(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

// Configuration errors surface at startup, where fixing three of them should
// take one restart, not three. Asserted by naming: the promise is that every
// bad field appears in the message, not that errors.Join produced a particular
// number of leaves.
func TestPoolConfig_ValidateReportsEveryProblemAtOnce(t *testing.T) {
	err := PoolConfig{}.validate()
	if err == nil {
		t.Fatal("expected the zero value to be invalid")
	}
	for _, field := range []string{
		"DSN", "MaxConns", "MaxConnLifetime", "MaxConnIdleTime", "HealthCheckPeriod",
	} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("%s is not named in the error:\n%v", field, err)
		}
	}
	// The comparison rules stay quiet while their operands are themselves
	// invalid: "MinConns 0 exceeds MaxConns 0" on top of "MaxConns must be
	// positive" would be noise, not information.
	if strings.Contains(err.Error(), "exceeds") {
		t.Errorf("a comparison rule fired on already-invalid operands:\n%v", err)
	}
}
