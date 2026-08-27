package postgres

import (
	"context"
	"testing"
)

func TestNewPool_RejectsAnUnparseableDSN(t *testing.T) {
	pool, err := NewPool(context.Background(), DefaultPoolConfig("://not-a-dsn", 4))
	if err == nil {
		pool.Close()
		t.Fatal("expected an error for an unparseable DSN")
	}
}

// A zero PoolConfig must fail rather than silently acquire defaults: a pool that
// runs with tuning nobody wrote down is the failure mode the struct exists to
// prevent.
func TestNewPool_AppliesNoHiddenDefaults(t *testing.T) {
	pool, err := NewPool(context.Background(), PoolConfig{DSN: testDSN})
	if err == nil {
		pool.Close()
		t.Fatal("expected a zero-value config to be rejected")
	}
}
