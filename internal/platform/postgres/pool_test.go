package postgres

import (
	"context"
	"testing"
)

func TestNewPool_RejectsAnUnparseableDSN(t *testing.T) {
	pool, err := NewPool(context.Background(), "://not-a-dsn", 4)
	if err == nil {
		pool.Close()
		t.Fatal("expected an error for an unparseable DSN")
	}
}

func TestNewPool_RejectsANonPositiveMaxConns(t *testing.T) {
	_, err := NewPool(context.Background(), "postgres://oe:oe@localhost:5432/oe_accounts", 0)
	if err == nil {
		t.Fatal("expected an error for maxConns = 0")
	}
}
