package migrations

import (
	"errors"
	"io/fs"
	"testing"
)

func TestLatest(t *testing.T) {
	for _, c := range []Context{Accounts, Notifications} {
		got, err := Latest(c)
		if err != nil {
			t.Fatalf("Latest(%s): %v", c, err)
		}
		if got != 1 {
			t.Errorf("Latest(%s) = %d, want 1", c, got)
		}
	}
}

func TestParse(t *testing.T) {
	got, err := Parse("accounts")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got != Accounts {
		t.Errorf("Parse = %q, want %q", got, Accounts)
	}
	if _, err := Parse("payments"); !errors.Is(err, ErrUnknownContext) {
		t.Fatalf("err = %v, want ErrUnknownContext", err)
	}
}

func TestLatest_UnknownContext(t *testing.T) {
	if _, err := Latest(Context("payments")); !errors.Is(err, ErrUnknownContext) {
		t.Fatalf("err = %v, want ErrUnknownContext", err)
	}
}

func TestFS_ContainsOnlyThatContextsMigrations(t *testing.T) {
	f, err := FS(Accounts)
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	names, err := fs.Glob(f, "*.sql")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(names) != 1 || names[0] != "0001_init.sql" {
		t.Fatalf("names = %v, want [0001_init.sql]", names)
	}
}
