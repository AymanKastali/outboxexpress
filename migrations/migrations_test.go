package migrations

import (
	"errors"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Latest must agree with the highest-numbered migration actually embedded.
// Asserting a literal makes every new migration break this test, which teaches
// people to bump the number rather than to check the claim.
func TestLatest(t *testing.T) {
	for _, c := range []Context{Accounts, Notifications} {
		got, err := Latest(c)
		if err != nil {
			t.Fatalf("Latest(%s): %v", c, err)
		}
		want := highestEmbedded(t, c)
		if got != want {
			t.Errorf("Latest(%s) = %d, want %d — the highest file embedded for %s",
				c, got, want, c)
		}
		if got < 1 {
			t.Errorf("Latest(%s) = %d; a context with no migrations is a packaging bug", c, got)
		}
	}
}

// highestEmbedded reads the version off the embedded filenames, which is the
// same source Latest is supposed to be reporting.
func highestEmbedded(t *testing.T, c Context) int64 {
	t.Helper()
	f, err := FS(c)
	if err != nil {
		t.Fatalf("FS(%s): %v", c, err)
	}
	names, err := fs.Glob(f, "*.sql")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	var highest int64
	for _, name := range names {
		v, err := strconv.ParseInt(strings.SplitN(name, "_", 2)[0], 10, 64)
		if err != nil {
			t.Fatalf("%s is not a goose-numbered migration: %v", name, err)
		}
		highest = max(highest, v)
	}
	return highest
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

// The subject here is isolation, not the file count: one context's FS must not
// carry another's migrations, or goose would apply notifications' schema to the
// accounts database. Asserting the exact file list instead made this test fail
// for the unrelated reason that a context gained a migration.
func TestFS_ContainsOnlyThatContextsMigrations(t *testing.T) {
	listing := func(c Context) []string {
		f, err := FS(c)
		if err != nil {
			t.Fatalf("FS(%s): %v", c, err)
		}
		names, err := fs.Glob(f, "*.sql")
		if err != nil {
			t.Fatalf("Glob: %v", err)
		}
		if len(names) == 0 {
			t.Fatalf("FS(%s) embeds no migrations", c)
		}
		return names
	}
	accounts, notifications := listing(Accounts), listing(Notifications)

	// Every context starts at 0001, so name collisions prove nothing. What must
	// hold is that each FS is rooted in its own directory: reading a name from
	// one must not find that same file's *content* in the other.
	accountsFS, err := FS(Accounts)
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	for _, name := range accounts {
		if _, err := fs.ReadFile(accountsFS, name); err != nil {
			t.Errorf("FS(accounts) lists %s but cannot read it: %v", name, err)
		}
	}
	for _, name := range notifications {
		if !slices.Contains(accounts, name) {
			if _, err := fs.ReadFile(accountsFS, name); err == nil {
				t.Errorf("FS(accounts) can read %s, which belongs to notifications", name)
			}
		}
	}
}
