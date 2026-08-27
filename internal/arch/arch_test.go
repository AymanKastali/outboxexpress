// Package arch_test enforces the dependency rule of spec §6.1 by walking the
// import graph. A boundary that is not checked is a comment.
package arch_test

import (
	"strings"
	"sync"
	"testing"

	"golang.org/x/tools/go/packages"
)

const module = "github.com/AymanKastali/outboxexpress"

// The import graph is read-only and identical for every rule, so it is loaded
// once per test binary rather than once per test. packages.Load takes seconds.
var loadOnce = sync.OnceValues(func() ([]*packages.Package, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
		Dir:  "../..", // the module root
	}
	return packages.Load(cfg, "./...")
})

func load(t *testing.T) []*packages.Package {
	t.Helper()
	pkgs, err := loadOnce()
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("no packages loaded")
	}
	if n := packages.PrintErrors(pkgs); n > 0 {
		t.Fatalf("%d packages failed to load", n)
	}
	return pkgs
}

// stdlib recognises a standard library import path: it has no dot in its first
// segment, because every module path is a domain name.
func stdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func TestDomainImportsOnlyStdlibAndUUID(t *testing.T) {
	const allowed = "github.com/google/uuid"
	for _, pkg := range load(t) {
		if !strings.Contains(pkg.PkgPath, "/domain") {
			continue
		}
		for imported := range pkg.Imports {
			if stdlib(imported) || imported == allowed {
				continue
			}
			t.Errorf("%s imports %s; the domain may import only the standard "+
				"library and %s", pkg.PkgPath, imported, allowed)
		}
	}
}

func TestApplicationImportsNothingConcrete(t *testing.T) {
	for _, pkg := range load(t) {
		if !strings.Contains(pkg.PkgPath, "/application") {
			continue
		}
		context, ok := contextOf(pkg.PkgPath)
		if !ok {
			t.Fatalf("cannot determine the bounded context of %s", pkg.PkgPath)
		}
		// This allowlist is also what keeps platform out of the application
		// layer: every platform package except messaging falls through to the
		// error below.
		allowed := map[string]bool{}
		allowed["github.com/google/uuid"] = true
		allowed[module+"/internal/platform/messaging"] = true
		allowed[module+"/internal/"+context+"/domain"] = true
		for imported := range pkg.Imports {
			if stdlib(imported) || allowed[imported] {
				continue
			}
			t.Errorf("%s imports %s; the application layer may import its own "+
				"domain, platform/messaging and the standard library — no driver, "+
				"no SQL, no HTTP, no Kafka, no logger", pkg.PkgPath, imported)
		}
	}
}

func TestPresentationAndInfrastructureAreSiblings(t *testing.T) {
	for _, pkg := range load(t) {
		for imported := range pkg.Imports {
			if strings.Contains(pkg.PkgPath, "/presentation") &&
				strings.Contains(imported, "/infrastructure") {
				t.Errorf("%s imports %s; presentation and infrastructure share the "+
					"outer ring and neither may import the other", pkg.PkgPath, imported)
			}
			if strings.Contains(pkg.PkgPath, "/infrastructure") &&
				strings.Contains(imported, "/presentation") {
				t.Errorf("%s imports %s; same rule, other direction", pkg.PkgPath, imported)
			}
		}
	}
}

func TestBoundedContextsDoNotKnowEachOther(t *testing.T) {
	for _, pkg := range load(t) {
		mine, ok := contextOf(pkg.PkgPath)
		if !ok {
			continue
		}
		for imported := range pkg.Imports {
			theirs, ok := contextOf(imported)
			if !ok || theirs == mine {
				continue
			}
			t.Errorf("%s imports %s; the two contexts communicate through a topic, "+
				"not through Go types", pkg.PkgPath, imported)
		}
	}
}

func TestNoProcessWiresBothContexts(t *testing.T) {
	for _, pkg := range load(t) {
		if !strings.HasPrefix(pkg.PkgPath, module+"/cmd/") {
			continue
		}
		if strings.HasPrefix(pkg.PkgPath, module+"/cmd/migrate") {
			continue // the migrator owns both schemas by definition
		}
		seen := map[string]bool{}
		for imported := range pkg.Imports {
			if c, ok := contextOf(imported); ok {
				seen[c] = true
			}
		}
		if len(seen) > 1 {
			t.Errorf("%s wires more than one bounded context (%v); a process that "+
				"holds both pools can write a cross-context transaction", pkg.PkgPath, seen)
		}
	}
}

// contextOf returns the bounded context a package belongs to, if any.
func contextOf(path string) (string, bool) {
	for _, c := range []string{"accounts", "notifications"} {
		if strings.HasPrefix(path, module+"/internal/"+c+"/") ||
			path == module+"/internal/"+c {
			return c, true
		}
	}
	return "", false
}
