package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, mux *http.ServeMux, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthz_IsAlwaysOK(t *testing.T) {
	rec := get(t, Router(func(context.Context) error { return errors.New("db is down") }), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — liveness must not depend on a dependency", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	ok := get(t, Router(func(context.Context) error { return nil }), "/readyz")
	if ok.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", ok.Code)
	}

	bad := get(t, Router(func(context.Context) error { return errors.New("schema is behind") }), "/readyz")
	if bad.Code != http.StatusServiceUnavailable {
		t.Errorf("unready status = %d, want 503", bad.Code)
	}
	if !strings.Contains(bad.Body.String(), "schema is behind") {
		t.Errorf("body %q does not say why", bad.Body.String())
	}
}
