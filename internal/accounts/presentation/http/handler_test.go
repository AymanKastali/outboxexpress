package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

var handlerUserID = uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1")

type fakeRegistrar struct {
	err     error
	lastCmd application.RegisterUserCommand
	calls   int
}

func (f *fakeRegistrar) Execute(ctx context.Context, cmd application.RegisterUserCommand) (application.RegisterUserResult, error) {
	f.calls++
	f.lastCmd = cmd
	if f.err != nil {
		return application.RegisterUserResult{}, f.err
	}
	return application.RegisterUserResult{UserID: handlerUserID}, nil
}

type staticIDs struct{}

func (staticIDs) New() (uuid.UUID, error) {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111"), nil
}

func serve(t *testing.T, reg Registrar, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	h := NewHandler(reg, staticIDs{}, slog.New(slog.DiscardHandler))
	rec := httptest.NewRecorder()
	NewRouter(h).ServeHTTP(rec, req)
	return rec
}

func postUsers(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestRegisterUser_Created(t *testing.T) {
	reg := &fakeRegistrar{}
	rec := serve(t, reg, postUsers(`{"email":"ada@example.com","display_name":"Ada Lovelace"}`))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/users/"+handlerUserID.String() {
		t.Errorf("Location = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	var body struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.UserID != handlerUserID.String() {
		t.Errorf("user_id = %q", body.UserID)
	}
	if reg.lastCmd.Email != "ada@example.com" || reg.lastCmd.DisplayName != "Ada Lovelace" {
		t.Errorf("command = %+v", reg.lastCmd)
	}
}

func TestRegisterUser_GeneratesACorrelationIDWhenTheClientSendsNone(t *testing.T) {
	reg := &fakeRegistrar{}
	rec := serve(t, reg, postUsers(`{"email":"ada@example.com","display_name":"Ada"}`))

	want := "11111111-1111-1111-1111-111111111111"
	if reg.lastCmd.Meta.CorrelationID != want {
		t.Errorf("CorrelationID = %q, want the generated %q", reg.lastCmd.Meta.CorrelationID, want)
	}
	if got := rec.Header().Get("X-Correlation-ID"); got != want {
		t.Errorf("response X-Correlation-ID = %q, want %q", got, want)
	}
}

func TestRegisterUser_PassesThroughCorrelationAndTrace(t *testing.T) {
	reg := &fakeRegistrar{}
	req := postUsers(`{"email":"ada@example.com","display_name":"Ada"}`)
	req.Header.Set("X-Correlation-ID", "corr-from-client")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	serve(t, reg, req)

	if reg.lastCmd.Meta.CorrelationID != "corr-from-client" {
		t.Errorf("CorrelationID = %q", reg.lastCmd.Meta.CorrelationID)
	}
	if reg.lastCmd.Meta.Traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("Traceparent = %q", reg.lastCmd.Meta.Traceparent)
	}
}

// One valid body, four use-case outcomes: this table is about error mapping and
// nothing else.
func TestRegisterUser_ErrorMapping(t *testing.T) {
	const validBody = `{"email":"ada@example.com","display_name":"Ada Lovelace"}`

	tests := []struct {
		name       string
		regErr     error
		wantStatus int
		wantCode   string
	}{
		{"invalid email", domain.ErrInvalidEmail, http.StatusBadRequest, "invalid_email"},
		{"invalid display name", domain.ErrInvalidDisplayName, http.StatusBadRequest, "invalid_display_name"},
		{"email taken", domain.ErrEmailTaken, http.StatusConflict, "email_taken"},
		{"unexpected failure", errors.New("connection reset by peer"), http.StatusInternalServerError, "internal_error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := &fakeRegistrar{err: tc.regErr}
			rec := serve(t, reg, postUsers(validBody))
			assertError(t, rec, tc.wantStatus, tc.wantCode)
			if reg.calls != 1 {
				t.Errorf("use case calls = %d, want 1", reg.calls)
			}
		})
	}
}

// Decoding failures are a different concern: they are rejected before the use
// case is reached at all.
func TestRegisterUser_RejectsMalformedBody(t *testing.T) {
	bodies := map[string]string{
		"not json":      `{`,
		"unknown field": `{"email":"ada@example.com","display_name":"Ada","admin":true}`,
		"oversized":     `{"email":"` + strings.Repeat("a", 64<<10) + `@example.com","display_name":"Ada"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			reg := &fakeRegistrar{}
			rec := serve(t, reg, postUsers(body))
			assertError(t, rec, http.StatusBadRequest, "malformed_body")
			if reg.calls != 0 {
				t.Errorf("use case calls = %d, want 0 — the body never decoded", reg.calls)
			}
		})
	}
}

func assertError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, wantStatus, rec.Body.String())
	}
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal %q: %v", rec.Body.String(), err)
	}
	if body.Code != wantCode {
		t.Errorf("code = %q, want %q", body.Code, wantCode)
	}
	if body.Message == "" {
		t.Error("message is empty; an error body with no message helps nobody")
	}
}

func TestRegisterUser_DoesNotLeakTheUnderlyingError(t *testing.T) {
	reg := &fakeRegistrar{err: errors.New("pq: password authentication failed for user \"oe\"")}
	rec := serve(t, reg, postUsers(`{"email":"ada@example.com","display_name":"Ada"}`))

	if strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("the response leaked a driver error: %s", rec.Body.String())
	}
}

func TestRouter_MethodAndRoute(t *testing.T) {
	reg := &fakeRegistrar{}

	rec := serve(t, reg, httptest.NewRequest(http.MethodGet, "/users", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /users = %d, want 405", rec.Code)
	}

	rec = serve(t, reg, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /nope = %d, want 404", rec.Code)
	}
}
