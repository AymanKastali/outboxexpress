package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

// maxBodyBytes is generous for two short strings and small enough that a
// malicious body cannot make the process allocate.
const maxBodyBytes = 8 << 10

// Registrar is what this handler needs, declared where it is needed. The use
// case satisfies it without knowing this interface exists.
type Registrar interface {
	Execute(ctx context.Context, cmd application.RegisterUserCommand) (application.RegisterUserResult, error)
}

type Handler struct {
	register Registrar
	ids      application.IDGen
	log      *slog.Logger
}

func NewHandler(register Registrar, ids application.IDGen, log *slog.Logger) *Handler {
	return &Handler{register: register, ids: ids, log: log}
}

func (h *Handler) RegisterUser(w http.ResponseWriter, r *http.Request) {
	meta := h.metadata(r)
	w.Header().Set("X-Correlation-ID", meta.CorrelationID)

	var body registerRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		// Strict on input, tolerant on consumed events: those are different
		// contracts. A client sending a field this API does not know is a client
		// expecting something to happen that will not.
		h.fail(w, http.StatusBadRequest, "malformed_body",
			"request body must be a JSON object with the fields email and display_name")
		return
	}

	res, err := h.register.Execute(r.Context(), application.RegisterUserCommand{
		Email:       body.Email,
		DisplayName: body.DisplayName,
		Meta:        meta,
	})
	if err != nil {
		h.failFor(w, meta, err)
		return
	}

	w.Header().Set("Location", "/users/"+res.UserID.String())
	h.write(w, http.StatusCreated, registerResponse{UserID: res.UserID.String()})
}

// failFor is the whole error contract of this endpoint, in one place you can
// read without following the happy path around it.
func (h *Handler) failFor(w http.ResponseWriter, meta application.Metadata, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidEmail):
		h.fail(w, http.StatusBadRequest, "invalid_email", "email is not a valid address")
	case errors.Is(err, domain.ErrInvalidDisplayName):
		h.fail(w, http.StatusBadRequest, "invalid_display_name", "display name is empty or too long")
	case errors.Is(err, domain.ErrEmailTaken):
		h.fail(w, http.StatusConflict, "email_taken", "that email is already registered")
	default:
		// The client gets a code and nothing else. The operator gets everything.
		h.log.Error("register user failed", "correlation_id", meta.CorrelationID, "error", err)
		h.fail(w, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}
}

func (h *Handler) metadata(r *http.Request) application.Metadata {
	correlation := r.Header.Get("X-Correlation-ID")
	if correlation == "" {
		if id, err := h.ids.New(); err == nil {
			correlation = id.String()
		}
	}
	return application.Metadata{
		CorrelationID: correlation,
		Traceparent:   r.Header.Get("traceparent"),
	}
}

func (h *Handler) fail(w http.ResponseWriter, status int, code, message string) {
	h.write(w, status, errorResponse{Code: code, Message: message})
}

func (h *Handler) write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent; there is nowhere to report this but
		// the log.
		h.log.Error("write response", "error", err)
	}
}
