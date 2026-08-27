package httpapi

import "net/http"

// NewRouter is the public surface of the api process: one command. cmd/api adds
// the shared liveness handler from platform/admin; readiness and the chaos hooks
// live on the admin listener, which is bound to loopback (spec §13.5).
func NewRouter(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", h.RegisterUser)
	return mux
}
