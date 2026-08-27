package httpapi

import "net/http"

// NewRouter is the public surface of the api process, in one place: one command
// plus the shared liveness handler. Readiness and the chaos hooks live on the
// admin listener, which is bound to loopback (spec §13.5).
//
// health is a parameter rather than a call to platform/admin because
// presentation and infrastructure are siblings — this package does not reach
// into platform, and §13.5's "one implementation" is preserved by main passing
// the same handler it mounts on the admin listener.
//
// The return type is http.Handler, not *http.ServeMux: routes are this
// package's business, and handing back something a caller can add to would make
// the surface above only half the answer.
func NewRouter(h *Handler, health http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", h.RegisterUser)
	mux.Handle("GET /healthz", health)
	return mux
}
