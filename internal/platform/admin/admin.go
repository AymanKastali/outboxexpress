// Package admin serves the operator surface — liveness and readiness — on a
// listener bound to loopback. It is separate from the public router because an
// unauthenticated surface has no business sharing a port with public traffic
// (spec §13.5). There is no /metrics: this project has no metrics system, and
// every operational signal is a field on a structured log line (spec §13.3).
package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// serverReadHeaderTimeout bounds how long a client may take over its headers.
// The admin listener binds loopback (§13.5) and answers two trivial routes, so
// this is about a stuck connection holding a goroutine, not about an attacker.
const serverReadHeaderTimeout = 5 * time.Second

// readyTimeout bounds the readiness check. A probe that can hang is a probe that
// makes an orchestrator's timeout the real behaviour.
const readyTimeout = 2 * time.Second

// Healthz is liveness: this process is running. It deliberately touches nothing
// else — a liveness probe that checks a dependency restarts a healthy process
// because something else broke.
//
// It is exported because spec §13.5 puts /healthz on the public listener too,
// and one operator contract deserves one implementation: cmd/api mounts this
// same handler on both muxes.
func Healthz() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// Router mounts the operator surface. ready reports whether this process can do
// its job: pools reachable, schema at the expected version. It must not check the
// broker — a broker outage is a condition this system tolerates, and failing
// readiness on it would turn a tolerated outage into a deployment incident
// (spec §13.4).
func Router(ready func(context.Context) error) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("GET /healthz", Healthz())

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := ready(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(err.Error() + "\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	return mux
}

// NewServer builds the admin listener every process in this project runs: /healthz
// from Healthz, /readyz from ready.
//
// It is here rather than in each main because the shutdown rule is the same in all
// of them and there will be four of them. A timeout that only two processes out of
// four apply is a timeout nobody can reason about.
func NewServer(addr string, ready func(context.Context) error) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           Router(ready),
		ReadHeaderTimeout: serverReadHeaderTimeout,
	}
}

// Listen serves until the server is shut down, treating a clean shutdown as
// success. ErrServerClosed is what Shutdown produces, and a process that reported
// it as a failure would exit non-zero on every ordinary stop.
func Listen(srv *http.Server) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("admin listener: %w", err)
	}
	return nil
}
