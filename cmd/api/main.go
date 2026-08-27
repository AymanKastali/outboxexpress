// Command api is the accounts context's write path. It writes the user row and
// its outbox row in one transaction and returns; it never talks to a broker,
// which is why a broker outage cannot make it fail (spec §11.1).
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	accountspg "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/postgres"
	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	httpapi "github.com/AymanKastali/outboxexpress/internal/accounts/presentation/http"
	"github.com/AymanKastali/outboxexpress/internal/platform/admin"
	"github.com/AymanKastali/outboxexpress/internal/platform/clock"
	"github.com/AymanKastali/outboxexpress/internal/platform/config"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/logging"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
	"github.com/AymanKastali/outboxexpress/migrations"
)

const (
	maxDBConns      = 10
	shutdownTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI(os.Getenv)
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := platformpg.NewPool(ctx, platformpg.DefaultPoolConfig(cfg.AccountsDatabaseURL, maxDBConns))
	if err != nil {
		return err
	}
	defer pool.Close()

	expectedSchema, err := migrations.Latest(migrations.Accounts)
	if err != nil {
		return err
	}

	// The wiring, in dependency order. This function is the only place in the
	// process where a concrete type meets a port.
	generator := ids.UUIDv7{}
	envelopes := application.NewCloudEventFactory(generator)
	uow := accountspg.NewUnitOfWork(pool, envelopes)
	notifier := wakeup.NewNotifier(pool, log)
	registerUser := application.NewRegisterUser(uow, clock.System{}, generator, notifier)
	handler := httpapi.NewHandler(registerUser, generator, log)

	// admin.Healthz is passed to both listeners: one implementation, two mounts
	// (spec §13.5).
	health := admin.Healthz()

	public := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewRouter(handler, health),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	adminSrv := &http.Server{
		Addr: cfg.AdminAddr,
		Handler: admin.Router(func(ctx context.Context) error {
			return migrations.Ready(ctx, pool, expectedSchema)
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("api starting",
		"http", cfg.HTTPAddr, "admin", cfg.AdminAddr, "expected_schema", expectedSchema)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return listen(public, "public") })
	g.Go(func() error { return listen(adminSrv, "admin") })
	g.Go(func() error {
		<-gctx.Done()
		log.Info("api shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		// Drain in-flight requests first: a request that has committed but not
		// yet responded is a request whose client does not know it succeeded.
		// Arguments evaluate left to right, so public still drains first.
		return errors.Join(public.Shutdown(shutdownCtx), adminSrv.Shutdown(shutdownCtx))
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("api stopped")
	return nil
}

func listen(srv *http.Server, name string) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	return nil
}
