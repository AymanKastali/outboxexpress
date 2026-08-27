// Command relay is the accounts context's polling publisher: it claims outbox
// rows, produces them to Kafka in id order, waits for the durable ack, and marks
// them published (spec §11.2).
//
// Its own process, per §7 and D2: "Deploy it as its own process. Embedding it in
// the API service … couples publishing throughput to request-handling." It is
// also what makes the crash-window demonstration real — you can kill this and
// watch the guarantee hold.
//
// Single active, with a hot standby (D7). SKIP LOCKED makes a second instance
// *safe* — no row is ever published twice concurrently — but not *ordered*: two
// relays can publish two events for one user at the same time and Kafka may
// accept them in either order. Per-aggregate ordering is a stated guarantee here,
// so the relay scales by standby, not by parallelism. README.md says what the
// alternatives cost.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	accountskafka "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/kafka"
	accountspg "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/postgres"
	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	"github.com/AymanKastali/outboxexpress/internal/accounts/presentation/worker"
	"github.com/AymanKastali/outboxexpress/internal/platform/admin"
	"github.com/AymanKastali/outboxexpress/internal/platform/backoff"
	"github.com/AymanKastali/outboxexpress/internal/platform/clock"
	"github.com/AymanKastali/outboxexpress/internal/platform/config"
	platformkafka "github.com/AymanKastali/outboxexpress/internal/platform/kafka"
	"github.com/AymanKastali/outboxexpress/internal/platform/logging"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
	"github.com/AymanKastali/outboxexpress/migrations"
)

const (
	// Smaller than the api's pool: this process runs two loops, one of which
	// holds a connection for the length of a pass. It does not serve requests,
	// so there is no concurrency for a large pool to absorb.
	maxDBConns = 5

	shutdownTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "relay: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadRelay(os.Getenv)
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

	// NewProducer does not connect, on purpose: a relay that refuses to boot
	// while Kafka is down cannot be the thing that drains the backlog the moment
	// Kafka comes back (spec §11.1's "Kafka unavailable for an hour" row).
	kafkaClient, err := platformkafka.NewProducer(
		platformkafka.DefaultProducerConfig(cfg.KafkaBrokers))
	if err != nil {
		return err
	}
	defer kafkaClient.Close()

	// The wiring, in dependency order. This function is the only place in the
	// process where a concrete type meets a port.
	publishPass := application.NewPublishPendingBatch(
		accountspg.NewPublishUnitOfWork(pool),
		accountskafka.NewPublisher(kafkaClient),
		// The routing table of §9.2. One entry, because this context has one
		// aggregate type — which is exactly why KAFKA_TOPIC is a single
		// variable and not a map somebody has to parse.
		application.Topics{domain.AggregateTypeUser: cfg.KafkaTopic},
		clock.System{},
		application.PublishPolicy{
			BatchSize:   cfg.BatchSize,
			Schedule:    backoff.NewExponential(cfg.BackoffBase, cfg.BackoffCap),
			MaxAttempts: cfg.MaxAttempts,
		})

	// Note what this function cannot do: there is no *accountspg type in scope
	// with Append on it. RelayOutbox is only reachable through
	// PublishUnitOfWork.Do, and NewOutboxPurger has exactly one method — so no
	// wiring mistake here can write an outbox row outside a transaction or mark a
	// row nobody claimed.
	purgeRun := application.NewPurgePublished(
		accountspg.NewOutboxPurger(pool), cfg.OutboxRetention, cfg.PurgeBatch)

	wake, closeWake := wakeupFor(cfg, log)
	defer closeWake()

	relayLoop := worker.NewRelay(publishPass, wake, backoff.Factor, log, worker.RelayPolicy{
		IdleMin: cfg.IdleMin,
		IdleMax: cfg.IdleMax,
	})
	purgeLoop := worker.NewPurger(purgeRun, log, cfg.PurgeInterval)

	// Readiness is the pool and the schema, and deliberately not Kafka. §13.4:
	// the whole point is that this system tolerates a broker outage, and a
	// readiness probe that failed on one would turn a tolerated outage into a
	// deployment incident. The relay's job during an outage is to sit there
	// retrying, which is not the same as being unready.
	adminSrv := admin.NewServer(cfg.AdminAddr, func(ctx context.Context) error {
		return migrations.Ready(ctx, pool, expectedSchema)
	})

	log.Info("relay starting",
		"admin", cfg.AdminAddr,
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"use_notify", cfg.UseNotify,
		"expected_schema", expectedSchema)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return relayLoop.Run(gctx) })
	g.Go(func() error { return purgeLoop.Run(gctx) })
	g.Go(func() error { return admin.Listen(adminSrv) })
	g.Go(func() error {
		<-gctx.Done()
		log.Info("relay shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return adminSrv.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("relay stopped")
	return nil
}

// wakeupFor builds the relay's wakeup, honouring RELAY_USE_NOTIFY.
//
// RELAY_USE_NOTIFY=false returns a nil Waiter, which worker.Relay reads as "poll
// on your own timer". Not a no-op implementation: a no-op's whole body would be
// the ctx-aware sleep worker.Relay already contains for its error path, and that
// sleep is the one thing in this design that must not be got wrong — it is what
// keeps a failed wakeup from turning the pass loop into a spin. Written twice, it
// would be tested once.
//
// §13.1 is explicit that dropping the mechanism and accepting poll latency is a
// legitimate production choice, which is why the flag exists at all.
func wakeupFor(cfg config.Relay, log *slog.Logger) (worker.Waiter, func()) {
	if !cfg.UseNotify {
		log.Info("RELAY_USE_NOTIFY=false; polling only, no LISTEN connection")
		return nil, func() {}
	}
	listener := wakeup.NewListener(cfg.AccountsDatabaseURL, log)
	return listener, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := listener.Close(shutdownCtx); err != nil {
			log.Warn("closing the listen connection", "error", err)
		}
	}
}
