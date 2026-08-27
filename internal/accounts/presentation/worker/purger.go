package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// Purger runs the outbox purge on a ticker (spec §13.2, PURGE_INTERVAL).
//
// It is a separate loop from the relay rather than a step inside a pass, because
// a purge that shares the pass loop would either delay publishing or run far more
// often than a retention measured in hours needs it to.
type Purger struct {
	purge    purger
	log      *slog.Logger
	interval time.Duration
}

func NewPurger(purge purger, log *slog.Logger, interval time.Duration) *Purger {
	return &Purger{purge: purge, log: log, interval: interval}
}

// Run purges every interval until ctx ends, starting immediately.
//
// Immediately, rather than after the first tick: with PURGE_INTERVAL at a minute
// it makes little difference to a running system, but a process that does nothing
// observable for its first tick is a process nobody can smoke-test.
func (p *Purger) Run(ctx context.Context) error {
	p.log.Info("outbox purger started", "interval", p.interval)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		p.runOnce(ctx)

		select {
		case <-ctx.Done():
			p.log.Info("outbox purger stopped")
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Purger) runOnce(ctx context.Context) {
	res, err := p.purge.Execute(ctx)
	switch {
	case err != nil && ctx.Err() != nil:
		// Shutting down mid-purge. The rows it did not delete are still
		// published and still purgeable on the next process's first tick.
		return
	case err != nil:
		// Not fatal: a purge is hygiene, and failing it costs disk rather than
		// correctness. res still carries whatever was deleted before the error.
		p.log.Error("outbox purge failed",
			"error", err, "deleted", res.Deleted, "passes", res.Passes)
	case res.Deleted > 0:
		// passes is on the line because it is how you notice a retention that
		// is too long for the batch size: hundreds of passes a minute means the
		// table is not keeping up.
		p.log.Info("outbox purged", "deleted", res.Deleted, "passes", res.Passes)
	default:
		p.log.Debug("outbox purge found nothing", "passes", res.Passes)
	}
}

// purger is the use case this loop drives, declared at its consumer.
type purger interface {
	Execute(ctx context.Context) (application.PurgeResult, error)
}
