// Package wakeup tells the relay that there is something to publish, so it does
// not have to wait out its idle backoff.
//
// This is an optimisation and never the delivery path (spec §13.1). PostgreSQL
// notifications are not durable and are not delivered to a listener that was
// disconnected when they were sent; the relay's poll loop, with its timeout,
// remains the source of truth. Every failure here is therefore logged and
// dropped.
package wakeup

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// ChannelOutboxNew is the channel the relay listens on.
const ChannelOutboxNew = "outbox_new"

type Notifier struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewNotifier takes no channel. There is one channel, ChannelOutboxNew, declared
// above; a parameter with one possible argument advertises flexibility that does
// not exist, and every call site passing the same constant is that parameter
// admitting it.
func NewNotifier(pool *pgxpool.Pool, log *slog.Logger) *Notifier {
	return &Notifier{pool: pool, log: log}
}

// Notify fires pg_notify outside any transaction. It is called after the commit
// returns, never inside it: NOTIFY inside a transaction is deferred to commit
// anyway, and holding the intent inside the business transaction would tie the
// write path to the notify queue's health.
//
// pg_notify is used rather than NOTIFY because it takes the channel as a bind
// parameter; NOTIFY accepts only a literal, which would mean interpolating the
// constant into the SQL string.
func (n *Notifier) Notify(ctx context.Context) {
	if _, err := n.pool.Exec(ctx, `SELECT pg_notify($1, '')`, ChannelOutboxNew); err != nil {
		n.log.Warn("outbox wakeup failed; the relay will find the row by polling",
			"channel", ChannelOutboxNew, "error", err)
	}
}

var _ application.Wakeup = (*Notifier)(nil)
