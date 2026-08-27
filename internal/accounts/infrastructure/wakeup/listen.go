package wakeup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Listener is the relay's half of the wakeup: one connection with
// LISTEN outbox_new outstanding, and a wait with a timeout.
//
// It holds its own *pgx.Conn rather than acquiring from the pool, for the hazard
// §13.1 names. PostgreSQL's notify queue is shared and finite, and it can only be
// trimmed past the slowest listener's position — so a session that has executed
// LISTEN and then sits in a long transaction prevents cleanup, and when the queue
// fills, every transaction that calls NOTIFY *fails at commit*. A wedged relay
// would start failing registrations: the exact opposite of what the outbox is
// for. A dedicated connection that never begins a transaction cannot become that
// session. It also keeps the pool's slots for work, and it cannot be recycled out
// from under a wait by MaxConnLifetime.
//
// Usage reports pg_notification_queue_usage() from this same connection, which is
// why the metric §13.1 requires read every pass lives here rather than on the
// outbox stats query: this connection *is* the hazard the number describes. It
// also means the number is simply absent when RELAY_USE_NOTIFY is false, instead
// of being collected and warned about for a mechanism the relay is not using.
type Listener struct {
	dsn  string
	log  *slog.Logger
	conn *pgx.Conn
}

// NewListener does not connect. The first Wait does, so that a relay whose
// database is briefly unreachable at startup still starts and still polls —
// §13.1 makes this an optimisation, and an optimisation that can stop a process
// booting is not one.
func NewListener(dsn string, log *slog.Logger) *Listener {
	return &Listener{dsn: dsn, log: log}
}

// Wait blocks until a notification arrives or timeout elapses, and reports
// whether it was woken.
//
// Every failure degrades to "not woken". This is an optimisation and never the
// delivery path (§13.1), so a caller that treated an error here as fatal would
// have misunderstood the mechanism; the error is returned only so that it can be
// logged. The caller must still poll, and must still wait out its own backoff —
// see the sleep in worker.Relay.wait, which exists because this method returns
// early when it fails.
//
// A wakeup means something was committed. It does not mean a row is still
// pending — another relay may already have taken it — and buffered notifications
// are delivered one per call, so it is not a count either. The next pass decides.
func (l *Listener) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	if err := l.listen(ctx); err != nil {
		return false, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := l.conn.WaitForNotification(waitCtx)
	switch {
	case err == nil:
		return true, nil

	case ctx.Err() != nil:
		// The caller's context ended, not our timeout. Shutting down is not a
		// failure, and reporting it as one would make every clean stop log a
		// warning.
		return false, nil

	case errors.Is(err, context.DeadlineExceeded):
		// The ordinary case on an idle relay. pgx leaves the connection open and
		// still listening after a deadline, so there is nothing to rebuild —
		// which is what makes a wait on every pass affordable.
		return false, nil

	default:
		// The connection is suspect. Drop it so the next Wait rebuilds it, and
		// let the caller poll in the meantime.
		l.drop(ctx)
		return false, fmt.Errorf("wakeup: wait on %s: %w", ChannelOutboxNew, err)
	}
}

// Usage reports how full PostgreSQL's shared notify queue is, as a fraction, and
// whether the number is available at all.
//
// It is read from the listening connection because that connection is the thing
// the number is about (§13.1): a session with LISTEN outstanding is what stops
// the queue being trimmed. Not available means not connected — the first pass
// before the first Wait, or after a dropped connection — and the caller reports
// the absence rather than a misleading zero.
//
// It never returns an error. §13.1 makes the whole wakeup an optimisation, and a
// failure to read its own health must not reach the delivery path.
// The threshold it is compared against is not here: §13.1's 25% is an
// observability decision, and the layer that writes the log line owns it (see
// notifyQueueWarnAt in presentation/worker). This method reports the reading.
func (l *Listener) Usage(ctx context.Context) (float64, bool) {
	if l.conn == nil || l.conn.IsClosed() {
		return 0, false
	}
	var usage float64
	if err := l.conn.QueryRow(ctx, `SELECT pg_notification_queue_usage()`).Scan(&usage); err != nil {
		l.log.Warn("cannot read the notify queue usage", "error", err)
		return 0, false
	}
	return usage, true
}

// Close releases the connection. It is safe to call on a Listener that never
// connected.
func (l *Listener) Close(ctx context.Context) error {
	if l.conn == nil || l.conn.IsClosed() {
		return nil
	}
	if err := l.conn.Close(ctx); err != nil {
		return fmt.Errorf("wakeup: close listen connection: %w", err)
	}
	l.conn = nil
	return nil
}

// listen connects and subscribes if there is no live connection.
func (l *Listener) listen(ctx context.Context) error {
	if l.conn != nil && !l.conn.IsClosed() {
		return nil
	}

	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		return fmt.Errorf("wakeup: listen connection: %w", err)
	}

	// LISTEN takes an identifier, not a bind parameter — the mirror image of
	// notify.go's reason for using pg_notify, which does take one. Sanitize is
	// how the constant becomes a quoted identifier rather than a concatenation
	// nobody checked.
	channel := pgx.Identifier{ChannelOutboxNew}.Sanitize()
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		_ = conn.Close(ctx)
		return fmt.Errorf("wakeup: listen %s: %w", ChannelOutboxNew, err)
	}

	l.conn = conn
	l.log.Info("listening for outbox wakeups", "channel", ChannelOutboxNew)
	return nil
}

func (l *Listener) drop(ctx context.Context) {
	if l.conn == nil {
		return
	}
	_ = l.conn.Close(ctx)
	l.conn = nil
}
