// Package worker holds the accounts context's driver loops: the things that
// trigger a use case on a schedule rather than on a request.
//
// A worker is structurally an HTTP handler with a ticker for a client (spec
// §6.3). It decides when to call, turns the result into log lines, and owns no
// business rule — which is why the interesting properties of the relay are in
// application/publish_pending_batch.go and the interesting properties of *this*
// file are all about timing and observability.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// notifyQueueWarnAt is §13.1's threshold. Above it, PostgreSQL's shared notify
// queue is filling, and when it fills every transaction that calls NOTIFY fails
// at commit — so this warning is about POST /users, not about the relay.
//
// The threshold lives here and the reading comes from the Waiter port: what
// counts as worth warning about is an observability decision, which is this
// layer's, while the number is a fact about a connection this layer cannot see.
const notifyQueueWarnAt = 0.25

type Relay struct {
	publish publisher
	wake    Waiter
	jitter  func() float64
	log     *slog.Logger
	policy  RelayPolicy

	// idle is the current idle backoff, doubling toward IdleMax while there is
	// nothing to do and resetting the moment there is.
	idle time.Duration
}

// RelayPolicy is the loop's tuning (spec §14: RELAY_BATCH_SIZE, RELAY_IDLE_MIN,
// RELAY_IDLE_MAX).
//
// There is no BatchSize here. The loop needs to recognise a full batch, but the
// pass already stamps the size it used into PublishResult, so asking the result
// is free and a second copy of the number in this struct would be a second thing
// that has to agree with the first. Nothing fails when two copies diverge — the
// relay just stops draining, or never sleeps.
type RelayPolicy struct {
	IdleMin time.Duration
	IdleMax time.Duration

	// DrainGrace is how long a pass already in flight may keep running after the
	// process has been asked to stop (spec §14, RELAY_DRAIN_GRACE). See draining
	// for what it buys. It has to fit inside whatever the orchestrator allows
	// between SIGTERM and SIGKILL, or the kill arrives first and the grace was a
	// promise nobody kept.
	//
	// Zero means no grace, which is the behaviour of a loop that has never heard
	// of shutdown: the pass is cancelled with everything else.
	DrainGrace time.Duration
}

// NewRelay builds the loop.
//
// jitter is a func rather than an interface because one function is the whole
// contract, and backoff.Factor satisfies it without an adapter.
func NewRelay(publish publisher, wake Waiter, jitter func() float64,
	log *slog.Logger, policy RelayPolicy) *Relay {
	return &Relay{
		publish: publish,
		wake:    wake,
		jitter:  jitter,
		log:     log,
		policy:  policy,
		idle:    policy.IdleMin,
	}
}

// Run drives passes until ctx ends.
//
// It returns nil on a clean shutdown: a cancelled context is how this loop is
// asked to stop, and reporting that as an error would make every ordinary stop
// look like a crash in the process's exit status.
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("relay started",
		"idle_min", r.policy.IdleMin,
		"idle_max", r.policy.IdleMax,
		"drain_grace", r.policy.DrainGrace,
		"wakeup", r.wake != nil)

	// Two contexts from here down, and the difference between them is the whole
	// of graceful shutdown. ctx means stop taking new work; pass means abandon
	// the work in hand. Every use of one rather than the other below is a
	// deliberate choice about which of those two things is being asked for.
	pass, abandon := r.draining(ctx)
	defer abandon()

	for {
		if ctx.Err() != nil {
			r.stopping(pass)
			return nil
		}

		res, err := r.publish.Execute(pass)
		if err != nil {
			r.logFailedPass(res, err)
			if ctx.Err() != nil {
				r.stopping(pass)
				return nil
			}
			// Back off the long way — retrying at IdleMin against a database that
			// is already unhappy is how a blip becomes an outage — and try again.
			// The relay is the thing that must not give up.
			r.wait(ctx, r.policy.IdleMax)
			continue
		}

		r.logPass(pass, res)
		r.wait(ctx, r.nextWait(res))
	}
}

// draining returns the context a pass runs under: one that survives ctx by
// DrainGrace, so that a pass already in flight when the signal arrives gets to
// finish and commit.
//
// This is the difference between a restart and a crash, and the reason it needs
// saying is that §11.2 says the crash window cannot be closed. That is true of a
// crash. A SIGTERM is not a crash — it is the most frequent stop there is, once
// per pod per release — and cancelling the pass mid-ack rolls back every mark it
// had made, so each message the broker had already durably accepted is published
// again on the next start. cmd/api makes exactly this argument for draining its
// listener: "a request that has committed but not yet responded is a request
// whose client does not know it succeeded." A produce that is acked but not yet
// marked is that same sentence, about the outbox.
//
// It is http.Server's rule applied to a loop instead of a listener: stop
// accepting, then let what is in flight finish. The loop stops between passes
// because Run checks ctx at the top and wait returns on it at once; only the pass
// itself is given the grace.
//
// The grace has to cover one produce and the marks around it rather than a whole
// batch, because the pass in flight is the only one there will be. Past it the
// pass is cancelled and rolls back — the old behaviour, and stopping says which
// of the two happened.
//
// Nothing here logs, and that is deliberate: the callbacks below run on the
// runtime's goroutines, and a line written from one of them can outlive Run — the
// stop returned by AfterFunc reports that the callback has *started*, and offers
// no way to join it. Keeping every line on Run's own goroutine means Run
// returning is the end of this relay, with nothing still writing behind it.
func (r *Relay) draining(ctx context.Context) (context.Context, context.CancelFunc) {
	// WithoutCancel and then a cancel of our own: the pass must not inherit ctx's
	// cancellation, only reach it through the grace period below.
	pass, abandon := context.WithCancel(context.WithoutCancel(ctx))

	// AfterFunc rather than a goroutine parked on a channel. It runs once if ctx
	// ends, and the stop it returns unregisters it if ctx never does — so a
	// process that runs for a month is not holding a goroutine against the
	// shutdown it has not had.
	stop := context.AfterFunc(ctx, func() {
		expired := time.AfterFunc(r.policy.DrainGrace, abandon)
		// The pass finished inside the grace, which is the ordinary case: drop the
		// timer rather than leave a stopping process holding one.
		context.AfterFunc(pass, func() { expired.Stop() })
	})

	return pass, func() {
		stop()
		abandon()
	}
}

// stopping reports how the relay went down. It is one line rather than none
// because the two ways differ in whether they left duplicates behind, and the
// operator reading it is mid-deploy.
//
// pass is done only if the grace expired: Run's own cancel has not run yet at
// either call site.
func (r *Relay) stopping(pass context.Context) {
	if pass.Err() != nil {
		r.log.Warn("relay stopped, but the drain grace expired with a pass still in "+
			"flight; it rolled back, and whatever it had already published will be "+
			"published again", "drain_grace", r.policy.DrainGrace)
		return
	}
	r.log.Info("relay stopped")
}

// nextWait picks the next sleep from the pass's own result (spec §6.3).
//
//	a full batch      no wait — there is certainly more, and sleeping on a full
//	                  batch is how a backlog stops draining
//	a transient error IdleMax — the broker is unwell, the rows carry their own
//	                  available_at, and hammering it helps nobody
//	some work done    IdleMin — work is arriving, stay responsive
//	nothing at all    the current idle backoff, doubling toward IdleMax
//
// The transient branch comes before the did-work branch on purpose: a pass that
// published four rows and then hit a broker error should back off, not speed up.
func (r *Relay) nextWait(res application.PublishResult) time.Duration {
	switch {
	case res.Full():
		r.idle = r.policy.IdleMin
		return 0

	case len(res.Transient) > 0:
		r.idle = r.policy.IdleMin
		return r.policy.IdleMax

	case res.Claimed > 0:
		r.idle = r.policy.IdleMin
		return r.policy.IdleMin

	default:
		wait := r.idle
		r.idle = min(r.idle*2, r.policy.IdleMax)
		return wait
	}
}

// wait sleeps, or returns early if the api says there is something to publish.
//
// Every non-zero wait is jittered, so that relays which restarted together do not
// fall into step and arrive at the table together (spec §11.2, §12.1). Note that
// this jitters *every* branch of nextWait, not just the idle ladder: the branch
// where it matters most is the transient one, where a fleet of relays is backing
// off IdleMax against a broker that is already unwell.
//
// This is not a second implementation of §12.1's schedule, despite the doubling
// in nextWait. That schedule answers "when may this row be tried again" and is
// persisted in available_at; this answers "when should this process look at the
// table again" and is process-local. They happen to both double.
func (r *Relay) wait(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	d = time.Duration(float64(d) * r.jitter())

	if r.wake != nil {
		woken, err := r.wake.Wait(ctx, d)
		if err == nil {
			if woken {
				// Something was committed, so be responsive again. It does not
				// mean the row is still pending — another relay may have taken
				// it — so the next pass decides, not this line.
				r.idle = r.policy.IdleMin
			}
			return
		}
		// The wakeup is unavailable, and it returned immediately rather than
		// waiting. Falling through to the sleep is what keeps a broken listener
		// from turning the pass loop into a spin against the database: the poll
		// loop is the source of truth (§13.1), and a poll loop has a period.
		r.log.Warn("outbox wakeup unavailable; polling instead", "error", err)
	}

	// No waiter (RELAY_USE_NOTIFY=false), or the waiter just failed. One timer,
	// one place.
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// logFailedPass reports a pass that rolled back.
//
// It is logged before the shutdown check in Run, and that ordering is the point:
// this is the only branch that can leave duplicates behind, and it used to be
// silent on exactly the path — a signal arriving mid-pass — where it happens most.
//
// A rolled-back pass has committed nothing, so every row it claimed is still
// pending and nothing is lost. But published is not zero: those messages were
// durably accepted by the broker before the marks were undone, so each one will
// be published a second time. §11.2 calls that the honest price of D6's lock;
// this line is where the bill is itemised.
func (r *Relay) logFailedPass(res application.PublishResult, err error) {
	r.log.Error("relay pass rolled back; every row it claimed is still pending",
		"error", err,
		"claimed", res.Claimed,
		"republish_on_retry", res.Published,
		"batch_ms", res.Duration.Milliseconds())
}

// logPass emits the one line per pass of spec §13.3 — INFO when it did work,
// DEBUG when it found none, so that an idle relay does not drown the log it will
// be read from during an incident.
//
// There is no metrics system and no /metrics endpoint in this project by design
// (§13.3). Every number here is a field on this line, and every one of them came
// out of a result struct rather than a counter incremented in the middle of
// business logic — which is why each is assertable in a unit test.
func (r *Relay) logPass(ctx context.Context, res application.PublishResult) {
	level := slog.LevelDebug
	if res.Claimed > 0 {
		level = slog.LevelInfo
	}

	attrs := []any{
		"claimed", res.Claimed,
		"published", res.Published,
		"transient_failures", len(res.Transient),
		"permanent_failures", len(res.Permanent),
		"batch_ms", res.Duration.Milliseconds(),
	}

	// The backlog fields, or the reason there are none. They are read after the
	// pass commits and a failure there is tolerated, precisely so that counting
	// rows can never undo delivery (§13.1, and application.OutboxStatsReader) —
	// which means this line has to be able to say the counting failed. Zeroes
	// would be indistinguishable from a healthy idle relay, and that is the one
	// reading an operator must never be handed by accident.
	if res.StatsErr != nil {
		attrs = append(attrs, "stats_error", res.StatsErr.Error())
	} else {
		attrs = append(attrs,
			"backlog", res.Stats.Backlog,
			"oldest_pending_age_ms", res.Stats.OldestPendingAge.Milliseconds(),
			"failed_rows", res.Stats.FailedRows,
			"stuck_rows", res.Stats.StuckRows)
	}

	// §13.1 wants notify queue usage read every pass and §13.3 wants it on this
	// line, so it is appended to this line rather than logged separately. It
	// comes from the waiter, not from the pass: the listening connection is the
	// thing the number is about. With no waiter — RELAY_USE_NOTIFY=false — there
	// is no number, and the field is absent rather than a misleading zero.
	usage, haveUsage := 0.0, false
	if r.wake != nil {
		usage, haveUsage = r.wake.Usage(ctx)
	}
	if haveUsage {
		attrs = append(attrs, "notify_queue_usage", usage)
	}

	r.log.Log(ctx, level, "relay pass", attrs...)

	// §13.1's hazard, watched. Past this point the shared notify queue is
	// filling, and when it fills it is POST /users that starts failing at
	// commit — the exact opposite of what the outbox is for.
	//
	// The threshold is here, in the layer that decides what is worth a warning,
	// and the reading comes from the port. presentation cannot import
	// infrastructure (§6.1), so it could not be otherwise even if it wanted to.
	if haveUsage && usage > notifyQueueWarnAt {
		r.log.Warn("postgres notify queue is filling; NOTIFY can start failing commits",
			"usage", usage,
			"warn_at", notifyQueueWarnAt)
	}

	// §12.1 asks for an alert at this point and §6.1 keeps the use case from
	// raising it, so it is raised here — once per row, with the ids somebody
	// needs to go and look at it.
	for _, failure := range res.Permanent {
		r.log.Error("outbox row parked as failed; a human has to look at it",
			"outbox_id", failure.OutboxID,
			"event_id", failure.EventID,
			"reason", failure.Reason)
	}
	for _, failure := range res.Transient {
		r.log.Warn("publish failed transiently; the row stays pending and will be retried",
			"outbox_id", failure.OutboxID,
			"event_id", failure.EventID,
			"reason", failure.Reason)
	}

	// §13.3: "Pending past RELAY_MAX_ATTEMPTS. Still retrying; look anyway."
	if res.Stats.StuckRows > 0 {
		r.log.Warn("outbox rows are past the attempt threshold and still retrying",
			"stuck_rows", res.Stats.StuckRows)
	}
}

// publisher is the use case this loop drives, declared at its consumer: the loop
// needs one method, and naming the concrete use case here would make it
// untestable without a transaction.
type publisher interface {
	Execute(ctx context.Context) (application.PublishResult, error)
}

// Waiter is the wakeup, declared here at its consumer.
//
// presentation and infrastructure are siblings and neither imports the other
// (spec §6.1), so this file cannot name wakeup.Listener. cmd/relay wires one in.
//
// A nil Waiter means "no wakeup": the loop then polls on its own timer, which is
// exactly what RELAY_USE_NOTIFY=false asks for. Nil rather than a no-op
// implementation because the no-op's only job would be to sleep, and wait below
// already contains that sleep for the error path — a second copy in cmd/relay
// would be the one line that must not get it wrong, written twice and tested
// once.
//
// An implementation must not return before timeout unless it was woken. Listener
// breaks that when it fails, and returns the error saying so; wait below is where
// that is absorbed.
//
// Usage reports the health of the mechanism — pg_notification_queue_usage() for a
// LISTEN-based waiter — and false when there is no number to report. It is on
// this port rather than on the pass's stats because the connection that listens
// is the thing the number describes (§13.1).
type Waiter interface {
	Wait(ctx context.Context, timeout time.Duration) (bool, error)
	Usage(ctx context.Context) (float64, bool)
}
