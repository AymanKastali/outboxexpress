package application

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// PublishPendingBatch is the relay pass of spec §11.2, and the pattern itself.
//
// It has no database, no broker and no clock of its own, which is the point:
// every property the outbox pattern claims — publish in id order, break the
// batch on a transient error, never park a row for one, mark only after a
// durable ack — is a property of this file, provable with fakes.
//
// Publishing happens *inside* the claiming transaction (D6), §7's recommended
// default. SKIP LOCKED holds the claim for exactly as long as the transaction, so
// committing before the ack would release the only thing stopping a second relay
// from publishing the row — mutual exclusion with no lease to tune and no reaper
// to write.
//
// It does not shrink the crash window. Between the broker's ack and the commit a
// crash loses every mark this pass has made, so it republishes every row it had
// already acked — the blast radius is BatchSize, not one row. That window cannot
// be closed at all: closing it would need an atomic commit across Kafka and
// PostgreSQL, which is the problem the outbox exists to avoid. It is why event_id
// must be stable and why the consumer needs an inbox.
type PublishPendingBatch struct {
	uow       PublishUnitOfWork
	publisher EventPublisher
	topics    Topics
	clock     Clock
	policy    PublishPolicy
}

// PublishPolicy is the tuning of one pass: how much to claim, how long a
// transient failure waits, and when a still-retrying row starts being counted as
// stuck.
type PublishPolicy struct {
	BatchSize   int
	Schedule    Schedule
	MaxAttempts int
}

func NewPublishPendingBatch(uow PublishUnitOfWork, publisher EventPublisher,
	topics Topics, clk Clock, policy PublishPolicy) *PublishPendingBatch {
	return &PublishPendingBatch{
		uow:       uow,
		publisher: publisher,
		topics:    topics,
		clock:     clk,
		policy:    policy,
	}
}

func (uc *PublishPendingBatch) Execute(ctx context.Context) (PublishResult, error) {
	started := uc.clock.Now()
	res := PublishResult{BatchSize: uc.policy.BatchSize}

	err := uc.uow.Do(ctx, func(w PublishWork) error {
		// Claim by state, never by cursor — see the SQL in outbox_relay.go for
		// what a cursor loses (spec §11.2).
		batch, err := w.Outbox.ClaimPending(ctx, uc.policy.BatchSize)
		if err != nil {
			return err
		}
		res.Claimed = len(batch)

		if err := uc.publishBatch(ctx, w.Outbox, batch, started, &res); err != nil {
			return err
		}

		// One stats query, on the transaction the pass already opened, so
		// observation costs one round trip rather than a background collector
		// holding its own connection (spec §13.3).
		stats, err := w.Outbox.Stats(ctx, uc.policy.MaxAttempts)
		if err != nil {
			return err
		}
		res.Stats = stats
		return nil
	})
	if err != nil {
		// A pass that rolled back observed nothing, and saying otherwise would
		// put counts on a log line for work that did not happen.
		return PublishResult{}, err
	}

	res.Duration = uc.clock.Now().Sub(started)
	return res, nil
}

// publishBatch publishes and marks each row in turn, stopping early on a
// transient failure.
//
// Strictly in id order, sequentially. id is the ordering authority (§8.1) and
// per-aggregate ordering is a stated guarantee (§12.4), so this loop must not
// become a fan-out however tempting the throughput.
//
// It is a method rather than a loop inside Execute's closure so that "stop the
// batch" is a plain return instead of a label jumping out of a switch inside a
// loop inside a closure. A returned error means abandon the pass; returning nil
// after an early stop is what lets the stats query still run.
func (uc *PublishPendingBatch) publishBatch(ctx context.Context, outbox OutboxRepository,
	batch []PendingMessage, started time.Time, res *PublishResult) error {
	for _, pending := range batch {
		err := uc.publish(ctx, pending)

		switch {
		case err == nil:
			// Only now, after the broker has durably acknowledged it.
			if err := outbox.MarkPublished(ctx, pending.ID); err != nil {
				return err
			}
			res.Published++

		case errors.Is(err, ErrPermanent):
			// This message's fault, and waiting will not help. Park it and keep
			// draining: one poison message must not stop the queue (spec §12.1).
			failure := failureOf(pending, err)
			if err := outbox.MarkFailed(ctx, pending.ID, failure.Reason); err != nil {
				return err
			}
			res.Permanent = append(res.Permanent, failure)

		case errors.Is(err, ErrTransient):
			// Not this message's fault. Push the row out by a backoff and stop
			// the batch — every remaining row is about to fail the same way, and
			// trying them all would burn a whole batch's attempts on one outage
			// (spec §12.1).
			//
			// The row stays pending. That it does is D8, and it is why an hour of
			// Kafka being down costs latency rather than a backlog of dead
			// letters.
			failure := failureOf(pending, err)
			availableAt := started.Add(uc.policy.Schedule.After(pending.Attempts))
			if err := outbox.Reschedule(ctx, pending.ID, availableAt, failure.Reason); err != nil {
				return err
			}
			res.Transient = append(res.Transient, failure)
			return nil

		default:
			// Unclassified: an adapter that returns an error wrapping neither
			// class has a bug. Abandon the pass — the transaction rolls back,
			// every row stays pending, and nothing has been marked on a guess.
			// The alternative is guessing, and one of the two guesses loses
			// messages.
			return err
		}
	}
	return nil
}

// publish maps one row onto the wire and hands it to the broker. It is separate
// from Execute so that the mapping — which is contract, not control flow — reads
// on its own.
func (uc *PublishPendingBatch) publish(ctx context.Context, pending PendingMessage) error {
	// Routing before producing: an unmapped aggregate type is a permanent error
	// (spec §9.2), and finding that out before the network call means the row is
	// parked without a wasted round trip.
	topic, err := uc.topics.For(pending.AggregateType)
	if err != nil {
		return err
	}
	// Formatted once: it is both a field and a header, and uuid.String allocates.
	eventID := pending.EventID.String()
	return uc.publisher.Publish(ctx, messaging.Message{
		EventID:       eventID,
		EventType:     pending.EventType,
		SchemaVersion: pending.SchemaVersion,
		// aggregate_id is the partition key. This one line is what buys
		// per-entity ordering: all events for one user land on one partition
		// (spec §9.2).
		Key:     pending.AggregateID,
		Payload: pending.Payload,
		Headers: publishHeaders(pending, eventID),
		Topic:   topic,
	})
}

// publishHeaders composes the header set of spec §9.2.
//
// The contract headers are built here, in the application layer, rather than in
// the Kafka adapter: which headers a consumer may rely on is message contract,
// and §6.4 puts the message contract in this layer. What is left for the adapter
// — turning a map into the broker's own header type — is genuinely Kafka's
// business. It also means the header set is assertable with a fake publisher and
// no broker at all.
//
// Every value comes from a column, never from the payload: a consumer must be
// able to route and deduplicate without deserialising, and the relay must be
// able to compose these without parsing (spec §8.1, §9.2).
//
// The row's stored headers are copied first so that a derived header can never
// be shadowed by one written months ago. §8.1 stores correlation_id and
// traceparent only, but a contract that depends on that staying true forever is
// a contract with a hostage.
func publishHeaders(pending PendingMessage, eventID string) map[string]string {
	headers := make(map[string]string, len(pending.Headers)+5)
	for key, value := range pending.Headers {
		headers[key] = value
	}
	headers[messaging.HeaderEventID] = eventID
	headers[messaging.HeaderEventType] = pending.EventType
	headers[messaging.HeaderSpecVersion] = messaging.SpecVersion
	headers[messaging.HeaderSchemaVersion] = strconv.Itoa(pending.SchemaVersion)
	headers[messaging.HeaderContentType] = messaging.DataContentType
	return headers
}

// failureOf is called only on the two failing branches. Building one for every
// row, including the ones that publish cleanly, allocates a reason string nobody
// reads on the overwhelmingly common path.
func failureOf(pending PendingMessage, err error) Failure {
	return Failure{OutboxID: pending.ID, EventID: pending.EventID, Reason: err.Error()}
}

// PublishResult is everything one pass observed.
//
// Every field spec §13.3 asks the relay to log comes from here rather than from
// a counter incremented in the middle of the loop, which is what makes each one
// assertable in a unit test (§6.1) — and what keeps a logger out of this layer.
type PublishResult struct {
	Claimed   int
	Published int
	Transient []Failure
	Permanent []Failure
	Stats     PassStats
	Duration  time.Duration

	// BatchSize is what this pass was asked to claim, stamped by Execute.
	//
	// It is here so that Full needs no argument. The worker's most consequential
	// branch turns on it — sleeping on a full batch is how a backlog stops
	// draining — and a second copy of the number in the worker's own
	// configuration is a second thing that has to agree. Nothing would fail if
	// they diverged; the relay would just quietly stop draining, or never sleep.
	BatchSize int
}

// Full reports whether the pass filled its batch, which means there is certainly
// more to publish and the worker should not sleep (spec §6.3).
func (r PublishResult) Full() bool {
	return r.BatchSize > 0 && r.Claimed >= r.BatchSize
}

// Failure names a row this pass could not publish.
//
// The reason is a string because it is on its way to two places that both want
// text: the row's last_error column, and a log line. §6.1 keeps the use case
// from logging it here, and §12.1 asks for an alert on a permanent failure, so
// the worker needs enough in this struct to raise one that means something.
type Failure struct {
	OutboxID int64
	EventID  uuid.UUID
	Reason   string
}
