//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	accountskafka "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/kafka"
	accountspg "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/postgres"
	"github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/wakeup"
	"github.com/AymanKastali/outboxexpress/internal/accounts/presentation/worker"
	"github.com/AymanKastali/outboxexpress/internal/platform/backoff"
	"github.com/AymanKastali/outboxexpress/internal/platform/clock"
	"github.com/AymanKastali/outboxexpress/internal/platform/ids"
	"github.com/AymanKastali/outboxexpress/internal/platform/kafkatest"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
	"github.com/AymanKastali/outboxexpress/internal/platform/pgtest"
)

// The whole producing end, wired as cmd/relay wires it, against a real
// PostgreSQL and a real Kafka: register users through the use case, run one relay
// pass, and find the events on the topic with their rows marked published.
//
// This is the test that would notice the pattern being broken end to end, as
// opposed to each half being right on its own.
func TestRelayPath(t *testing.T) {
	dsn, pool := pgtest.Accounts(t)
	topic := kafkatest.Topic(t, 3)
	ctx := context.Background()

	// The write path, as cmd/api wires it.
	generator := ids.UUIDv7{}
	registerUser := application.NewRegisterUser(
		accountspg.NewUnitOfWork(pool, application.NewCloudEventFactory(generator)),
		clock.System{},
		generator,
		noWakeup{},
	)

	registered := []string{"ada@example.com", "grace@example.com", "alan@example.com"}
	for _, email := range registered {
		_, err := registerUser.Execute(ctx, application.RegisterUserCommand{
			Email:       email,
			DisplayName: "Test Person",
			Meta:        application.Metadata{CorrelationID: "corr-" + email},
		})
		if err != nil {
			t.Fatalf("register %s: %v", email, err)
		}
	}

	// Three rows, all pending, nothing published — the state Plan 1 left behind.
	if users, outbox := pgtest.CountAccounts(t, pool); users != 3 || outbox != 3 {
		t.Fatalf("users=%d outbox=%d, want 3 and 3", users, outbox)
	}
	if pending := countByStatus(t, pool, "pending"); pending != 3 {
		t.Fatalf("pending = %d, want 3", pending)
	}

	// The relay pass, as cmd/relay wires it.
	pass := application.NewPublishPendingBatch(
		accountspg.NewPublishUnitOfWork(pool),
		accountskafka.NewPublisher(kafkatest.Producer(t)),
		application.Topics{domain.AggregateTypeUser: topic},
		clock.System{},
		application.PublishPolicy{
			BatchSize:   100,
			Schedule:    backoff.NewExponential(time.Second, time.Minute),
			MaxAttempts: 10,
		})

	// Driven through worker.Relay rather than by calling pass.Execute directly,
	// because this file lives in the worker package and the loop is the part of
	// it that only an integration test can exercise: a real Listener, a real
	// wakeup, a real pass. countingPass stops the loop after one pass so the test
	// does not have to wait for a timeout.
	counted := &countingPass{inner: pass}
	loopCtx, stopLoop := context.WithCancel(ctx)
	counted.stopAfter, counted.stop = 1, stopLoop

	listener := wakeup.NewListener(dsn, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = listener.Close(ctx) })

	relay := worker.NewRelay(counted, listener, backoff.Factor,
		slog.New(slog.DiscardHandler), worker.RelayPolicy{
			IdleMin: 10 * time.Millisecond,
			IdleMax: 50 * time.Millisecond,
		})
	if err := relay.Run(loopCtx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	res := counted.last
	if res.Claimed != 3 || res.Published != 3 {
		t.Fatalf("claimed=%d published=%d, want 3 and 3; failures %v / %v",
			res.Claimed, res.Published, res.Transient, res.Permanent)
	}

	// Every row is published and nothing is pending.
	if pending := countByStatus(t, pool, "pending"); pending != 0 {
		t.Errorf("pending = %d after a successful pass, want 0", pending)
	}
	if published := countByStatus(t, pool, "published"); published != 3 {
		t.Errorf("published = %d, want 3", published)
	}
	// The backlog the pass reported is the backlog after its own marks, because
	// the stats query runs on the same transaction.
	if res.Stats.Backlog != 0 {
		t.Errorf("reported backlog = %d, want 0", res.Stats.Backlog)
	}

	// And the events are on the topic, with the headers a consumer will route
	// and deduplicate on (§9.2).
	records := kafkatest.Records(t, topic, 3)
	for _, record := range records {
		headers := map[string]string{}
		for _, h := range record.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers[messaging.HeaderEventType] != domain.EventTypeUserRegistered {
			t.Errorf("ce_type = %q, want %q", headers[messaging.HeaderEventType], domain.EventTypeUserRegistered)
		}
		if headers[messaging.HeaderSpecVersion] != messaging.SpecVersion {
			t.Errorf("ce_specversion = %q, want %q", headers[messaging.HeaderSpecVersion], messaging.SpecVersion)
		}
		if headers[messaging.HeaderSchemaVersion] != "1" {
			t.Errorf("schema_version = %q, want 1", headers[messaging.HeaderSchemaVersion])
		}
		if headers[messaging.HeaderCorrelationID] == "" {
			t.Error("correlation_id is missing; the trace stops at the broker")
		}

		// event_id is minted once, at insert, and the header must be the same
		// id the payload carries (§8.1). Regenerating it per attempt is, per §4,
		// "the single most common way to break the pattern while appearing to
		// implement it correctly".
		var envelope struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Subject string `json:"subject"`
		}
		if err := json.Unmarshal(record.Value, &envelope); err != nil {
			t.Fatalf("payload is not a CloudEvent: %v", err)
		}
		if envelope.ID != headers[messaging.HeaderEventID] {
			t.Errorf("payload id %q != ce_id header %q", envelope.ID, headers[messaging.HeaderEventID])
		}
		// The partition key is the aggregate id, which is the envelope's subject.
		if string(record.Key) != envelope.Subject {
			t.Errorf("key = %q, want the subject %q", record.Key, envelope.Subject)
		}
		if envelope.Type != domain.EventTypeUserRegistered {
			t.Errorf("payload type = %q, want %q", envelope.Type, domain.EventTypeUserRegistered)
		}
	}

	// A second pass finds nothing. This is the mark actually having taken
	// effect, rather than the same rows being republished forever.
	second, err := pass.Execute(ctx)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if second.Claimed != 0 {
		t.Errorf("the second pass claimed %d rows; the marks did not stick", second.Claimed)
	}
}

// A broker outage must not touch the write path, and its rows must drain when the
// broker returns. This is §11.1's "Kafka unavailable for an hour" row, with a
// topic that does not exist standing in for the outage — the point is that a row
// which fails to publish is still there afterwards.
func TestRelayPath_AFailedPublishLeavesTheRowForTheNextPass(t *testing.T) {
	_, pool := pgtest.Accounts(t)
	topic := kafkatest.Topic(t, 1)
	ctx := context.Background()

	generator := ids.UUIDv7{}
	registerUser := application.NewRegisterUser(
		accountspg.NewUnitOfWork(pool, application.NewCloudEventFactory(generator)),
		clock.System{}, generator, noWakeup{})

	if _, err := registerUser.Execute(ctx, application.RegisterUserCommand{
		Email: "ada@example.com", DisplayName: "Ada Lovelace",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	publisher := accountskafka.NewPublisher(kafkatest.Producer(t))

	policy := application.PublishPolicy{
		BatchSize:   100,
		Schedule:    backoff.NewExponential(time.Nanosecond, time.Nanosecond),
		MaxAttempts: 10,
	}

	// First: a routing table pointing nowhere. The topic does not exist and
	// auto-creation is off, so the publish is permanent — but the point here is
	// the row's survival, which a transient failure would show as well.
	failing := application.NewPublishPendingBatch(
		accountspg.NewPublishUnitOfWork(pool), publisher,
		application.Topics{}, clock.System{}, policy)

	first, err := failing.Execute(ctx)
	if err != nil {
		t.Fatalf("failing Execute: %v", err)
	}
	if first.Published != 0 || len(first.Permanent) != 1 {
		t.Fatalf("published=%d permanent=%d, want 0 and 1", first.Published, len(first.Permanent))
	}
	if _, outbox := pgtest.CountAccounts(t, pool); outbox != 1 {
		t.Fatalf("outbox rows = %d; a failed publish must not delete the row", outbox)
	}

	// A parked row is visible to operations. That is the whole reason status
	// 'failed' exists rather than a silent delete.
	if failed := countByStatus(t, pool, "failed"); failed != 1 {
		t.Errorf("failed rows = %d, want 1", failed)
	}

	// Replay is Plan 5's endpoint; the state change it makes is this, and doing
	// it by hand here shows the row genuinely comes back with the same event_id.
	before := eventIDOf(t, pool)
	pgtest.MustExec(t, pool, `UPDATE accounts.outbox SET status = 'pending', attempts = 0, last_error = NULL`)

	working := application.NewPublishPendingBatch(
		accountspg.NewPublishUnitOfWork(pool), publisher,
		application.Topics{domain.AggregateTypeUser: topic}, clock.System{}, policy)

	second, err := working.Execute(ctx)
	if err != nil {
		t.Fatalf("working Execute: %v", err)
	}
	if second.Published != 1 {
		t.Fatalf("published = %d on the retry, want 1", second.Published)
	}
	if after := eventIDOf(t, pool); after != before {
		t.Errorf("event_id changed from %s to %s across a retry; §4 calls "+
			"regenerating it the single most common way to break the pattern", before, after)
	}

	record := kafkatest.Records(t, topic, 1)[0]
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatalf("payload is not a CloudEvent: %v", err)
	}
	if envelope.ID != before {
		t.Errorf("published id = %s, want the original event_id %s", envelope.ID, before)
	}
}

// --- helpers -----------------------------------------------------------------

// noWakeup stands in for the notifier on the write-path side. Registration's
// wakeup is best-effort by design (Plan 1), and these tests drive the relay
// themselves.
type noWakeup struct{}

func (noWakeup) Notify(context.Context) {}

// countingPass wraps the real use case, keeps the last result, and cancels the
// loop's context once it has run enough times — so worker.Relay.Run returns
// without the test waiting out a deadline (spec §15).
type countingPass struct {
	inner     *application.PublishPendingBatch
	stopAfter int
	stop      context.CancelFunc
	calls     int
	last      application.PublishResult
}

func (c *countingPass) Execute(ctx context.Context) (application.PublishResult, error) {
	res, err := c.inner.Execute(ctx)
	c.calls++
	if err == nil {
		c.last = res
	}
	if c.stop != nil && c.calls >= c.stopAfter {
		defer c.stop()
	}
	return res, err
}

func countByStatus(t *testing.T, pool *pgxpool.Pool, status string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM accounts.outbox WHERE status = $1`, status).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", status, err)
	}
	return n
}

func eventIDOf(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(),
		`SELECT event_id::text FROM accounts.outbox ORDER BY id LIMIT 1`).Scan(&id); err != nil {
		t.Fatalf("event_id: %v", err)
	}
	return id
}
