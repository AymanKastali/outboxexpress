package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducer builds the relay's client from cfg (spec §10.1).
//
// Everything below SeedBrokers and RequestTimeoutOverhead is hard-coded rather
// than a ProducerConfig field, because each one is a guarantee this design
// makes and not a value an operator tunes. That is the difference between this
// constructor and postgres.NewPool, where every setting is a field: no pgx
// setting can make a stated guarantee untrue, and three of these can.
//
// Two further options are absent on purpose.
//
// Idempotency is franz-go's default and is never disabled — DisableIdempotentWrite
// is the call we do not make. An idempotent producer implies acks=all, so the two
// settings §7 asks you to set rather than assume are, in this client, the defaults
// you have to work to break. RequiredAcks(AllISRAcks()) is still passed
// explicitly, because a durability floor that is only a default is a durability
// floor nobody reviewing this file can see.
//
// MaxProduceRequestsInflightPerBroker(5) — which spec §10.1 lists — is *not*
// passed, because franz-go refuses it: kgo.NewClient returns "invalid usage of
// MaxProduceRequestsInflightPerBroker with idempotency enabled". The option
// exists only for the non-idempotent path, and the client already uses 5 in
// flight for Kafka v1 and later. The spec's intent is satisfied; its code line
// is not writable.
//
// RecordRetries(0) looks wrong and is deliberate. The outbox *is* the retry
// mechanism: a failed produce leaves the row pending behind a backoff and the
// relay tries again with the same event_id. A client-side retry loop would be a
// second, invisible retry mechanism with different semantics, and — because D6
// publishes inside the claiming transaction — it would hold that transaction
// open across every one of them.
//
// There is no Ping. The relay must tolerate a broker outage at startup exactly
// as it tolerates one mid-flight: a process that refuses to boot while Kafka is
// down cannot be the thing that drains the backlog the moment Kafka returns,
// and §13.4 makes the same argument for not putting broker reachability in a
// readiness probe. franz-go connects lazily, so there is nothing to wait for.
func NewProducer(cfg ProducerConfig) (*kgo.Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		// Below this line are decisions, not settings. See the doc comment.
		kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all — §7's durability floor
		kgo.RecordRetries(0),               // the outbox is the retry
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: producer: %w", err)
	}
	return client, nil
}
