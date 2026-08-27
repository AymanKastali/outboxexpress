// Package kafka publishes the accounts context's messages to the broker. It is
// the only place in this context that knows Kafka exists, and its whole job is
// two translations: a map into Kafka's headers, and a Kafka error into one of the
// application's two error classes.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(client *kgo.Client) *Publisher {
	return &Publisher{client: client}
}

// Publish produces one record and returns only once the broker has durably
// acknowledged it.
//
// ProduceSync, not Produce. §7: "Fire-and-forget publishing plus 'mark
// published' loses messages: the relay records success, the broker never durably
// had the message, and the row is gone from the pending set forever." The client
// is configured with acks=all (spec §10.1), so the ack this waits for means the
// full in-sync replica set has it.
//
// One record per call rather than a batch, because the caller marks each row
// individually and in order: a batched produce would either mark rows the broker
// rejected or need per-record result unpicking that buys nothing here — the
// claiming transaction is already the thing bounding throughput.
func (p *Publisher) Publish(ctx context.Context, msg messaging.Message) error {
	record := &kgo.Record{
		Topic:   msg.Topic,
		Key:     []byte(msg.Key),
		Value:   msg.Payload,
		Headers: recordHeaders(msg.Headers),
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return classify(err)
	}
	return nil
}

// recordHeaders turns the contract's map into Kafka's own header type, sorted by
// key.
//
// Sorted because map iteration order is random in Go, and a record whose header
// order changes between runs is one no test can compare and no operator can diff
// against another. Kafka does not care about the order; the people reading the
// records do.
//
// Composing *which* headers exist is not done here — that is message contract
// and lives in the application layer (spec §6.4). This function's entire job is
// the type change.
func recordHeaders(headers map[string]string) []kgo.RecordHeader {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	out := make([]kgo.RecordHeader, 0, len(keys))
	for _, key := range keys {
		out = append(out, kgo.RecordHeader{Key: key, Value: []byte(headers[key])})
	}
	return out
}

// classify translates a produce error into one of the application's two error
// classes (spec §12.1). It is the reason this adapter exists.
func classify(err error) error {
	if err == nil {
		return nil
	}

	var kafkaErr *kerr.Error
	if !errors.As(err, &kafkaErr) {
		// Not an error the broker sent: the client could not get a request out,
		// timed out, or is shutting down. None of those is about this message.
		//
		// Transient is also the right default for an error nobody anticipated.
		// It leaves the row pending behind a backoff, so the cost of a surprise
		// is latency and a log line. Defaulting to permanent would turn one
		// unrecognised error into a parked event nobody is looking for — and the
		// relay's job is to not lose things.
		return fmt.Errorf("%w: %w", application.ErrTransient, err)
	}
	if permanentCodes[kafkaErr.Code] || !kafkaErr.Retriable {
		return fmt.Errorf("%w: %w", application.ErrPermanent, err)
	}
	return fmt.Errorf("%w: %w", application.ErrTransient, err)
}

// permanentCodes are the codes spec §12.1 treats as permanent although Kafka's
// own protocol marks them retriable.
//
// The base classification is Kafka's Retriable flag, for the reason config.go
// parses LOG_LEVEL with slog's parser: which errors are retriable is a fact Kafka
// owns, and a copy of it here is a second source of truth that goes stale every
// time Kafka adds a code. Checked against franz-go v1.21.6, that flag already
// agrees with §12.1's table on NOT_ENOUGH_REPLICAS, LEADER_NOT_AVAILABLE,
// REQUEST_TIMED_OUT, MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE and
// TOPIC_AUTHORIZATION_FAILED.
//
// It disagrees on the unknown-topic errors, and §12.1 says why: they are
// retriable in the protocol because a topic may be halfway through being
// created. With auto-creation disabled — which deploy/docker-compose.yml and
// kafkatest both set, precisely so this branch is reachable — an unknown topic
// means the topic does not exist and will not start existing, and retrying it
// forever hides a deployment mistake behind a growing backlog.
var permanentCodes = map[int16]bool{
	kerr.UnknownTopicOrPartition.Code: true,
	kerr.UnknownTopicID.Code:          true,
}

var _ application.EventPublisher = (*Publisher)(nil)
