//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	accountskafka "github.com/AymanKastali/outboxexpress/internal/accounts/infrastructure/kafka"
	"github.com/AymanKastali/outboxexpress/internal/platform/kafkatest"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// Spec §9.2: the key is aggregate_id, and "this is what buys per-entity
// ordering. All events for one user land on one partition." Three partitions, so
// that a single-partition topic cannot make this pass by accident.
func TestPublisher_KeyingPutsOneAggregateOnOnePartition(t *testing.T) {
	topic := kafkatest.Topic(t, 3)
	pub := publisher(t)
	ctx := context.Background()

	// Six messages over three aggregates, interleaved.
	aggregates := []string{"ada", "grace", "alan", "ada", "grace", "ada"}
	for i, aggregate := range aggregates {
		msg := messaging.Message{
			EventID:       "event-" + strconv.Itoa(i),
			EventType:     "com.outboxexpress.accounts.user.registered",
			SchemaVersion: 1,
			Key:           aggregate,
			Payload:       []byte(`{"user":"` + aggregate + `"}`),
			Headers: map[string]string{
				messaging.HeaderEventID:     "event-" + strconv.Itoa(i),
				messaging.HeaderContentType: messaging.DataContentType,
			},
			Topic: topic,
		}
		if err := pub.Publish(ctx, msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	partitionOf := map[string]int32{}
	for _, record := range kafkatest.Records(t, topic, len(aggregates)) {
		key := string(record.Key)
		if seen, ok := partitionOf[key]; ok && seen != record.Partition {
			t.Errorf("key %q landed on partitions %d and %d; per-aggregate ordering "+
				"is not available if one aggregate spans partitions", key, seen, record.Partition)
		}
		partitionOf[key] = record.Partition
	}
	if len(partitionOf) != 3 {
		t.Fatalf("saw %d distinct keys, want 3", len(partitionOf))
	}

	// And the keys must not all be on one partition, or the assertion above
	// would hold on a topic where keying does nothing.
	distinct := map[int32]bool{}
	for _, partition := range partitionOf {
		distinct[partition] = true
	}
	if len(distinct) < 2 {
		t.Errorf("all three keys landed on partition %v; the partitioner is not "+
			"distributing and this test would pass on a broken key", distinct)
	}
}

// The headers survive the hop, in sorted order.
func TestPublisher_HeadersReachTheBrokerSorted(t *testing.T) {
	topic := kafkatest.Topic(t, 1)
	pub := publisher(t)

	err := pub.Publish(context.Background(), messaging.Message{
		Key:     "ada",
		Payload: []byte(`{}`),
		Topic:   topic,
		Headers: map[string]string{
			messaging.HeaderTraceparent:   "00-abc-def-01",
			messaging.HeaderEventID:       "e1",
			messaging.HeaderSchemaVersion: "1",
		},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	record := kafkatest.Records(t, topic, 1)[0]
	var keys []string
	for _, h := range record.Headers {
		keys = append(keys, h.Key)
	}
	want := []string{messaging.HeaderEventID, messaging.HeaderSchemaVersion, messaging.HeaderTraceparent}
	if len(keys) != len(want) {
		t.Fatalf("headers = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("header %d = %q, want %q", i, keys[i], want[i])
		}
	}
}

// Spec §9.2: "An unmapped type is a *permanent* error, so the branch is
// reachable." This is the other half of that — a topic that does not exist, on a
// broker with auto-creation disabled. Kafka marks UNKNOWN_TOPIC_OR_PARTITION
// retriable; §12.1 does not, and this is where that override is proven to matter
// rather than asserted in a comment.
func TestPublisher_AnUnknownTopicIsPermanent(t *testing.T) {
	pub := publisher(t)

	err := pub.Publish(context.Background(), messaging.Message{
		Key:     "ada",
		Payload: []byte(`{}`),
		Topic:   "test.no.such.topic.should.not.be.created",
	})
	if err == nil {
		t.Fatal("Publish succeeded; auto-creation must be disabled on the test broker")
	}
	if !errors.Is(err, application.ErrPermanent) {
		t.Errorf("err = %v, want it to wrap ErrPermanent — a topic that does not "+
			"exist will not start existing, and retrying forever hides the mistake", err)
	}
}

func publisher(t *testing.T) *accountskafka.Publisher {
	t.Helper()
	return accountskafka.NewPublisher(kafkatest.Producer(t))
}
