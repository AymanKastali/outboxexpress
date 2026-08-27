//go:build integration

package kafka_test

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AymanKastali/outboxexpress/internal/platform/kafka"
	"github.com/AymanKastali/outboxexpress/internal/platform/kafkatest"
)

// The producer of spec §10.1 against a real Apache Kafka 4.3.1: the options are
// accepted, acks=all is satisfiable on a single broker, and what went in comes
// back out. Everything downstream in this plan assumes this much works.
func TestProducer_ProducesAndTheRecordComesBack(t *testing.T) {
	topic := kafkatest.Topic(t, 1)

	// NewProducer directly rather than kafkatest.Producer: this is the one test
	// whose subject is the constructor, and a helper that wraps it would hide
	// what is being asserted.
	client, err := kafka.NewProducer(
		kafka.DefaultProducerConfig([]string{kafkatest.Broker(t)}))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer client.Close()

	want := []byte(`{"specversion":"1.0"}`)
	result := client.ProduceSync(context.Background(), &kgo.Record{
		Topic: topic,
		Key:   []byte("ada"),
		Value: want,
	})
	record, err := result.First()
	if err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}
	if record.Offset != 0 {
		t.Errorf("offset = %d, want 0 on a fresh topic", record.Offset)
	}

	got := kafkatest.Records(t, topic, 1)
	if string(got[0].Value) != string(want) {
		t.Errorf("value = %q, want %q", got[0].Value, want)
	}
	if string(got[0].Key) != "ada" {
		t.Errorf("key = %q, want %q", got[0].Key, "ada")
	}
}
