package kafka_test

import (
	"errors"
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/platform/kafka"
)

// The option set of spec §10.1 is not obviously legal: franz-go validates
// combinations at construction and rejects some of them outright. This test is
// what caught MaxProduceRequestsInflightPerBroker being unsettable while
// idempotency is on, and it is what would catch the next such option.
func TestNewProducer_TheOptionSetIsOneFranzGoAccepts(t *testing.T) {
	client, err := kafka.NewProducer(kafka.DefaultProducerConfig([]string{"localhost:9092"}))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer client.Close()
}

// NewProducer validates before it builds, so a bad config never reaches
// kgo.NewClient. config_test.go pins what the message says; this pins that the
// constructor consults it at all — the failure mode being guarded against is
// someone adding a field to ProducerConfig and wiring it past validate().
func TestNewProducer_RefusesAConfigThatDoesNotValidate(t *testing.T) {
	_, err := kafka.NewProducer(kafka.DefaultProducerConfig(nil))
	if !errors.Is(err, kafka.ErrNoBrokers) {
		t.Fatalf("err = %v, want ErrNoBrokers", err)
	}
}
