package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDefaultProducerConfig_IsUsableAsIs(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"})
	if err := cfg.validate(); err != nil {
		t.Fatalf("the defaults must be valid without further tuning: %v", err)
	}
	// Spec §10.1 names this value, and it is the ceiling on how long one produce
	// can hold the claiming transaction open. A default that drifted from the
	// spec would be invisible everywhere else.
	if cfg.RequestTimeoutOverhead != 10*time.Second {
		t.Errorf("RequestTimeoutOverhead = %s, want the 10s of §10.1", cfg.RequestTimeoutOverhead)
	}
}

// A producer with nowhere to produce is a configuration mistake, and franz-go
// would accept it: kgo.SeedBrokers() with no seeds defaults to localhost:9092,
// which turns a missing KAFKA_BROKERS into a mysterious connection failure
// against the wrong host rather than a refusal to start.
func TestProducerConfig_ValidateRefusesAnEmptyBrokerList(t *testing.T) {
	err := DefaultProducerConfig(nil).validate()
	if !errors.Is(err, ErrNoBrokers) {
		t.Fatalf("err = %v, want ErrNoBrokers", err)
	}
	if !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Errorf("err = %q, want it to name the variable an operator has to set", err)
	}
}

// A blank seed is worse than a missing one: kgo.SeedBrokers("") passes
// validation here and fails later inside the client, far from the cause.
func TestProducerConfig_ValidateRefusesABlankBroker(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092", "  "})
	if err := cfg.validate(); err == nil {
		t.Fatal("expected a blank seed to be rejected")
	}
}

func TestProducerConfig_ValidateReportsEveryProblemAtOnce(t *testing.T) {
	err := ProducerConfig{}.validate()
	if err == nil {
		t.Fatal("expected the zero value to be invalid")
	}
	for _, field := range []string{"Brokers", "RequestTimeoutOverhead"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("%s is not named in the error:\n%v", field, err)
		}
	}
}
