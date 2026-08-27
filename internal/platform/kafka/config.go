// Package kafka builds the franz-go clients this project uses, so that the
// durability options of spec §10.1 are written down once — with the classic
// Kafka setting each corresponds to — rather than spread across process wiring
// where a missing one is invisible.
package kafka

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ProducerConfig is this project's franz-go producer tuning, in the shape
// platform/postgres uses for PoolConfig.
//
// It holds only the producer's *values*. The producer's *decisions* — acks=all,
// RecordRetries(0), idempotency, compression — are not fields, and that
// asymmetry is the point. PoolConfig exposes every knob because no pgx setting
// can invalidate a guarantee this design makes; here, three of them can. A
// RequiredAcks field is an invitation to write RequiredAcks(LeaderAck()) in one
// process and silently lose §7's durability floor, with no test anywhere to
// notice. NewProducer's doc comment argues each one, and a reader who wants to
// know what the relay produces with reads one function rather than every call
// site.
type ProducerConfig struct {
	// Brokers is the seed list, from KAFKA_BROKERS (§14).
	Brokers []string

	// RequestTimeoutOverhead bounds how long one produce can hold the claiming
	// transaction open (spec §10.1). D6 publishes inside that transaction, so a
	// produce with no ceiling is a transaction with no ceiling — which is why
	// this is a field rather than a constant: the value belongs to whoever owns
	// the transaction, not to this package.
	RequestTimeoutOverhead time.Duration
}

// DefaultProducerConfig returns the tuning every producer in this project starts
// from. Brokers is the only value taken as a parameter, because it is the only
// one that legitimately differs between call sites. Override any other field on
// the returned struct.
func DefaultProducerConfig(brokers []string) ProducerConfig {
	return ProducerConfig{
		Brokers:                brokers,
		RequestTimeoutOverhead: 10 * time.Second, // §10.1
	}
}

// validate reports every problem at once. Configuration errors surface at
// startup, where fixing three of them should take one restart, not three.
func (c ProducerConfig) validate() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("%w: Brokers is empty, set KAFKA_BROKERS", ErrNoBrokers))
	}
	for i, broker := range c.Brokers {
		if strings.TrimSpace(broker) == "" {
			errs = append(errs, fmt.Errorf("blank seed at Brokers[%d]", i))
		}
	}
	if c.RequestTimeoutOverhead <= 0 {
		errs = append(errs, fmt.Errorf("RequestTimeoutOverhead must be positive, got %s",
			c.RequestTimeoutOverhead))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("kafka: producer config: %w", errors.Join(errs...))
}

// ErrNoBrokers is a refusal to start, not a runtime failure: kgo.SeedBrokers()
// with no seeds silently defaults to localhost:9092, so an unset KAFKA_BROKERS
// would otherwise present as a connection error against a host nobody chose.
var ErrNoBrokers = errors.New("kafka: no brokers configured")
