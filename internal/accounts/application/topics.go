package application

import (
	"errors"
	"fmt"
)

// Topics is the routing table of spec §9.2: aggregate_type selects the topic.
//
// It is not a port. It is a value object with one rule, and it is in its own file
// because §9.2 is the only reason it ever changes.
//
// It is a map rather than a function because the mapping is configuration — the
// topic name comes from KAFKA_TOPIC — and a map is the smallest thing that can
// be built at wiring time and asserted on in a test.
//
// Routing on aggregate_type is why that value is a column and not a payload
// field: the relay must choose a topic without opening the payload (spec §8.1).
type Topics map[string]string

// For returns the topic for an aggregate type, or a permanent error.
func (t Topics) For(aggregateType string) (string, error) {
	// An entry that is present but empty is a misconfiguration rather than a
	// route: producing to the topic named "" would fail at the broker with
	// something far less legible than this.
	if topic := t[aggregateType]; topic != "" {
		return topic, nil
	}
	return "", fmt.Errorf("%w: %w: %q", ErrPermanent, ErrUnmappedAggregateType, aggregateType)
}

// ErrUnmappedAggregateType is the permanent error spec §9.2 says must be
// reachable: an aggregate type with no topic is a deployment mistake, and one
// that retries forever behind a growing backlog is a deployment mistake nobody
// finds.
var ErrUnmappedAggregateType = errors.New("application: no topic for aggregate type")
