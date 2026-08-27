package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
)

// Spec §12.1's table, one row per line, against franz-go's own error values.
// Getting this wrong in either direction is a real incident: classifying a
// broker outage as permanent dead-letters a backlog, and classifying a
// too-large record as transient retries it forever behind a growing queue.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		// Transient, and Kafka agrees.
		{"not enough replicas", kerr.NotEnoughReplicas, application.ErrTransient},
		{"leader not available", kerr.LeaderNotAvailable, application.ErrTransient},
		{"request timed out", kerr.RequestTimedOut, application.ErrTransient},

		// Permanent, and Kafka agrees.
		{"message too large", kerr.MessageTooLarge, application.ErrPermanent},
		{"record list too large", kerr.RecordListTooLarge, application.ErrPermanent},
		{"topic authorization failed", kerr.TopicAuthorizationFailed, application.ErrPermanent},
		{"invalid record", kerr.InvalidRecord, application.ErrPermanent},

		// Permanent, and Kafka does *not* agree: the override of §12.1, valid
		// only because auto-creation is disabled.
		{"unknown topic or partition", kerr.UnknownTopicOrPartition, application.ErrPermanent},
		{"unknown topic id", kerr.UnknownTopicID, application.ErrPermanent},

		// Client-side: the request never got out, or the client gave up on it.
		// None of these is about this message.
		{"record timeout", kgo.ErrRecordTimeout, application.ErrTransient},
		{"record retries", kgo.ErrRecordRetries, application.ErrTransient},
		{"client closed", kgo.ErrClientClosed, application.ErrTransient},
		{"context deadline", context.DeadlineExceeded, application.ErrTransient},

		// Unrecognised. Transient is the safe default: the row stays pending
		// behind a backoff, so a surprise costs latency. Defaulting to permanent
		// would turn one unrecognised error into a parked event nobody is
		// looking for.
		{"something nobody anticipated", errors.New("¯\\_(ツ)_/¯"), application.ErrTransient},

		// Wrapped, because franz-go wraps: "no partitions available after
		// attempting to refresh metadata 1 times, last err: UNKNOWN_TOPIC_OR_PARTITION".
		{"wrapped unknown topic", fmt.Errorf("no partitions available: %w",
			kerr.UnknownTopicOrPartition), application.ErrPermanent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.err)
			if !errors.Is(got, tc.want) {
				t.Errorf("classify(%v) = %v, want it to wrap %v", tc.err, got, tc.want)
			}
			// The original must survive: last_error is what a human reads when
			// deciding whether a parked row is genuinely poison (§12.1).
			if !errors.Is(got, tc.err) {
				t.Errorf("classify(%v) = %v, want the original error still unwrappable", tc.err, got)
			}
		})
	}
}

func TestClassify_NilStaysNil(t *testing.T) {
	if err := classify(nil); err != nil {
		t.Errorf("classify(nil) = %v, want nil", err)
	}
}

// A class is exactly one of the two, never both. A test asserting only "wraps
// ErrPermanent" would pass on an error that wraps both, and the use case's
// switch checks permanent first — so the bug would be silent.
func TestClassify_IsNeverBothClasses(t *testing.T) {
	for _, err := range []error{kerr.NotEnoughReplicas, kerr.MessageTooLarge, kerr.UnknownTopicOrPartition} {
		got := classify(err)
		transient := errors.Is(got, application.ErrTransient)
		permanent := errors.Is(got, application.ErrPermanent)
		if transient == permanent {
			t.Errorf("classify(%v): transient=%v permanent=%v, want exactly one", err, transient, permanent)
		}
	}
}

// Map iteration order is random in Go. A record whose header order changes
// between runs is a record no test can compare and no operator can diff against
// another, so the adapter sorts.
func TestRecordHeaders_SortsByKey(t *testing.T) {
	got := recordHeaders(map[string]string{
		"traceparent":    "00-abc-def-01",
		"ce_id":          "e1",
		"content-type":   "application/json",
		"correlation_id": "c1",
	})

	want := []string{"ce_id", "content-type", "correlation_id", "traceparent"}
	keys := make([]string, 0, len(got))
	for _, h := range got {
		keys = append(keys, h.Key)
	}
	if !slices.Equal(keys, want) {
		t.Errorf("header keys = %v, want %v", keys, want)
	}
	if string(got[0].Value) != "e1" {
		t.Errorf("ce_id value = %q, want %q", got[0].Value, "e1")
	}
}

func TestRecordHeaders_NoHeadersIsNotOneEmptyHeader(t *testing.T) {
	if got := recordHeaders(nil); len(got) != 0 {
		t.Errorf("recordHeaders(nil) = %v, want empty", got)
	}
}
