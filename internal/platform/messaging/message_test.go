package messaging_test

import (
	"testing"

	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// The header names are a published contract, so they get the same treatment as
// the envelope's field order: pinned, so that a rename shows up as a failing
// test rather than as a consumer that silently stops finding a header.
// A slice rather than a map keyed by the constant: two constants that
// accidentally shared a value would collapse into one map entry and the test
// would still pass — the one failure mode this test exists to catch.
func TestHeaderNames(t *testing.T) {
	tests := []struct{ got, want string }{
		{messaging.HeaderEventID, "ce_id"},
		{messaging.HeaderEventType, "ce_type"},
		{messaging.HeaderSpecVersion, "ce_specversion"},
		{messaging.HeaderSchemaVersion, "schema_version"},
		{messaging.HeaderContentType, "content-type"},
		{messaging.HeaderCorrelationID, "correlation_id"},
		{messaging.HeaderTraceparent, "traceparent"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("header name = %q, want %q", tc.got, tc.want)
		}
	}
}
