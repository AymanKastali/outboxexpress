// Package messaging holds the wire contract: the CloudEvents 1.0 envelope every
// bounded context publishes. From Plan 2 it also holds the transport Message,
// the only type that crosses the context boundary (spec §6.5).
//
// This is the one platform package the application layer may import (spec §6.1),
// because a message contract is an application concern — it is what a use case
// promises the outside world, not a detail of how bytes reach a broker.
package messaging

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	SpecVersion     = "1.0"
	DataContentType = "application/json"

	// TimeFormat is RFC 3339 with fixed millisecond precision. time.RFC3339Nano
	// trims trailing zeros, which would make the wire format of a timestamp
	// depend on its value — a needless variation in a published contract.
	TimeFormat = "2006-01-02T15:04:05.000Z07:00"
)

// Attributes are the CloudEvents context attributes the caller supplies.
// Everything else in the envelope is constant or derived from these.
type Attributes struct {
	ID          string
	Type        string
	Source      string
	Subject     string
	Time        time.Time
	SchemaName  string // e.g. "accounts/user.registered"
	SchemaBase  string
	Version     int
	Traceparent string
}

// cloudEvent is the wire format. Field order here is field order in the emitted
// JSON, which is what makes a payload byte-comparable in a test.
type cloudEvent struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Type            string `json:"type"`
	Source          string `json:"source"`
	Subject         string `json:"subject"`
	Time            string `json:"time"`
	DataSchema      string `json:"dataschema"`
	DataContentType string `json:"datacontenttype"`
	Traceparent     string `json:"traceparent,omitempty"`
	Data            any    `json:"data"`
}

// EncodeCloudEvent serialises one envelope. Every context calls this, so there
// is one definition of the published format rather than one per context.
func EncodeCloudEvent(attrs Attributes, data any) ([]byte, error) {
	payload, err := json.Marshal(cloudEvent{
		SpecVersion: SpecVersion,
		ID:          attrs.ID,
		Type:        attrs.Type,
		Source:      attrs.Source,
		Subject:     attrs.Subject,
		Time:        attrs.Time.UTC().Format(TimeFormat),
		DataSchema: fmt.Sprintf("%s/%s/%d.json",
			attrs.SchemaBase, attrs.SchemaName, attrs.Version),
		DataContentType: DataContentType,
		Traceparent:     attrs.Traceparent,
		Data:            data,
	})
	if err != nil {
		return nil, fmt.Errorf("messaging: marshal %s: %w", attrs.Type, err)
	}
	return payload, nil
}
