package application

import (
	"errors"
	"fmt"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

const (
	// This context's identity on the wire. Constants rather than constructor
	// parameters: there is one accounts service, no configuration path sets
	// either value, and a parameter with one possible argument advertises
	// flexibility that does not exist.
	source     = "/services/accounts"
	schemaBase = "https://schemas.outboxexpress.dev"
)

var ErrUnmappedEvent = errors.New("application: no message mapping for event type")

// CloudEventFactory maps this context's domain events onto the message contract.
//
// The mapping is an explicit type switch rather than reflection over the domain
// struct, for two reasons. The domain carries no struct tags (spec §6.1), so it
// has no opinion about JSON; and a wire contract that changes silently when
// someone renames a domain field is not a contract.
type CloudEventFactory struct {
	ids IDGen
}

func NewCloudEventFactory(ids IDGen) CloudEventFactory {
	return CloudEventFactory{ids: ids}
}

type userRegisteredData struct {
	UserID       string `json:"user_id"`
	Email        string `json:"email"`
	DisplayName  string `json:"display_name"`
	Version      int    `json:"version"`
	RegisteredAt string `json:"registered_at"`
}

func (f CloudEventFactory) From(events []domain.Event, meta Metadata) ([]Envelope, error) {
	if len(events) == 0 {
		return nil, nil
	}

	// Loop-invariant: every envelope from one transaction carries the same
	// correlation and trace. The map is shared deliberately — nothing mutates it
	// after this point, and it is written to a JSONB column as-is.
	h := headers(meta)

	envelopes := make([]Envelope, 0, len(events))
	for _, event := range events {
		data, schemaName, err := mapData(event)
		if err != nil {
			return nil, err
		}

		// One id per event, minted here, at insert time, and never again:
		// regenerating it per publish attempt is the single most common way to
		// break the pattern while appearing to implement it (spec §8.1).
		eventID, err := f.ids.New()
		if err != nil {
			return nil, fmt.Errorf("application: event id: %w", err)
		}

		occurred := event.OccurredAt().UTC()
		payload, err := messaging.EncodeCloudEvent(messaging.Attributes{
			ID:          eventID.String(),
			Type:        event.EventType(),
			Source:      source,
			Subject:     event.AggregateID(),
			Time:        occurred,
			SchemaName:  schemaName,
			SchemaBase:  schemaBase,
			Version:     event.SchemaVersion(),
			Traceparent: meta.Traceparent,
		}, data)
		if err != nil {
			return nil, err
		}

		envelopes = append(envelopes, Envelope{
			EventID:       eventID,
			AggregateType: event.AggregateType(),
			AggregateID:   event.AggregateID(),
			EventType:     event.EventType(),
			SchemaVersion: event.SchemaVersion(),
			Payload:       payload,
			Headers:       h,
			OccurredAt:    occurred,
		})
	}
	return envelopes, nil
}

// mapData is the only context-specific part of the envelope: which domain event
// becomes which `data` shape, and which schema names it.
func mapData(event domain.Event) (any, string, error) {
	switch e := event.(type) {
	case domain.UserRegistered:
		return userRegisteredData{
			UserID:       e.UserID.String(),
			Email:        e.Email,
			DisplayName:  e.DisplayName,
			Version:      e.Version,
			RegisteredAt: e.RegisteredAt.UTC().Format(messaging.TimeFormat),
		}, "accounts/user.registered", nil
	default:
		return nil, "", fmt.Errorf("%w: %s (%T)", ErrUnmappedEvent, event.EventType(), event)
	}
}

// headers holds only what has no column of its own. The relay composes ce_id,
// ce_type, ce_specversion, schema_version and content-type from the row itself
// at publish time (spec §9.2, §11.2).
func headers(meta Metadata) map[string]string {
	h := make(map[string]string, 2)
	if meta.CorrelationID != "" {
		h["correlation_id"] = meta.CorrelationID
	}
	if meta.Traceparent != "" {
		h["traceparent"] = meta.Traceparent
	}
	return h
}

// Compile-time proof that the factory satisfies the port. A test would catch
// this too, but a wiring mistake should not require running tests to find.
var _ EnvelopeFactory = CloudEventFactory{}
