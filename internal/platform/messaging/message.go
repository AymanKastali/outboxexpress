package messaging

// The header names of the Kafka mapping (spec §9.2).
//
// They are constants here, in the package that owns the wire contract, because
// the relay writes them and the notifier reads them. Two bounded contexts
// agreeing on a spelling by each holding their own string literal is how a
// contract breaks quietly: the producer's typo is the consumer's missing header,
// and nothing fails until something silently stops deduplicating.
const (
	HeaderEventID       = "ce_id"
	HeaderEventType     = "ce_type"
	HeaderSpecVersion   = "ce_specversion"
	HeaderSchemaVersion = "schema_version"
	HeaderContentType   = "content-type"
	HeaderCorrelationID = "correlation_id"
	HeaderTraceparent   = "traceparent"
)

// Message is the transport envelope, and the only type that crosses the bounded
// context boundary (spec §6.5). It is deliberately not a shared domain model:
// each context maps it to and from its own types at the edge.
//
// Payload is opaque and stays opaque. The relay is a dumb pipe (spec §4) — it
// never parses business content — which is why nothing here describes the
// payload's shape, and why routing lives in fields beside it rather than in it.
//
// EventID, EventType and SchemaVersion duplicate what is inside Payload on
// purpose. A consumer must be able to route and deduplicate without
// deserialising (spec §9.2), and these are the three fields that let it.
//
// The consumer-side fields of §6.5 — Partition, Offset and Attempt — arrive with
// the consumer that reads them, in Plan 3. A produced message has no offset yet,
// and Attempt is process-local bookkeeping a producer has no version of; a field
// that nothing writes is a field every reader has to guess about.
type Message struct {
	EventID       string
	EventType     string
	SchemaVersion int
	Key           string
	Payload       []byte
	Headers       map[string]string
	Topic         string
}
