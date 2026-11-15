package events

import (
	"encoding/json"
	"errors"
	"time"
)

// ErrEventTooLarge is returned by Publish for an event whose encoding exceeds MaximumEventBytes.
var ErrEventTooLarge = errors.New("events: event too large")

// SpecVersion is the CloudEvents version of every Event.
const SpecVersion = "1.0"

// ContentType is the media type of a delivery's body: a CloudEvent in structured JSON mode.
const ContentType = "application/cloudevents+json"

// MaximumEventBytes caps an encoded event: Publish refuses a larger one with ErrEventTooLarge,
// and the inbox answers 413. Events carry references, not documents (J2).
const MaximumEventBytes = 256 << 10

// Event is the envelope of every event: CloudEvents 1.0 in structured JSON mode (E3), with the
// extensions traceparent and tracestate (W3C trace context) and actor (the publisher's acting
// identity, audit information only, E9).
type Event struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`     // a UUIDv7 from Publish; unique per event, the inbox deduplicates by it
	Source          string          `json:"source"` // the publishing service
	Type            string          `json:"type"`   // the topic name
	Time            time.Time       `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
	TraceParent     string          `json:"traceparent,omitempty"`
	TraceState      string          `json:"tracestate,omitempty"`
	Actor           string          `json:"actor,omitempty"`
}

// Message is the typed view of a delivered event that a subscription's handler receives.
type Message[P any] struct {
	ID     string
	Source string
	Topic  string
	Time   time.Time
	// Actor names who caused the event at the publisher ("provider:subject"). It is audit
	// information only: the handler runs as this service's own identity, never as the actor (E9).
	Actor   string
	Data    P
	Attempt int // the handler's attempt, 1 for the first
}
