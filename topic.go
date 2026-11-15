package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// ErrInvalidTopic is returned by Publish and Subscribe for a topic whose name is not
// "<domain>.<event>.v<major>" in lower case, for example "orders.order-placed.v1".
var ErrInvalidTopic = errors.New("events: invalid topic")

// topicPattern is E2: dot-separated lower-case words with hyphens, ending in the major version.
var topicPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*(\.[a-z0-9]+(-[a-z0-9]+)*)*\.v[1-9][0-9]*$`)

// Topic is the typed contract of one kind of event. Declare it once in the publisher's contract
// package, which subscribers import:
//
//	var OrderPlacedTopic = events.NewTopic[OrderPlaced]("orders.order-placed.v1")
//
// The major version is part of the name: within a version only additive changes are allowed; a
// breaking change publishes v1 and v2 side by side until every subscriber moved. The data P is
// encoded as JSON. A Topic is immutable and safe for concurrent use.
type Topic[P any] struct {
	name string
}

// NewTopic declares a topic. It does not validate, so it can initialize a package-level variable;
// Publish and Subscribe return ErrInvalidTopic for a malformed name.
func NewTopic[P any](name string) *Topic[P] {
	return &Topic[P]{name: name}
}

// Name is the topic's name, the CloudEvents type of its events.
func (t *Topic[P]) Name() string { return t.name }

// New prepares an event of this topic for the store's Publish.
func (t *Topic[P]) New(data P) *PreparedEvent {
	return &PreparedEvent{topic: t.name, data: data}
}

// PreparedEvent is an event prepared by Topic.New, ready for the store's Publish.
type PreparedEvent struct {
	topic string
	data  any
}

// Topic is the name of the event's topic.
func (e *PreparedEvent) Topic() string { return e.topic }

// EncodeData returns the event's data as JSON. It fails with ErrInvalidTopic for a malformed topic
// (also for a zero PreparedEvent), with the encoding error when the data cannot be encoded, and
// with ErrNulCharacter for data with a NUL character.
func (e *PreparedEvent) EncodeData() (json.RawMessage, error) {
	if err := ValidateTopic(e.topic); err != nil {
		return nil, err
	}
	data, err := json.Marshal(e.data)
	if err != nil {
		return nil, fmt.Errorf("encode data of %s: %w", e.topic, err)
	}
	if hasNulCharacter(data) {
		return nil, fmt.Errorf("%w: %s", ErrNulCharacter, e.topic)
	}
	return data, nil
}

// ValidateTopic returns ErrInvalidTopic unless name is a well-formed topic name.
func ValidateTopic(name string) error {
	if !topicPattern.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidTopic, name)
	}
	return nil
}
