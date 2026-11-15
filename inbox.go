package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
)

// InboxHandler returns the inbox (E7): POST {InboxURL}/{subscription}. Mount it without the
// application's authentication: the signature authenticates the publisher. The subscription is
// the last segment of the request path, so the handler works under any router and prefix.
//
// It verifies the signature on the raw body, checks that the event is of the subscription's
// topic and then, in one transaction, stores the event's key and enqueues the handler's job.
// It answers 202 for a new and a duplicate delivery alike, 404 for an unknown subscription (the
// publisher retries: this service may be mid-rollout), 401 for a bad signature, 400 for a body
// that is not an event of the subscription's topic, 413 above MaximumEventBytes.
func (b *Bus) InboxHandler() http.Handler {
	return http.HandlerFunc(b.receive)
}

func (b *Bus) receive(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reception := &Reception{Subscription: path.Base(request.URL.Path)}
	status := b.accept(writer, request, reception)
	b.observer.Received(reception)
	if status == http.StatusAccepted {
		writer.WriteHeader(status)
		return
	}
	http.Error(writer, http.StatusText(status), status)
}

// accept runs the inbox's checks and returns the status to answer; it fills reception.
func (b *Bus) accept(writer http.ResponseWriter, request *http.Request, reception *Reception) int {
	declared, found := b.subscription(reception.Subscription)
	if !found || declared.publisher == "" {
		reception.Result = ReceptionUnknownSubscription
		return http.StatusNotFound
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, MaximumEventBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reception.Result, reception.Err = ReceptionTooLarge, err
			return http.StatusRequestEntityTooLarge
		}
		reception.Result, reception.Err = ReceptionInvalidEvent, err
		return http.StatusBadRequest
	}
	if err := Verify(request.Header, DeriveSecret(b.inboxKey, declared.name), body, b.now()); err != nil {
		reception.Result, reception.Err = ReceptionInvalidSignature, err
		return http.StatusUnauthorized
	}
	event, err := decodeEvent(body, declared.topic)
	if err != nil {
		reception.Result, reception.Err = ReceptionInvalidEvent, err
		return http.StatusBadRequest
	}
	reception.EventID = event.ID
	ctx := request.Context()
	carried := map[string]string{"traceparent": event.TraceParent, "tracestate": event.TraceState}
	for _, propagator := range b.propagators {
		ctx = propagator.Extract(ctx, carried)
	}
	local := Subscription{Subscriber: b.name, Name: declared.name, Queue: declared.queue, MaximumAttempts: declared.maximumAttempts}
	accepted, err := b.store.Accept(ctx, &Acceptance{
		Subscriber: b.name, Subscription: declared.name, EventID: event.ID, Job: local.Job(event),
	})
	switch {
	case err != nil:
		reception.Result, reception.Err = ReceptionFailed, err
		return http.StatusInternalServerError
	case accepted:
		reception.Result = ReceptionAccepted
	default:
		reception.Result = ReceptionDuplicate
	}
	return http.StatusAccepted
}

// maximumEventIDLength keeps the inbox key indexable; Publish's IDs are 36 bytes.
const maximumEventIDLength = 200

// decodeEvent parses a CloudEvent in structured JSON mode and checks it belongs to topic.
func decodeEvent(body []byte, topic string) (*Event, error) {
	var event Event
	if err := json.Unmarshal(body, &event); err != nil {
		return nil, fmt.Errorf("decode the event: %w", err)
	}
	switch {
	case event.SpecVersion != SpecVersion:
		return nil, fmt.Errorf("specversion %q, want %s", event.SpecVersion, SpecVersion)
	case event.ID == "" || len(event.ID) > maximumEventIDLength || event.Source == "":
		return nil, errors.New("the event needs a source and an id of at most 200 bytes")
	case event.Type != topic:
		return nil, fmt.Errorf("event of type %q, the subscription's topic is %s", event.Type, topic)
	}
	return &event, nil
}
