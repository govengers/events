package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/govengers/jobs"
)

// maximumRegistrationBytes caps a registration's body: a few hundred subscriptions.
const maximumRegistrationBytes = 1 << 20

// registrationPayload is the registration job's payload: the set Register computed, so whichever
// replica claims the job sends the set of the code that enqueued it, also mid-rollout. It holds no
// secret: the claiming process derives them, and only one with the same inbox key may.
// RegisteredAt orders it at the publisher against other registrations (E31).
type registrationPayload struct {
	Publisher     string                 `json:"publisher"`
	InboxURL      string                 `json:"inboxUrl"`
	InboxKeyID    string                 `json:"inboxKeyId"`
	Subscriptions []declaredSubscription `json:"subscriptions"`
	RegisteredAt  time.Time              `json:"registeredAt"`
}

// declaredSubscription is one remote subscription of a registration job.
type declaredSubscription struct {
	Name  string `json:"name"`
	Topic string `json:"topic"`
}

// inboxKeyID names an inbox key without revealing it.
func inboxKeyID(key []byte) string {
	digest := sha256.Sum256(append([]byte("govengers.events inbox key\x00"), key...))
	return hex.EncodeToString(digest[:8])
}

// registrationBody is the body of PUT {publisher}/subscribers/{subscriber}. The publisher
// ignores a registration made before the one it stored last (E31); without RegisteredAt it is
// not ordered.
type registrationBody struct {
	RegisteredAt  time.Time                `json:"registeredAt,omitzero"`
	Subscriptions []registeredSubscription `json:"subscriptions"`
}

type registeredSubscription struct {
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	Endpoint string `json:"endpoint"`
	Secret   string `json:"secret"`
}

// register is the handler of the registration job: it sends the publisher the job's complete set
// of this service's subscriptions to it. 2xx completes, 400 is permanent, everything else
// retries, also a job this process cannot send (another version's publisher or inbox key).
func (b *Bus) register(ctx context.Context, job *jobs.Job[registrationPayload]) error {
	publisher, known := b.publishers[job.Payload.Publisher]
	if !known || job.Payload.InboxKeyID != inboxKeyID(b.inboxKey) {
		// a job of another version of this service, mid-rollout: a replica of that version claims a later attempt
		return fmt.Errorf("the registration at %s needs a replica that declares the publisher and holds the job's inbox key", job.Payload.Publisher)
	}
	body := registrationBody{RegisteredAt: job.Payload.RegisteredAt, Subscriptions: b.registeredSubscriptions(&job.Payload)}
	registration := &Registration{Publisher: publisher.Name, Subscriber: b.name, Subscriptions: len(body.Subscriptions)}
	defer func() { b.observer.Registered(registration) }()
	registration.StatusCode, registration.Err = b.sendRegistration(ctx, publisher, &body)
	if registration.StatusCode == http.StatusBadRequest {
		return jobs.Permanent(registration.Err)
	}
	return registration.Err
}

// registeredSubscriptions adds endpoints and secrets to the job's set.
func (b *Bus) registeredSubscriptions(payload *registrationPayload) []registeredSubscription {
	registered := make([]registeredSubscription, 0, len(payload.Subscriptions))
	for _, declared := range payload.Subscriptions {
		registered = append(registered, registeredSubscription{
			Name: declared.Name, Topic: declared.Topic,
			Endpoint: payload.InboxURL + "/" + url.PathEscape(declared.Name),
			Secret:   DeriveSecret(b.inboxKey, declared.Name),
		})
	}
	return registered
}

func (b *Bus) sendRegistration(ctx context.Context, publisher Publisher, body *registrationBody) (int, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("encode the registration: %w", err)
	}
	target, err := url.JoinPath(publisher.URL, "subscribers", b.name)
	if err != nil {
		return 0, fmt.Errorf("registration URL of %s: %w", publisher.Name, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(encoded))
	if err != nil {
		return 0, fmt.Errorf("build the registration at %s: %w", publisher.Name, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+publisher.APIKey)
	response, err := b.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("register at %s: %w", publisher.Name, err)
	}
	defer func() { _ = response.Body.Close() }() // the answer's body carries nothing we keep
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, discardedResponseBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, fmt.Errorf("register at %s: the publisher answered %d", publisher.Name, response.StatusCode)
	}
	return response.StatusCode, nil
}

// RegistrationHandler returns the publisher's registration route (E5): PUT
// {base}/subscribers/{subscriber} with the subscriber's complete set of subscriptions, which
// replaces its earlier set in one transaction, unless the subscriber registered a newer set
// before (E31). The subscriber is the last segment of the request path. Mount it behind the
// application's authentication (an API key, E8); Options.Authorize then decides whether the
// caller may register that subscriber with those topics and endpoints, and nil Authorize refuses
// everything. It answers 204 on success and for a superseded registration, 400 for an invalid
// set, 403 when refused or when the subscriber is this service itself.
func (b *Bus) RegistrationHandler() http.Handler {
	return http.HandlerFunc(b.acceptRegistration)
}

func (b *Bus) acceptRegistration(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	registration := &Registration{Publisher: b.name, Subscriber: path.Base(request.URL.Path), Inbound: true}
	registration.StatusCode, registration.Err = b.storeRegistration(writer, request, registration)
	b.observer.Registered(registration)
	if registration.StatusCode == http.StatusNoContent {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	message := http.StatusText(registration.StatusCode)
	if registration.StatusCode == http.StatusBadRequest {
		message = registration.Err.Error() // the subscriber's own input, safe to echo
	}
	http.Error(writer, message, registration.StatusCode)
}

func (b *Bus) storeRegistration(writer http.ResponseWriter, request *http.Request, registration *Registration) (int, error) {
	if b.authorize == nil {
		return http.StatusForbidden, errors.New("registrations are disabled: no Options.Authorize")
	}
	if err := ValidateName(registration.Subscriber); err != nil {
		return http.StatusBadRequest, err
	}
	if registration.Subscriber == b.name {
		// it would replace this service's own local subscriptions
		return http.StatusForbidden, errors.New("a registration cannot name this service as its subscriber")
	}
	var body registrationBody
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maximumRegistrationBytes))
	if err := decoder.Decode(&body); err != nil {
		return http.StatusBadRequest, fmt.Errorf("decode the registration: %w", err)
	}
	subscriptions, err := b.validateRegistration(registration.Subscriber, body.Subscriptions)
	if err != nil {
		return http.StatusBadRequest, err
	}
	registration.Subscriptions = len(subscriptions)
	if err := b.authorize(request, authorizationRequest(registration.Subscriber, subscriptions)); err != nil {
		return http.StatusForbidden, err
	}
	applied, err := b.store.PutRegistration(request.Context(), registration.Subscriber, body.RegisteredAt, subscriptions)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	registration.Superseded = !applied
	return http.StatusNoContent, nil
}

func (b *Bus) validateRegistration(subscriber string, registered []registeredSubscription) ([]Subscription, error) {
	subscriptions := make([]Subscription, 0, len(registered))
	names := map[string]bool{}
	for _, entry := range registered {
		if err := ValidateName(entry.Name); err != nil {
			return nil, err
		}
		if names[entry.Name] {
			return nil, fmt.Errorf("subscription %s is listed twice", entry.Name)
		}
		names[entry.Name] = true
		if err := ValidateTopic(entry.Topic); err != nil {
			return nil, fmt.Errorf("subscription %s: %w", entry.Name, err)
		}
		if err := ValidateEndpoint(entry.Endpoint, b.allowHTTP); err != nil {
			return nil, fmt.Errorf("subscription %s: %w", entry.Name, err)
		}
		if err := ValidateSecret(entry.Secret); err != nil {
			return nil, fmt.Errorf("subscription %s: %w", entry.Name, err)
		}
		subscriptions = append(subscriptions, Subscription{
			Subscriber: subscriber, Name: entry.Name, Topic: entry.Topic, Endpoint: entry.Endpoint, Secret: entry.Secret,
		})
	}
	return subscriptions, nil
}

// authorizationRequest is the registration without its secrets.
func authorizationRequest(subscriber string, subscriptions []Subscription) *RegistrationRequest {
	request := &RegistrationRequest{Subscriber: subscriber, Subscriptions: make([]Subscription, 0, len(subscriptions))}
	for _, subscription := range subscriptions {
		subscription.Secret = ""
		request.Subscriptions = append(request.Subscriptions, subscription)
	}
	return request
}

// ErrInvalidEndpoint is returned for an endpoint that is not an absolute https URL (or http with
// allowHTTP), or that carries credentials or a fragment.
var ErrInvalidEndpoint = errors.New("events: invalid endpoint")

// ValidateEndpoint checks a remote subscription's endpoint before it is stored. Where the
// endpoint resolves to is checked when a delivery dials it (safehttp), not here: DNS may change.
func ValidateEndpoint(endpoint string, allowHTTP bool) error {
	parsed, err := url.Parse(endpoint)
	switch {
	case err != nil:
		return fmt.Errorf("%w: %v", ErrInvalidEndpoint, err) //nolint:errorlint // LIB-010: url errors are not part of the API
	case parsed.Scheme != "https" && (parsed.Scheme != "http" || !allowHTTP):
		return fmt.Errorf("%w: scheme %q", ErrInvalidEndpoint, parsed.Scheme)
	case parsed.Host == "" || parsed.User != nil || parsed.Fragment != "":
		return fmt.Errorf("%w: needs a host, no credentials and no fragment", ErrInvalidEndpoint)
	}
	return nil
}
