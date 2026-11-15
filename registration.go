package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"

	"github.com/govengers/jobs"
)

// maximumRegistrationBytes caps a registration's body: a few hundred subscriptions.
const maximumRegistrationBytes = 1 << 20

// registrationPayload is the registration job's payload: the set is computed when the job runs,
// from the subscriptions this process declared, so the newest code is the source of truth.
type registrationPayload struct {
	Publisher string `json:"publisher"`
}

// registrationBody is the body of PUT {publisher}/subscribers/{subscriber}.
type registrationBody struct {
	Subscriptions []registeredSubscription `json:"subscriptions"`
}

type registeredSubscription struct {
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	Endpoint string `json:"endpoint"`
	Secret   string `json:"secret"`
}

// register is the handler of the registration job: it sends the publisher this service's
// complete set of subscriptions to it. 2xx completes, 400 is permanent, everything else retries.
func (b *Bus) register(ctx context.Context, job *jobs.Job[registrationPayload]) error {
	publisher, known := b.publishers[job.Payload.Publisher]
	if !known {
		return jobs.Permanent(fmt.Errorf("publisher %s is no longer declared", job.Payload.Publisher))
	}
	body := registrationBody{Subscriptions: b.remoteSubscriptions(publisher.Name)}
	registration := &Registration{Publisher: publisher.Name, Subscriber: b.name, Subscriptions: len(body.Subscriptions)}
	defer func() { b.observer.Registered(registration) }()
	registration.StatusCode, registration.Err = b.sendRegistration(ctx, publisher, &body)
	if registration.StatusCode == http.StatusBadRequest {
		return jobs.Permanent(registration.Err)
	}
	return registration.Err
}

func (b *Bus) remoteSubscriptions(publisher string) []registeredSubscription {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	registered := []registeredSubscription{}
	for _, declared := range b.subscriptions {
		if declared.publisher == publisher {
			registered = append(registered, registeredSubscription{
				Name: declared.name, Topic: declared.topic,
				Endpoint: b.inboxURL + "/" + url.PathEscape(declared.name),
				Secret:   DeriveSecret(b.inboxKey, declared.name),
			})
		}
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
// replaces its earlier set in one transaction. The subscriber is the last segment of the request
// path. Mount it behind the application's authentication (an API key, E8); Options.Authorize
// then decides whether the caller may register that subscriber for those topics, and nil
// Authorize refuses everything. It answers 204 on success, 400 for an invalid set, 403 when
// refused.
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
	if err := ValidateName(registration.Subscriber); err != nil {
		return http.StatusBadRequest, err
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
	if b.authorize == nil {
		return http.StatusForbidden, errors.New("registrations are disabled: no Options.Authorize")
	}
	if err := b.authorize(request, registration.Subscriber, topicsOf(subscriptions)); err != nil {
		return http.StatusForbidden, err
	}
	if err := b.store.PutSubscriptions(request.Context(), registration.Subscriber, subscriptions); err != nil {
		return http.StatusInternalServerError, err
	}
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
		if err := ValidateEndpoint(entry.Endpoint, b.allowHTTPEndpoints); err != nil {
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

func topicsOf(subscriptions []Subscription) []string {
	topics := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		if !slices.Contains(topics, subscription.Topic) {
			topics = append(topics, subscription.Topic)
		}
	}
	return topics
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
