package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
)

// inboxRequest builds a POST to billing's create-invoice inbox, signed with secret.
func inboxRequest(t *testing.T, secret string, event *events.Event) *http.Request {
	t.Helper()
	body, err := json.Marshal(event)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/events/create-invoice", strings.NewReader(string(body)))
	webhookID := event.ID
	if webhookID == "" {
		webhookID = "msg-without-event-id"
	}
	require.NoError(t, events.Sign(request.Header, secret, webhookID, time.Now(), body))
	return request
}

func validEvent() *events.Event {
	return &events.Event{
		SpecVersion: events.SpecVersion, ID: "0193a3f0-0000-7000-8000-000000000001", Source: "orders",
		Type: orderPlacedTopic.Name(), Time: time.Now().UTC(), DataContentType: "application/json",
		Data: json.RawMessage(`{"orderId":"o-1","version":1}`),
	}
}

func TestTheInboxRefusesWhatItCannotTrust(t *testing.T) {
	orders := newService(t, "orders", nil)
	billing := newService(t, "billing", &serviceOptions{configure: func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
	}})
	var invoices received
	require.NoError(t, events.Subscribe(billing.bus, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, events.Subscribe(billing.bus, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "local-audit"}))
	secret := events.DeriveSecret(inboxKey, "create-invoice")

	wrongTopic := validEvent()
	wrongTopic.Type = "orders.order-cancelled.v1"
	noID := validEvent()
	noID.ID = ""
	oldVersion := validEvent()
	oldVersion.SpecVersion = "0.3"
	cases := map[string]struct {
		request *http.Request
		status  int
		result  events.ReceptionResult
	}{
		"an unknown subscription": {
			request: func() *http.Request {
				request := inboxRequest(t, secret, validEvent())
				request.URL.Path = "/events/unknown"
				return request
			}(),
			status: http.StatusNotFound, result: events.ReceptionUnknownSubscription,
		},
		"a local subscription": {
			request: func() *http.Request {
				request := inboxRequest(t, events.DeriveSecret(inboxKey, "local-audit"), validEvent())
				request.URL.Path = "/events/local-audit"
				return request
			}(),
			status: http.StatusNotFound, result: events.ReceptionUnknownSubscription,
		},
		"another subscription's secret": {
			request: inboxRequest(t, events.DeriveSecret(inboxKey, "local-audit"), validEvent()),
			status:  http.StatusUnauthorized, result: events.ReceptionInvalidSignature,
		},
		"a body changed after signing": {
			request: func() *http.Request {
				request := inboxRequest(t, secret, validEvent())
				signed := request.Header.Clone()
				request = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/events/create-invoice", strings.NewReader(`{"specversion":"1.0"}`))
				request.Header = signed
				return request
			}(),
			status: http.StatusUnauthorized, result: events.ReceptionInvalidSignature,
		},
		"an event of another topic":   {request: inboxRequest(t, secret, wrongTopic), status: http.StatusBadRequest, result: events.ReceptionInvalidEvent},
		"an event without an id":      {request: inboxRequest(t, secret, noID), status: http.StatusBadRequest, result: events.ReceptionInvalidEvent},
		"another CloudEvents version": {request: inboxRequest(t, secret, oldVersion), status: http.StatusBadRequest, result: events.ReceptionInvalidEvent},
		"a body above the cap": {
			request: func() *http.Request {
				large := validEvent()
				large.Data = json.RawMessage(`"` + strings.Repeat("x", events.MaximumEventBytes) + `"`)
				return inboxRequest(t, secret, large)
			}(),
			status: http.StatusRequestEntityTooLarge, result: events.ReceptionTooLarge,
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			billing.observer.receptions = nil
			recorder := httptest.NewRecorder()
			billing.bus.InboxHandler().ServeHTTP(recorder, testCase.request)
			require.Equal(t, testCase.status, recorder.Code)
			require.Equal(t, []events.ReceptionResult{testCase.result}, billing.observer.receptionResults())
		})
	}

	recorder := httptest.NewRecorder()
	billing.bus.InboxHandler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/events/create-invoice", nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Zero(t, billing.drain(t), "nothing was enqueued")

	// The valid request, under another router's prefix: the last path segment names the subscription.
	request := inboxRequest(t, secret, validEvent())
	request.URL.Path = "/api/v1/inbox/create-invoice"
	recorder = httptest.NewRecorder()
	billing.bus.InboxHandler().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, 1, billing.drain(t))
	require.Len(t, invoices.all(), 1)
}

func registrationRequest(t *testing.T, subscriber string, body any) *http.Request {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	return httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/events/subscribers/"+subscriber, strings.NewReader(string(encoded)))
}

type registered struct {
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	Endpoint string `json:"endpoint"`
	Secret   string `json:"secret"`
}

func registrationOf(entries ...registered) map[string]any {
	return map[string]any{"subscriptions": entries}
}

func TestTheRegistrationRefusesInvalidAndUnauthorizedSets(t *testing.T) {
	var authorized *events.RegistrationRequest
	orders := newService(t, "orders", &serviceOptions{configure: func(options *events.Options) {
		options.AllowHTTP, options.InboxURL = false, ""
		options.Authorize = func(_ *http.Request, registration *events.RegistrationRequest) error {
			authorized = registration
			if registration.Subscriber != "billing" && registration.Subscriber != "orders" {
				return errors.New("unknown subscriber")
			}
			return nil
		}
	}})
	valid := registered{Name: "create-invoice", Topic: orderPlacedTopic.Name(), Endpoint: "https://billing.example.com/events/create-invoice", Secret: events.NewSecret()}
	withEndpoint := valid
	withEndpoint.Endpoint = "http://billing.example.com/events/create-invoice"
	withSecret := valid
	withSecret.Secret = "plain"
	withTopic := valid
	withTopic.Topic = "orders"
	withName := valid
	withName.Name = "Create Invoice"
	cases := map[string]struct {
		request *http.Request
		status  int
	}{
		"another subscriber":    {registrationRequest(t, "shipping", registrationOf(valid)), http.StatusForbidden},
		"the publisher itself":  {registrationRequest(t, "orders", registrationOf(valid)), http.StatusForbidden},
		"an invalid subscriber": {registrationRequest(t, "Billing", registrationOf(valid)), http.StatusBadRequest},
		"an http endpoint":      {registrationRequest(t, "billing", registrationOf(withEndpoint)), http.StatusBadRequest},
		"a malformed secret":    {registrationRequest(t, "billing", registrationOf(withSecret)), http.StatusBadRequest},
		"a malformed topic":     {registrationRequest(t, "billing", registrationOf(withTopic)), http.StatusBadRequest},
		"a malformed name":      {registrationRequest(t, "billing", registrationOf(withName)), http.StatusBadRequest},
		"a duplicate name":      {registrationRequest(t, "billing", registrationOf(valid, valid)), http.StatusBadRequest},
		"a body that is no set": {registrationRequest(t, "billing", "nope"), http.StatusBadRequest},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			orders.bus.RegistrationHandler().ServeHTTP(recorder, testCase.request)
			require.Equal(t, testCase.status, recorder.Code, recorder.Body.String())
		})
	}
	records, err := orders.store.Subscriptions(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, records)

	recorder := httptest.NewRecorder()
	orders.bus.RegistrationHandler().ServeHTTP(recorder, registrationRequest(t, "billing", registrationOf(valid)))
	require.Equal(t, http.StatusNoContent, recorder.Code)
	require.Equal(t, &events.RegistrationRequest{Subscriber: "billing", Subscriptions: []events.Subscription{{
		Subscriber: "billing", Name: valid.Name, Topic: valid.Topic, Endpoint: valid.Endpoint,
	}}}, authorized, "Authorize sees the endpoints, not the secrets")

	recorder = httptest.NewRecorder()
	orders.bus.RegistrationHandler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/events/subscribers/billing", nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
}

func TestWithoutAuthorizeEveryRegistrationIsRefused(t *testing.T) {
	orders := newService(t, "orders", &serviceOptions{configure: func(options *events.Options) { options.Authorize = nil }})
	recorder := httptest.NewRecorder()
	orders.bus.RegistrationHandler().ServeHTTP(recorder, registrationRequest(t, "billing", registrationOf()))
	require.Equal(t, http.StatusForbidden, recorder.Code)
}

func TestNewAndSubscribeValidate(t *testing.T) {
	orders := newService(t, "orders", nil)
	handler := (&received{}).handle
	_, err := events.New(nil)
	require.Error(t, err)
	_, err = events.New(&events.Options{Name: "Orders", Store: orders.store, Worker: orders.worker})
	require.ErrorIs(t, err, events.ErrInvalidName)
	for name, options := range map[string]*events.Options{
		"a scheme-less inbox URL": {InboxURL: "billing.example.com/events"},
		"an http inbox URL":       {InboxURL: "http://billing.example.com/events"},
		"an http publisher":       {Publishers: []events.Publisher{{Name: "orders", URL: "http://orders.example.com/events"}}},
	} {
		options.Name, options.Store, options.Worker = "billing", orders.store, orders.worker
		_, err = events.New(options)
		require.ErrorIs(t, err, events.ErrInvalidEndpoint, name)
	}

	invalid := map[string]*events.SubscribeOptions{
		"no name":                   {},
		"an unknown publisher":      {Name: "audit", Publisher: "shipping"},
		"negative maximum attempts": {Name: "audit", MaximumAttempts: -1},
	}
	for name, options := range invalid {
		require.Error(t, events.Subscribe(orders.bus, orderPlacedTopic, handler, options), name)
	}
	require.Error(t, events.Subscribe(orders.bus, orderPlacedTopic, nil, &events.SubscribeOptions{Name: "audit"}))
	require.ErrorIs(t, events.Subscribe(orders.bus, events.NewTopic[OrderPlaced]("orders"), handler, &events.SubscribeOptions{Name: "audit"}), events.ErrInvalidTopic)
	require.NoError(t, events.Subscribe(orders.bus, orderPlacedTopic, handler, &events.SubscribeOptions{Name: "audit"}))
	require.Error(t, events.Subscribe(orders.bus, orderPlacedTopic, handler, &events.SubscribeOptions{Name: "audit"}), "declared twice")

	withoutInbox := newService(t, "shipping", &serviceOptions{configure: func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
		options.InboxKey = []byte("short")
	}})
	require.Error(t, events.Subscribe(withoutInbox.bus, orderPlacedTopic, handler, &events.SubscribeOptions{Name: "ship", Publisher: "orders"}))
}
