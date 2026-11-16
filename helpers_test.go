package events_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
	"github.com/govengers/events/internal/postgrestest"
	"github.com/govengers/events/postgresadapter"
	"github.com/govengers/jobs"
	"github.com/govengers/jobs/jobstest"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

// OrderPlaced is the data of the test topic.
type OrderPlaced struct {
	OrderID string `json:"orderId"`
	Version int    `json:"version"`
}

var orderPlacedTopic = events.NewTopic[OrderPlaced]("orders.order-placed.v1")

var inboxKey = []byte("billing-inbox-key-0123456789abcdef")

// service is one service of a test: its own database, job store, worker, bus and
// HTTP server with the inbox under /events/ and the registration under /events/subscribers/.
type service struct {
	name     string
	pool     *pgxpool.Pool
	store    *postgresadapter.Store
	worker   *jobs.Worker
	bus      *events.Bus
	server   *httptest.Server
	observer *recordingObserver
}

// serviceOptions adjusts a test service; options.Name, Store, Worker and Observer are set.
type serviceOptions struct {
	configure func(options *events.Options)
	// intercept wraps the HTTP handler, to make the service fail or disappear.
	intercept func(next http.Handler) http.Handler
}

func newService(t *testing.T, name string, adjust *serviceOptions) *service {
	t.Helper()
	if adjust == nil {
		adjust = &serviceOptions{}
	}
	created := &service{name: name, pool: postgrestest.New(t), observer: &recordingObserver{}}
	jobStore, err := jobspostgres.New(&jobspostgres.Options{Pool: created.pool})
	require.NoError(t, err)
	created.store, err = postgresadapter.New(&postgresadapter.Options{
		Pool: created.pool, Jobs: jobStore, Source: name, Sealer: postgrestest.Sealer{}, Observer: created.observer,
		Actor: func(context.Context) string { return "password:ada" },
	})
	require.NoError(t, err)
	created.worker, err = jobs.NewWorker(&jobs.WorkerOptions{
		Store:  jobStore,
		Queues: map[string]jobs.QueueOptions{events.Queue: {}, events.SubscriberQueue(name): {}},
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	handler := http.Handler(mux)
	if adjust.intercept != nil {
		handler = adjust.intercept(mux)
	}
	created.server = httptest.NewServer(handler)
	t.Cleanup(created.server.Close)
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)

	options := &events.Options{
		Name: name, Store: created.store, Worker: created.worker, Observer: created.observer,
		HTTPClient: &http.Client{Transport: transport, Timeout: 10 * time.Second},
		InboxKey:   inboxKey, InboxURL: created.server.URL + "/events",
		Authorize: func(*http.Request, *events.RegistrationRequest) error { return nil },
		AllowHTTP: true,
	}
	if adjust.configure != nil {
		adjust.configure(options)
	}
	created.bus, err = events.New(options)
	require.NoError(t, err)
	mux.Handle("/events/", created.bus.InboxHandler())
	mux.Handle("/events/subscribers/", created.bus.RegistrationHandler())
	return created
}

// publisherOf declares service as a remote publisher.
func publisherOf(publisher *service) events.Publisher {
	return events.Publisher{Name: publisher.name, URL: publisher.server.URL + "/events", APIKey: "billing-api-key"}
}

func (s *service) drain(t *testing.T) int {
	t.Helper()
	return jobstest.Drain(t, s.worker)
}

// makeDue moves every pending job's run time to now, so a retry runs without waiting for its backoff.
func (s *service) makeDue(t *testing.T) {
	t.Helper()
	_, err := s.pool.Exec(context.Background(), "UPDATE govengers_jobs SET run_at = now() WHERE status = 'pending'")
	require.NoError(t, err)
}

func (s *service) publish(t *testing.T, data OrderPlaced) string {
	t.Helper()
	ctx := context.Background()
	transaction, err := s.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = transaction.Rollback(ctx) }()
	ids, err := s.store.Publish(ctx, transaction, orderPlacedTopic.New(data))
	require.NoError(t, err)
	require.NoError(t, transaction.Commit(ctx))
	require.Len(t, ids, 1)
	return ids[0]
}

func (s *service) delivery(t *testing.T, key events.DeliveryKey) *postgresadapter.DeliveryEntry {
	t.Helper()
	entries, err := s.store.Deliveries(context.Background(), &postgresadapter.DeliveryFilter{Subscriber: key.Subscriber, Subscription: key.Subscription})
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.Key == key {
			return entry
		}
	}
	require.FailNow(t, "no delivery", "%+v", key)
	return nil
}

func (s *service) subscription(t *testing.T, subscriber, name string) *postgresadapter.SubscriptionRecord {
	t.Helper()
	records, err := s.store.Subscriptions(context.Background(), &postgresadapter.SubscriptionFilter{Subscriber: subscriber})
	require.NoError(t, err)
	for _, record := range records {
		if record.Name == name {
			return record
		}
	}
	require.FailNow(t, "no subscription", "%s/%s", subscriber, name)
	return nil
}

// received collects the messages a handler received.
type received struct {
	mutex    sync.Mutex
	messages []*events.Message[OrderPlaced]
}

func (r *received) handle(_ context.Context, message *events.Message[OrderPlaced]) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.messages = append(r.messages, message)
	return nil
}

func (r *received) all() []*events.Message[OrderPlaced] {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return append([]*events.Message[OrderPlaced](nil), r.messages...)
}

// recordingObserver keeps every report.
type recordingObserver struct {
	mutex         sync.Mutex
	publications  []events.Publication
	deliveries    []events.DeliveryAttempt
	receptions    []events.Reception
	registrations []events.Registration
}

func (o *recordingObserver) Published(publication *events.Publication) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.publications = append(o.publications, *publication)
}

func (o *recordingObserver) DeliveryFinished(attempt *events.DeliveryAttempt) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.deliveries = append(o.deliveries, *attempt)
}

func (o *recordingObserver) Received(reception *events.Reception) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.receptions = append(o.receptions, *reception)
}

func (o *recordingObserver) Registered(registration *events.Registration) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.registrations = append(o.registrations, *registration)
}

func (o *recordingObserver) deliveryResults() []events.DeliveryResult {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	results := make([]events.DeliveryResult, 0, len(o.deliveries))
	for _, attempt := range o.deliveries {
		results = append(results, attempt.Result)
	}
	return results
}

func (o *recordingObserver) receptionResults() []events.ReceptionResult {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	results := make([]events.ReceptionResult, 0, len(o.receptions))
	for _, reception := range o.receptions {
		results = append(results, reception.Result)
	}
	return results
}

// statusCodes lists the subscriber's status of every delivery attempt that sent a request.
func (o *recordingObserver) statusCodes() []int {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	codes := []int{}
	for _, attempt := range o.deliveries {
		if attempt.StatusCode != 0 {
			codes = append(codes, attempt.StatusCode)
		}
	}
	return codes
}
