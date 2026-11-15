package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/events"
	"github.com/govengers/events/internal/postgrestest"
	"github.com/govengers/events/postgresadapter"
	"github.com/govengers/jobs"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

type orderPlaced struct {
	OrderID string `json:"orderId"`
}

var orderPlacedTopic = events.NewTopic[orderPlaced]("orders.order-placed.v1")

func newStore(t *testing.T, adjust func(options *postgresadapter.Options)) (*postgresadapter.Store, *pgxpool.Pool) {
	t.Helper()
	return storeOn(t, postgrestest.New(t), adjust)
}

// repeatableRead returns a second pool on the same schema whose transactions default to
// REPEATABLE READ, the setting under which an unpinned multi-statement path breaks.
func repeatableRead(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	configuration := pool.Config().Copy()
	configuration.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	strict, err := pgxpool.NewWithConfig(context.Background(), configuration)
	require.NoError(t, err)
	t.Cleanup(strict.Close)
	return strict
}

func storeOn(t *testing.T, pool *pgxpool.Pool, adjust func(options *postgresadapter.Options)) (*postgresadapter.Store, *pgxpool.Pool) {
	t.Helper()
	jobStore, err := jobspostgres.New(&jobspostgres.Options{Pool: pool})
	require.NoError(t, err)
	options := &postgresadapter.Options{Pool: pool, Jobs: jobStore, Source: "orders", Sealer: postgrestest.Sealer{}}
	if adjust != nil {
		adjust(options)
	}
	store, err := postgresadapter.New(options)
	require.NoError(t, err)
	return store, pool
}

func remote(name string) events.Subscription {
	return events.Subscription{Name: name, Topic: orderPlacedTopic.Name(), Endpoint: "https://billing.example.com/events/" + name, Secret: events.NewSecret()}
}

func local(name string) events.Subscription {
	return events.Subscription{Name: name, Topic: orderPlacedTopic.Name(), Queue: "events.billing", MaximumAttempts: 3}
}

func names(t *testing.T, store *postgresadapter.Store, subscriber string) []string {
	t.Helper()
	records, err := store.Subscriptions(context.Background(), &postgresadapter.SubscriptionFilter{Subscriber: subscriber})
	require.NoError(t, err)
	listed := []string{}
	for _, record := range records {
		listed = append(listed, record.Name)
	}
	return listed
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var rows int
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&rows))
	return rows
}

func TestNewValidates(t *testing.T) {
	_, err := postgresadapter.New(nil)
	require.Error(t, err)
	pool := postgrestest.New(t)
	_, err = postgresadapter.New(&postgresadapter.Options{Pool: pool})
	require.Error(t, err, "Jobs is required")
	jobStore, err := jobspostgres.New(&jobspostgres.Options{Pool: pool})
	require.NoError(t, err)
	_, err = postgresadapter.New(&postgresadapter.Options{Pool: pool, Jobs: jobStore, Propagators: []jobs.Propagator{nil}})
	require.Error(t, err)
}

func TestPutSubscriptionsReplacesTheSubscribersSet(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t, nil)
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("create-invoice"), local("audit")}))
	require.NoError(t, store.PutSubscriptions(ctx, "shipping", []events.Subscription{remote("ship")}))
	require.Equal(t, []string{"audit", "create-invoice"}, names(t, store, "billing"))

	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("send-receipt"), remote("create-invoice")}))
	require.Equal(t, []string{"create-invoice", "send-receipt"}, names(t, store, "billing"))
	require.Equal(t, []string{"ship"}, names(t, store, "shipping"), "other subscribers keep theirs")

	require.NoError(t, store.PutSubscriptions(ctx, "billing", nil))
	require.Empty(t, names(t, store, "billing"))
}

func TestPutSubscriptionsValidatesBeforeWriting(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t, nil)
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{local("audit")}))
	withoutQueue := local("x")
	withoutQueue.Queue = ""
	foreign := local("x")
	foreign.Subscriber = "shipping"
	badSecret := remote("x")
	badSecret.Secret = "secret"
	badEndpoint := remote("x")
	badEndpoint.Endpoint = "ftp://billing.example.com"
	badTopic := local("x")
	badTopic.Topic = "orders"
	invalid := map[string][]events.Subscription{
		"a local subscription without a queue": {withoutQueue},
		"another subscriber's subscription":    {foreign},
		"a malformed secret":                   {badSecret},
		"a malformed endpoint":                 {badEndpoint},
		"a malformed topic":                    {badTopic},
		"a malformed name":                     {local("Audit")},
		"a duplicate":                          {local("x"), local("x")},
	}
	for name, subscriptions := range invalid {
		require.Error(t, store.PutSubscriptions(ctx, "billing", subscriptions), name)
		require.Equal(t, []string{"audit"}, names(t, store, "billing"), name)
	}
	require.ErrorIs(t, store.PutSubscriptions(ctx, "Billing", nil), events.ErrInvalidName)

	withoutSealer, _ := newStore(t, func(options *postgresadapter.Options) { options.Sealer = nil })
	require.ErrorIs(t, withoutSealer.PutSubscriptions(ctx, "billing", []events.Subscription{remote("x")}), postgresadapter.ErrNoSealer)
	require.NoError(t, withoutSealer.PutSubscriptions(ctx, "billing", []events.Subscription{local("x")}), "local ones need no sealer")
}

func TestTheSecretIsSealedAndBoundToItsRow(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, nil)
	subscription := remote("create-invoice")
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{subscription, remote("send-receipt")}))
	var sealed []byte
	require.NoError(t, pool.QueryRow(ctx, "SELECT sealed_secret FROM govengers_event_subscriptions WHERE name = 'create-invoice'").Scan(&sealed))
	require.NotContains(t, string(sealed), strings.TrimPrefix(subscription.Secret, "whsec_"))

	ids, err := store.Publish(ctx, pool, orderPlacedTopic.New(orderPlaced{OrderID: "o-1"}))
	require.NoError(t, err)
	delivery, err := store.Delivery(ctx, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: ids[0]})
	require.NoError(t, err)
	require.Equal(t, subscription.Secret, delivery.Secret)
	require.Equal(t, subscription.Endpoint, delivery.Endpoint)

	// A sealed secret copied to another row does not open there.
	_, err = pool.Exec(ctx, `UPDATE govengers_event_subscriptions SET sealed_secret = $1 WHERE name = 'send-receipt'`, sealed)
	require.NoError(t, err)
	_, err = store.Delivery(ctx, events.DeliveryKey{Subscriber: "billing", Subscription: "send-receipt", EventID: ids[0]})
	require.Error(t, err)
}

func TestConcurrentPutsOfOneSubscriberEndWithOneOfTheSets(t *testing.T) {
	ctx := context.Background()
	store, _ := storeOn(t, repeatableRead(t, postgrestest.New(t)), nil)
	sets := [][]events.Subscription{{local("a"), local("b")}, {local("b"), local("c")}, {local("c"), local("d")}, {local("d"), local("a")}}
	var group errgroup.Group
	for range 4 {
		for _, set := range sets {
			group.Go(func() error { return store.PutSubscriptions(ctx, "billing", set) })
		}
	}
	require.NoError(t, group.Wait(), "no deadlock and no unique violation")
	require.Len(t, names(t, store, "billing"), 2)
}

func TestPublishFansOutInTheCallersTransaction(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, func(options *postgresadapter.Options) {
		options.Propagators = []jobs.Propagator{traceCarrier{}}
		options.Actor = func(context.Context) string { return "oidc:ada" }
	})
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("create-invoice"), local("audit")}))
	require.NoError(t, store.PutSubscriptions(ctx, "shipping", []events.Subscription{remote("ship")}))
	require.NoError(t, store.DisableSubscription(ctx, "shipping", "ship"))

	transaction, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = store.Publish(ctx, transaction, orderPlacedTopic.New(orderPlaced{OrderID: "o-rolled-back"}))
	require.NoError(t, err)
	require.NoError(t, transaction.Rollback(ctx))
	require.Zero(t, count(t, pool, "govengers_jobs"))
	require.Zero(t, count(t, pool, "govengers_event_deliveries"))

	ids, err := store.Publish(ctx, pool, orderPlacedTopic.New(orderPlaced{OrderID: "o-1"}), orderPlacedTopic.New(orderPlaced{OrderID: "o-2"}))
	require.NoError(t, err)
	require.Len(t, ids, 2)
	require.Equal(t, 4, count(t, pool, "govengers_jobs"), "per event one local job and one delivery job; the disabled subscription is skipped")
	require.Equal(t, 2, count(t, pool, "govengers_event_deliveries"))

	entries, err := store.Deliveries(ctx, nil)
	require.NoError(t, err)
	var event events.Event
	require.NoError(t, json.Unmarshal(entries[0].Event, &event))
	require.Equal(t, events.SpecVersion, event.SpecVersion)
	require.Equal(t, "orders", event.Source)
	require.Equal(t, orderPlacedTopic.Name(), event.Type)
	require.Equal(t, "oidc:ada", event.Actor)
	require.Equal(t, "00-trace-span-01", event.TraceParent)
	require.JSONEq(t, `{"orderId":"o-2"}`, string(event.Data), "newest first")
	require.Equal(t, ids[1], event.ID)

	var kind, queue string
	require.NoError(t, pool.QueryRow(ctx, "SELECT kind, queue FROM govengers_jobs WHERE kind LIKE 'events.%' LIMIT 1").Scan(&kind, &queue))
	require.Equal(t, "events.billing.audit", kind)
	require.Equal(t, "events.billing", queue)
}

type traceCarrier struct{}

func (traceCarrier) Inject(_ context.Context, headers map[string]string) error {
	headers["traceparent"] = "00-trace-span-01"
	return nil
}
func (traceCarrier) Extract(ctx context.Context, _ map[string]string) context.Context { return ctx }

type refusingCarrier struct{}

func (refusingCarrier) Inject(context.Context, map[string]string) error { return errors.New("no") }
func (refusingCarrier) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

func TestPublishRefusesBeforeWriting(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, nil)
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("create-invoice")}))
	large := events.NewTopic[string](orderPlacedTopic.Name()).New(strings.Repeat("x", events.MaximumEventBytes))
	_, err := store.Publish(ctx, pool, orderPlacedTopic.New(orderPlaced{}), large)
	require.ErrorIs(t, err, events.ErrEventTooLarge)
	_, err = store.Publish(ctx, pool, orderPlacedTopic.New(orderPlaced{}), events.NewTopic[int]("orders").New(1))
	require.ErrorIs(t, err, events.ErrInvalidTopic)

	refusing, _ := newStore(t, func(options *postgresadapter.Options) { options.Propagators = []jobs.Propagator{refusingCarrier{}} })
	_, err = refusing.Publish(ctx, pool, orderPlacedTopic.New(orderPlaced{}))
	require.Error(t, err)
	withoutSource, _ := newStore(t, func(options *postgresadapter.Options) { options.Source = "" })
	_, err = withoutSource.Publish(ctx, pool, orderPlacedTopic.New(orderPlaced{}))
	require.Error(t, err)
	require.Zero(t, count(t, pool, "govengers_event_deliveries"))
	require.Zero(t, count(t, pool, "govengers_jobs"))
}

func publishOne(t *testing.T, store *postgresadapter.Store, pool *pgxpool.Pool) events.DeliveryKey {
	t.Helper()
	ids, err := store.Publish(context.Background(), pool, orderPlacedTopic.New(orderPlaced{OrderID: "o-1"}))
	require.NoError(t, err)
	return events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: ids[0]}
}

func record(t *testing.T, store *postgresadapter.Store, key events.DeliveryKey, status events.DeliveryStatus, disableAfter int) *events.RecordedDelivery {
	t.Helper()
	recorded, err := store.RecordDelivery(context.Background(), &events.DeliveryRecord{Key: key, Status: status, StatusCode: 500, Error: "boom", DisableAfter: disableAfter})
	require.NoError(t, err)
	return recorded
}

func TestRecordDeliveryCountsFailuresInARow(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, nil)
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("create-invoice")}))
	subscription := func() *postgresadapter.SubscriptionRecord {
		records, err := store.Subscriptions(ctx, nil)
		require.NoError(t, err)
		return records[0]
	}

	first := publishOne(t, store, pool)
	require.Equal(t, 1, record(t, store, first, events.DeliveryPending, 2).Attempts)
	require.Zero(t, subscription().FailedDeliveries, "a retry does not count")
	require.False(t, record(t, store, first, events.DeliveryFailed, 2).Disabled)
	require.Equal(t, 1, subscription().FailedDeliveries)

	second := publishOne(t, store, pool)
	record(t, store, second, events.DeliveryDelivered, 2)
	require.Zero(t, subscription().FailedDeliveries, "a delivered delivery resets the count")

	third, fourth := publishOne(t, store, pool), publishOne(t, store, pool)
	require.False(t, record(t, store, third, events.DeliveryFailed, 2).Disabled)
	require.True(t, record(t, store, fourth, events.DeliveryFailed, 2).Disabled)
	require.Equal(t, "failures", subscription().DisabledBy)
	require.False(t, record(t, store, fourth, events.DeliveryFailed, 2).Disabled, "only the transition reports")

	// A delivered delivery stays delivered when a stale run records a failure.
	record(t, store, second, events.DeliveryFailed, 2)
	entries, err := store.Deliveries(ctx, &postgresadapter.DeliveryFilter{Status: events.DeliveryDelivered})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, second, entries[0].Key)

	require.NoError(t, store.EnableSubscription(ctx, "billing", "create-invoice"))
	require.Nil(t, subscription().DisabledAt)
	require.Zero(t, subscription().FailedDeliveries)
	require.ErrorIs(t, store.EnableSubscription(ctx, "billing", "unknown"), events.ErrNotFound)
	require.ErrorIs(t, store.DisableSubscription(ctx, "billing", "unknown"), events.ErrNotFound)

	gone := events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: "gone"}
	require.Zero(t, record(t, store, gone, events.DeliveryFailed, 2).Attempts)
	_, err = store.Delivery(ctx, gone)
	require.ErrorIs(t, err, events.ErrNotFound)
	require.ErrorIs(t, store.Redeliver(ctx, gone), events.ErrNotFound)
}

func TestARegistrationEnablesOnlyWhatFailuresDisabled(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, nil)
	set := []events.Subscription{remote("create-invoice"), remote("send-receipt")}
	require.NoError(t, store.PutSubscriptions(ctx, "billing", set))
	_, err := pool.Exec(ctx, `UPDATE govengers_event_subscriptions SET disabled_at = now(), disabled_by = 'failures', failed_deliveries = 5 WHERE name = 'create-invoice'`)
	require.NoError(t, err)
	require.NoError(t, store.DisableSubscription(ctx, "billing", "send-receipt"))

	require.NoError(t, store.PutSubscriptions(ctx, "billing", set))
	records, err := store.Subscriptions(ctx, &postgresadapter.SubscriptionFilter{Topic: orderPlacedTopic.Name()})
	require.NoError(t, err)
	require.Nil(t, records[0].DisabledAt)
	require.Zero(t, records[0].FailedDeliveries)
	require.Equal(t, "admin", records[1].DisabledBy)
}

func TestAcceptEnqueuesOnceUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	store, pool := storeOn(t, repeatableRead(t, postgrestest.New(t)), nil)
	kind := jobs.NewKind[string]("events.billing.create-invoice", &jobs.KindOptions{Queue: "events.billing"})
	var accepted sync.Map
	var group errgroup.Group
	for attempt := range 8 {
		group.Go(func() error {
			ok, err := store.Accept(ctx, &events.Acceptance{Subscriber: "billing", Subscription: "create-invoice", EventID: "e-1", Job: kind.New("e-1", nil)})
			accepted.Store(attempt, ok)
			return err
		})
	}
	require.NoError(t, group.Wait())
	trues := 0
	accepted.Range(func(_, value any) bool {
		if value.(bool) {
			trues++
		}
		return true
	})
	require.Equal(t, 1, trues)
	require.Equal(t, 1, count(t, pool, "govengers_jobs"))
	require.Equal(t, 1, count(t, pool, "govengers_event_inbox"))

	failing := jobs.NewKind[string]("", nil) // an invalid kind fails the enqueue: the key rolls back with it
	_, err := store.Accept(ctx, &events.Acceptance{Subscriber: "billing", Subscription: "create-invoice", EventID: "e-2", Job: failing.New("x", nil)})
	require.ErrorIs(t, err, jobs.ErrInvalidKind)
	require.Equal(t, 1, count(t, pool, "govengers_event_inbox"))
}

func TestDeliveriesPagesAndRedeliverResets(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, nil)
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("create-invoice"), remote("send-receipt")}))
	for range 3 {
		publishOne(t, store, pool)
	}
	var listed []events.DeliveryKey
	var after *events.DeliveryKey
	for {
		page, err := store.Deliveries(ctx, &postgresadapter.DeliveryFilter{After: after, Limit: 4})
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for _, entry := range page {
			listed = append(listed, entry.Key)
		}
		after = &page[len(page)-1].Key
	}
	require.Len(t, listed, 6, "two subscriptions times three events, each once")
	seen := map[events.DeliveryKey]bool{}
	for _, key := range listed {
		require.False(t, seen[key])
		seen[key] = true
	}

	key := listed[0]
	record(t, store, key, events.DeliveryFailed, 0)
	_, err := pool.Exec(ctx, "DELETE FROM govengers_jobs")
	require.NoError(t, err)
	require.NoError(t, store.Redeliver(ctx, key))
	require.NoError(t, store.Redeliver(ctx, key), "a second redelivery reuses the pending job")
	require.Equal(t, 1, count(t, pool, "govengers_jobs"))
	entries, err := store.Deliveries(ctx, &postgresadapter.DeliveryFilter{Subscriber: key.Subscriber, Subscription: key.Subscription, Status: events.DeliveryPending})
	require.NoError(t, err)
	require.Equal(t, key, entries[0].Key)
	require.Zero(t, entries[0].Attempts)
	require.Empty(t, entries[0].LastError)
}

func TestSweepDeletesFinishedRowsPastTheirRetention(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, nil)
	require.NoError(t, store.PutSubscriptions(ctx, "billing", []events.Subscription{remote("create-invoice")}))
	delivered, failed, pending := publishOne(t, store, pool), publishOne(t, store, pool), publishOne(t, store, pool)
	record(t, store, delivered, events.DeliveryDelivered, 0)
	record(t, store, failed, events.DeliveryFailed, 0)
	_, err := pool.Exec(ctx, "UPDATE govengers_event_deliveries SET finished_at = now() - interval '2 hours' WHERE status <> 'pending'")
	require.NoError(t, err)
	for _, id := range []string{"old", "new"} {
		_, err := store.Accept(ctx, &events.Acceptance{Subscriber: "billing", Subscription: "create-invoice", EventID: id,
			Job: jobs.NewKind[string]("events.billing.create-invoice", &jobs.KindOptions{Queue: "events.billing"}).New(id, nil)})
		require.NoError(t, err)
	}
	_, err = pool.Exec(ctx, "UPDATE govengers_event_inbox SET received_at = now() - interval '2 hours' WHERE event_id = 'old'")
	require.NoError(t, err)

	deleted, err := store.Sweep(ctx, &events.SweepRequest{RetainDelivered: 3 * 3600e9, RetainFailed: 3600e9, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "only the failed delivery is past its retention; the inbox is kept forever without one")
	deleted, err = store.Sweep(ctx, &events.SweepRequest{RetainDelivered: 3600e9, RetainInbox: 3600e9, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	entries, err := store.Deliveries(ctx, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, pending, entries[0].Key, "a pending delivery is never swept")
	require.Equal(t, 1, count(t, pool, "govengers_event_inbox"))
	deleted, err = store.Sweep(ctx, &events.SweepRequest{RetainDelivered: 1, Limit: 0})
	require.NoError(t, err)
	require.Zero(t, deleted)
}
