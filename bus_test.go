package events_test

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
	"github.com/govengers/events/postgresadapter"
	"github.com/govengers/jobs"
	"github.com/govengers/jobs/jobstest"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

func TestALocalSubscriptionReceivesEventsOfCommittedTransactionsOnly(t *testing.T) {
	ctx := context.Background()
	orders := newService(t, "orders", nil)
	var invoices received
	require.NoError(t, events.Subscribe(orders.bus, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice"}))
	require.NoError(t, orders.bus.Register(ctx))

	transaction, err := orders.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = orders.store.Publish(ctx, transaction, orderPlacedTopic.New(OrderPlaced{OrderID: "rolled-back", Version: 1}))
	require.NoError(t, err)
	require.NoError(t, transaction.Rollback(ctx))
	id := orders.publish(t, OrderPlaced{OrderID: "o-1", Version: 1})

	orders.drain(t)
	messages := invoices.all()
	require.Len(t, messages, 1)
	require.Equal(t, id, messages[0].ID)
	require.Equal(t, OrderPlaced{OrderID: "o-1", Version: 1}, messages[0].Data)
	require.Equal(t, "orders", messages[0].Source)
	require.Equal(t, "password:ada", messages[0].Actor)
	require.Equal(t, orderPlacedTopic.Name(), messages[0].Topic)
	require.Equal(t, 1, messages[0].Attempt)
}

func TestAnEventWithoutSubscriptionsStoresNothing(t *testing.T) {
	orders := newService(t, "orders", nil)
	orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.Zero(t, orders.drain(t))
	require.Equal(t, events.Publication{Topic: orderPlacedTopic.Name()}, orders.observer.publications[0])
}

// remotePair is a publisher (orders) and a subscriber (billing) with their own databases;
// billing's create-invoice subscription is registered at orders.
func remotePair(t *testing.T, billingOptions *serviceOptions) (orders, billing *service, invoices *received) {
	t.Helper()
	orders = newService(t, "orders", nil)
	if billingOptions == nil {
		billingOptions = &serviceOptions{}
	}
	configure := billingOptions.configure
	billingOptions.configure = func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
		if configure != nil {
			configure(options)
		}
	}
	billing = newService(t, "billing", billingOptions)
	invoices = &received{}
	require.NoError(t, events.Subscribe(billing.bus, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, billing.bus.Register(context.Background()))
	require.Equal(t, 1, billing.drain(t), "the registration job")
	return orders, billing, invoices
}

func TestARemoteSubscriptionRegistersAndReceivesEventsThroughTheInbox(t *testing.T) {
	orders, billing, invoices := remotePair(t, nil)
	record := orders.subscription(t, "billing", "create-invoice")
	require.Equal(t, billing.server.URL+"/events/create-invoice", record.Endpoint)
	require.Equal(t, orderPlacedTopic.Name(), record.Topic)

	id := orders.publish(t, OrderPlaced{OrderID: "o-1", Version: 2})
	require.Equal(t, 1, orders.drain(t), "the delivery job")
	require.Equal(t, 1, billing.drain(t), "the handler job")

	messages := invoices.all()
	require.Len(t, messages, 1)
	require.Equal(t, id, messages[0].ID)
	require.Equal(t, OrderPlaced{OrderID: "o-1", Version: 2}, messages[0].Data)
	key := events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id}
	entry := orders.delivery(t, key)
	require.Equal(t, events.DeliveryDelivered, entry.Status)
	require.Equal(t, 1, entry.Attempts)
	require.Equal(t, http.StatusAccepted, entry.LastStatusCode)

	// A redelivery reaches the inbox, which recognises it: the handler does not run again.
	require.NoError(t, orders.store.Redeliver(context.Background(), key))
	require.Equal(t, 1, orders.drain(t))
	require.Zero(t, billing.drain(t))
	require.Len(t, invoices.all(), 1)
	require.Equal(t, []events.ReceptionResult{events.ReceptionAccepted, events.ReceptionDuplicate}, billing.observer.receptionResults())
}

func TestADeliveredDeliveryIsNotSentAgainByASecondJob(t *testing.T) {
	orders, billing, _ := remotePair(t, nil)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	key := events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id}
	require.NoError(t, orders.store.Enqueue(context.Background(), events.DeliveryJob(key, 0)), "a second job of the same delivery")
	// the unique key coalesced the second job with the pending first one
	require.Equal(t, 1, orders.drain(t))
	require.NoError(t, orders.store.Enqueue(context.Background(), events.DeliveryJob(key, 0)))
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, []events.DeliveryResult{events.DeliveryResultDelivered, events.DeliveryResultDropped}, orders.observer.deliveryResults())
	require.Len(t, billing.observer.receptions, 1)
}

// switchable answers every request with status while it is not zero.
func switchable(status *atomic.Int32) *serviceOptions {
	return &serviceOptions{intercept: func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if code := int(status.Load()); code != 0 {
				if code == http.StatusTooManyRequests {
					writer.Header().Set("Retry-After", "120")
				}
				writer.WriteHeader(code)
				return
			}
			next.ServeHTTP(writer, request)
		})
	}}
}

func TestASubscriberThatIsDownReceivesTheEventWhenItIsBack(t *testing.T) {
	var down atomic.Int32
	orders, billing, invoices := remotePair(t, switchable(&down))
	down.Store(http.StatusServiceUnavailable)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	key := events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id}

	require.Equal(t, 1, orders.drain(t))
	entry := orders.delivery(t, key)
	require.Equal(t, events.DeliveryPending, entry.Status)
	require.Equal(t, http.StatusServiceUnavailable, entry.LastStatusCode)
	require.Contains(t, entry.LastError, "503")
	require.Zero(t, orders.drain(t), "the retry waits for its backoff")

	down.Store(http.StatusNotFound) // mid-rollout: the subscription is not known yet, retried too
	orders.makeDue(t)
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, events.DeliveryPending, orders.delivery(t, key).Status)

	down.Store(0)
	orders.makeDue(t)
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, 1, billing.drain(t))
	require.Len(t, invoices.all(), 1)
	entry = orders.delivery(t, key)
	require.Equal(t, events.DeliveryDelivered, entry.Status)
	require.Equal(t, 3, entry.Attempts)
	require.Equal(t, []events.DeliveryResult{events.DeliveryResultRetried, events.DeliveryResultRetried, events.DeliveryResultDelivered}, orders.observer.deliveryResults())
}

func TestTooManyRequestsSnoozesWithoutSpendingAJobAttempt(t *testing.T) {
	var status atomic.Int32
	orders, _, _ := remotePair(t, switchable(&status))
	status.Store(http.StatusTooManyRequests)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, []events.DeliveryResult{events.DeliveryResultSnoozed}, orders.observer.deliveryResults())
	entry := orders.delivery(t, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id})
	require.Equal(t, events.DeliveryPending, entry.Status)
	require.Equal(t, 1, entry.Attempts, "the ledger counts the attempt, so snoozes cannot repeat forever")

	var runAt, attempt int
	require.NoError(t, orders.pool.QueryRow(context.Background(),
		"SELECT extract(epoch FROM run_at - now())::int, attempt FROM govengers_jobs WHERE status = 'pending'").Scan(&runAt, &attempt))
	require.InDelta(t, 120, runAt, 5, "Retry-After")
	require.Zero(t, attempt)
}

func TestRepeatedFailuresDisableTheSubscription(t *testing.T) {
	var status atomic.Int32
	orders, billing, invoices := remotePair(t, switchable(&status))
	status.Store(http.StatusGone)
	ctx := context.Background()

	first := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	second := orders.publish(t, OrderPlaced{OrderID: "o-2"})
	require.Equal(t, 2, orders.drain(t))
	require.Equal(t, events.DeliveryFailed, orders.delivery(t, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: first}).Status)
	record := orders.subscription(t, "billing", "create-invoice")
	require.Equal(t, 2, record.FailedDeliveries)
	require.Nil(t, record.DisabledAt, "the default threshold is 5")

	for index := range 3 {
		orders.publish(t, OrderPlaced{OrderID: "o-more", Version: index})
	}
	require.Equal(t, 3, orders.drain(t))
	record = orders.subscription(t, "billing", "create-invoice")
	require.NotNil(t, record.DisabledAt)
	require.Equal(t, "failures", record.DisabledBy)
	disabled := 0
	for _, attempt := range orders.observer.deliveries {
		if attempt.Disabled {
			disabled++
		}
	}
	require.Equal(t, 1, disabled, "one attempt reports the disable")

	// Disabled: Publish skips it. Billing's next registration enables it again.
	orders.publish(t, OrderPlaced{OrderID: "o-skipped"})
	require.Zero(t, orders.drain(t))
	status.Store(0)
	require.NoError(t, billing.bus.Register(ctx))
	require.Equal(t, 1, billing.drain(t))
	record = orders.subscription(t, "billing", "create-invoice")
	require.Nil(t, record.DisabledAt)
	require.Zero(t, record.FailedDeliveries)

	// A failed delivery goes through once redelivered.
	require.NoError(t, orders.store.Redeliver(ctx, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: second}))
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, 1, billing.drain(t))
	require.Equal(t, "o-2", invoices.all()[0].Data.OrderID)
}

func TestAPendingDeliveryOfADisabledSubscriptionIsRecordedAsFailedUnsent(t *testing.T) {
	orders, billing, _ := remotePair(t, nil)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.NoError(t, orders.store.DisableSubscription(context.Background(), "billing", "create-invoice"))
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, []events.DeliveryResult{events.DeliveryResultSkipped}, orders.observer.deliveryResults())
	entry := orders.delivery(t, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id})
	require.Equal(t, events.DeliveryFailed, entry.Status)
	require.Equal(t, "subscription disabled", entry.LastError)
	require.Empty(t, billing.observer.receptions)

	// An administrator's disable survives the subscriber's next registration.
	require.NoError(t, billing.bus.Register(context.Background()))
	require.Equal(t, 1, billing.drain(t))
	require.Equal(t, "admin", orders.subscription(t, "billing", "create-invoice").DisabledBy)
}

func TestADeliveryOfARemovedSubscriptionFailsUncountedAndIsSwept(t *testing.T) {
	ctx := context.Background()
	orders, _, _ := remotePair(t, nil)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.NoError(t, orders.store.PutSubscriptions(ctx, "billing", nil))
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, []events.DeliveryResult{events.DeliveryResultSkipped}, orders.observer.deliveryResults())
	entry := orders.delivery(t, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id})
	require.Equal(t, events.DeliveryFailed, entry.Status, "not pending for good")
	require.Equal(t, "the subscription was removed or made local", entry.LastError)

	_, err := orders.pool.Exec(ctx, "UPDATE govengers_event_deliveries SET finished_at = now() - interval '400 days'")
	require.NoError(t, err)
	deleted, err := orders.store.Sweep(ctx, &events.SweepRequest{RetainFailed: time.Hour, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
}

func TestATopicChangeFailsPendingDeliveriesWithoutCountingThem(t *testing.T) {
	ctx := context.Background()
	orders, billing, _ := remotePair(t, nil)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.NoError(t, orders.store.PutSubscriptions(ctx, "billing", []events.Subscription{{
		Name: "create-invoice", Topic: "orders.order-placed.v2", Endpoint: billing.server.URL + "/events/create-invoice",
		Secret: events.DeriveSecret(inboxKey, "create-invoice"),
	}}))
	require.Equal(t, 1, orders.drain(t))
	entry := orders.delivery(t, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id})
	require.Equal(t, events.DeliveryFailed, entry.Status)
	require.Zero(t, entry.LastStatusCode, "nothing was sent")
	require.Zero(t, orders.subscription(t, "billing", "create-invoice").FailedDeliveries)
	require.Empty(t, billing.observer.receptions)
}

func TestARedeliveryDuringABackoffSendsOnceAndCountsOnce(t *testing.T) {
	ctx := context.Background()
	var status atomic.Int32
	orders, _, _ := remotePair(t, switchable(&status))
	status.Store(http.StatusServiceUnavailable)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	key := events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id}
	require.Equal(t, 1, orders.drain(t), "attempt 1 answered 503, the job waits for its backoff")
	require.NoError(t, orders.store.Redeliver(ctx, key))
	status.Store(http.StatusGone)
	orders.makeDue(t)
	require.Equal(t, 2, orders.drain(t), "the waiting job and the redelivery's job")
	require.Equal(t, []int{503, 410}, orders.observer.statusCodes(), "the older generation sent nothing")
	require.Equal(t, 1, orders.subscription(t, "billing", "create-invoice").FailedDeliveries)
	entry := orders.delivery(t, key)
	require.Equal(t, events.DeliveryFailed, entry.Status)
	require.Equal(t, 1, entry.Attempts)
}

func TestAFailedDeliveryRunAgainIsNotSentOrCountedAgain(t *testing.T) {
	ctx := context.Background()
	var status atomic.Int32
	orders, _, _ := remotePair(t, switchable(&status))
	status.Store(http.StatusGone)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	key := events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id}
	require.Equal(t, 1, orders.drain(t))
	// the worker died after the record: jobs runs the job of the same generation again
	require.NoError(t, orders.store.Enqueue(ctx, events.DeliveryJob(key, 0)))
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, []int{410}, orders.observer.statusCodes())
	require.Equal(t, 1, orders.subscription(t, "billing", "create-invoice").FailedDeliveries)
}

func TestARegistrationRetriesUntilThePublisherAnswers(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	orders := newService(t, "orders", switchable(&status))
	billing := newService(t, "billing", &serviceOptions{configure: func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
	}})
	var invoices received
	require.NoError(t, events.Subscribe(billing.bus, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, billing.bus.Register(context.Background()), "the service starts while the publisher is down")
	require.Equal(t, 1, billing.drain(t))
	records, err := orders.store.Subscriptions(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, records)

	status.Store(0)
	billing.makeDue(t)
	require.Equal(t, 1, billing.drain(t))
	require.Equal(t, "create-invoice", orders.subscription(t, "billing", "create-invoice").Name)
	require.Len(t, billing.observer.registrations, 2)
	require.Equal(t, http.StatusServiceUnavailable, billing.observer.registrations[0].StatusCode)
	require.Equal(t, http.StatusNoContent, billing.observer.registrations[1].StatusCode)
}

// replica is another replica of service s, of another version: its own worker and bus on the
// same database and store. configure adjusts the bus's options.
func (s *service) replica(t *testing.T, configure func(options *events.Options)) (*events.Bus, *jobs.Worker) {
	t.Helper()
	jobStore, err := jobspostgres.New(&jobspostgres.Options{Pool: s.pool})
	require.NoError(t, err)
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{Store: jobStore, Queues: map[string]jobs.QueueOptions{events.Queue: {}, events.SubscriberQueue(s.name): {}}})
	require.NoError(t, err)
	options := &events.Options{
		Name: s.name, Store: s.store, Worker: worker, HTTPClient: &http.Client{Timeout: 10 * time.Second},
		InboxKey: inboxKey, InboxURL: s.server.URL + "/events", AllowHTTP: true,
	}
	if configure != nil {
		configure(options)
	}
	bus, err := events.New(options)
	require.NoError(t, err)
	return bus, worker
}

func registeredNames(t *testing.T, publisher *service, subscriber string) []string {
	t.Helper()
	records, err := publisher.store.Subscriptions(context.Background(), &postgresadapter.SubscriptionFilter{Subscriber: subscriber})
	require.NoError(t, err)
	names := []string{}
	for _, record := range records {
		names = append(names, record.Name)
	}
	return names
}

func TestAnOldReplicaSendsTheSetOfTheReplicaThatRegistered(t *testing.T) {
	ctx := context.Background()
	orders := newService(t, "orders", nil)
	old := newService(t, "billing", &serviceOptions{configure: func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
	}})
	var invoices received
	require.NoError(t, events.Subscribe(old.bus, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, old.bus.Register(ctx))
	require.Equal(t, 1, old.drain(t))

	next, nextWorker := old.replica(t, func(options *events.Options) { options.Publishers = []events.Publisher{publisherOf(orders)} })
	require.NoError(t, events.Subscribe(next, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, events.Subscribe(next, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "send-receipt", Publisher: "orders"}))
	require.NoError(t, next.Register(ctx))
	require.Equal(t, 1, old.drain(t), "the old replica claims the new replica's job")
	require.Zero(t, jobstest.Drain(t, nextWorker))
	require.Equal(t, []string{"create-invoice", "send-receipt"}, registeredNames(t, orders, "billing"))
}

func TestAReplicaWithoutThePublisherOrTheInboxKeyLeavesTheRegistrationToAnother(t *testing.T) {
	ctx := context.Background()
	orders := newService(t, "orders", nil)
	old := newService(t, "billing", nil) // declares no publisher
	other, otherWorker := old.replica(t, func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
		options.InboxKey = []byte("another-inbox-key-0123456789abcdef")
	})
	next, nextWorker := old.replica(t, func(options *events.Options) { options.Publishers = []events.Publisher{publisherOf(orders)} })
	var invoices received
	require.NoError(t, events.Subscribe(other, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, events.Subscribe(next, orderPlacedTopic, invoices.handle, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, next.Register(ctx))

	require.Equal(t, 1, old.drain(t), "a replica without the publisher retries")
	old.makeDue(t)
	require.Equal(t, 1, jobstest.Drain(t, otherWorker), "a replica with another inbox key retries")
	require.Empty(t, registeredNames(t, orders, "billing"))
	old.makeDue(t)
	require.Equal(t, 1, jobstest.Drain(t, nextWorker))
	require.Equal(t, []string{"create-invoice"}, registeredNames(t, orders, "billing"))

	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	delivery, err := orders.store.Delivery(ctx, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: id})
	require.NoError(t, err)
	require.Equal(t, events.DeriveSecret(inboxKey, "create-invoice"), delivery.Secret, "the secret of the job's inbox key")
}

func TestTheSweepScheduleRunsTheStoresSweep(t *testing.T) {
	orders, _, _ := remotePair(t, nil)
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.Equal(t, 1, orders.drain(t))
	_, err := orders.pool.Exec(context.Background(), "UPDATE govengers_event_deliveries SET finished_at = now() - interval '8 days'")
	require.NoError(t, err)

	schedule := events.SweepSchedule()
	require.Equal(t, "govengers.events.sweep", schedule.Name)
	require.NoError(t, orders.store.Enqueue(context.Background(), schedule.Job))
	require.Equal(t, 1, orders.drain(t))
	entries, err := orders.store.Deliveries(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, entries, "the delivery of %s was swept", id)
}

func TestOneSweepRunDeletesEveryBatchPastTheRetention(t *testing.T) {
	ctx := context.Background()
	orders := newService(t, "orders", nil)
	_, err := orders.pool.Exec(ctx, `INSERT INTO govengers_event_deliveries (subscriber, subscription, event_id, topic, event, status, finished_at)
		SELECT 'billing', 'create-invoice', 'event-' || n, 'orders.order-placed.v1', '{}', 'delivered', now() - interval '8 days'
		FROM generate_series(1, 2500) n`)
	require.NoError(t, err)
	require.NoError(t, orders.store.Enqueue(ctx, events.SweepSchedule().Job))
	require.Equal(t, 1, orders.drain(t))
	var left int
	require.NoError(t, orders.pool.QueryRow(ctx, "SELECT count(*) FROM govengers_event_deliveries").Scan(&left))
	require.Zero(t, left, "three batches of at most 1000 in one run")
}

func TestDataThatDoesNotDecodeIsAPermanentFailure(t *testing.T) {
	orders := newService(t, "orders", nil)
	called := false
	require.NoError(t, events.Subscribe(orders.bus, orderPlacedTopic, func(context.Context, *events.Message[OrderPlaced]) error {
		called = true
		return nil
	}, &events.SubscribeOptions{Name: "create-invoice"}))
	require.NoError(t, orders.bus.Register(context.Background()))
	_, err := orders.store.Publish(context.Background(), orders.pool, events.NewTopic[string](orderPlacedTopic.Name()).New("not an object"))
	require.NoError(t, err)
	require.Equal(t, 1, orders.drain(t))
	require.False(t, called)
	var status string
	require.NoError(t, orders.pool.QueryRow(context.Background(), "SELECT status FROM govengers_jobs").Scan(&status))
	require.Equal(t, "dead", status)
}

type panickingObserver struct{}

func (panickingObserver) Published(*events.Publication)            { panic("observer bug") }
func (panickingObserver) DeliveryFinished(*events.DeliveryAttempt) { panic("observer bug") }
func (panickingObserver) Received(*events.Reception)               { panic("observer bug") }
func (panickingObserver) Registered(*events.Registration)          { panic("observer bug") }

func TestAPanickingObserverIsDropped(t *testing.T) {
	observer := events.GuardObserver(panickingObserver{})
	require.NotPanics(t, func() {
		observer.Published(&events.Publication{})
		observer.DeliveryFinished(&events.DeliveryAttempt{})
		observer.Received(&events.Reception{})
		observer.Registered(&events.Registration{})
	})
	silent := events.GuardObserver(nil)
	silent.Published(&events.Publication{})
	silent.DeliveryFinished(&events.DeliveryAttempt{})
	silent.Received(&events.Reception{})
	silent.Registered(&events.Registration{})
}

func TestTheDefaultClientRefusesInternalAddressesForGood(t *testing.T) {
	orders := newService(t, "orders", &serviceOptions{configure: func(options *events.Options) { options.HTTPClient = nil }})
	require.NoError(t, orders.store.PutSubscriptions(context.Background(), "intruder", []events.Subscription{{
		Name: "metadata", Topic: orderPlacedTopic.Name(), Endpoint: "http://169.254.169.254/latest/meta-data", Secret: events.NewSecret(),
	}}))
	id := orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.Equal(t, 1, orders.drain(t))
	entry := orders.delivery(t, events.DeliveryKey{Subscriber: "intruder", Subscription: "metadata", EventID: id})
	require.Equal(t, events.DeliveryFailed, entry.Status, "no retry")
	require.Contains(t, entry.LastError, "blocked address")
}

// largestEvent prepares an event of the jsonb-heaviest shape, an array of zeros, as close to
// MaximumEventBytes as the envelope leaves room for.
func largestEvent(t *testing.T) (*events.Topic[[]int], *events.PreparedEvent) {
	t.Helper()
	topic := events.NewTopic[[]int](orderPlacedTopic.Name())
	return topic, topic.New(make([]int, (events.MaximumEventBytes-1024)/2))
}

func TestAnEventAtTheLimitReachesARemoteHandler(t *testing.T) {
	ctx := context.Background()
	orders := newService(t, "orders", nil)
	billing := newService(t, "billing", &serviceOptions{configure: func(options *events.Options) {
		options.Publishers = []events.Publisher{publisherOf(orders)}
	}})
	topic, event := largestEvent(t)
	var handled atomic.Int64
	require.NoError(t, events.Subscribe(billing.bus, topic, func(_ context.Context, message *events.Message[[]int]) error {
		handled.Store(int64(len(message.Data)))
		return nil
	}, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"}))
	require.NoError(t, billing.bus.Register(ctx))
	require.Equal(t, 1, billing.drain(t))

	ids, err := orders.store.Publish(ctx, orders.pool, event)
	require.NoError(t, err)
	require.Equal(t, 1, orders.drain(t))
	entry := orders.delivery(t, events.DeliveryKey{Subscriber: "billing", Subscription: "create-invoice", EventID: ids[0]})
	require.Equal(t, events.DeliveryDelivered, entry.Status, "the inbox took the bytes Publish checked: %s", entry.LastError)
	require.Equal(t, 1, billing.drain(t))
	require.Equal(t, int64((events.MaximumEventBytes-1024)/2), handled.Load())
}

func TestAnEventAtTheLimitReachesALocalHandler(t *testing.T) {
	ctx := context.Background()
	orders := newService(t, "orders", nil)
	topic, event := largestEvent(t)
	var handled atomic.Bool
	require.NoError(t, events.Subscribe(orders.bus, topic, func(context.Context, *events.Message[[]int]) error {
		handled.Store(true)
		return nil
	}, &events.SubscribeOptions{Name: "count"}))
	require.NoError(t, orders.bus.Register(ctx))
	_, err := orders.store.Publish(ctx, orders.pool, event)
	require.NoError(t, err, "the job's jsonb payload stays within jobs' cap")
	require.Equal(t, 1, orders.drain(t))
	require.True(t, handled.Load())
}

func TestANulCharacterIsRefusedBeforeTheFirstWrite(t *testing.T) {
	ctx := context.Background()
	orders, _, _ := remotePair(t, nil)
	transaction, err := orders.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = transaction.Rollback(ctx) }()
	_, err = orders.store.Publish(ctx, transaction, orderPlacedTopic.New(OrderPlaced{OrderID: "a\x00b"}))
	require.ErrorIs(t, err, events.ErrNulCharacter)
	_, err = orders.store.Publish(ctx, transaction, orderPlacedTopic.New(OrderPlaced{OrderID: `a\u0000b`}))
	require.NoError(t, err, "the caller's transaction is intact, and the text \\u0000 is no NUL")
}
