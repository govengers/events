package events_test

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
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
	require.NoError(t, orders.store.Enqueue(context.Background(), events.DeliveryJob(key)), "a second job of the same delivery")
	// the unique key coalesced the second job with the pending first one
	require.Equal(t, 1, orders.drain(t))
	require.NoError(t, orders.store.Enqueue(context.Background(), events.DeliveryJob(key)))
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

func TestADeliveryOfARemovedSubscriptionIsDropped(t *testing.T) {
	orders, _, _ := remotePair(t, nil)
	orders.publish(t, OrderPlaced{OrderID: "o-1"})
	require.NoError(t, orders.store.PutSubscriptions(context.Background(), "billing", nil))
	require.Equal(t, 1, orders.drain(t))
	require.Equal(t, []events.DeliveryResult{events.DeliveryResultDropped}, orders.observer.deliveryResults())
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
