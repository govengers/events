package prometheusadapter_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/govengers/events"
	"github.com/govengers/events/prometheusadapter"
)

func TestMetricsCountEveryReport(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "orders"})
	require.NoError(t, err)

	metrics.Published(&events.Publication{Topic: "orders.order-placed.v1", Remote: 1})
	metrics.Published(&events.Publication{Topic: "orders.order-placed.v1", Err: errors.New("boom")})
	metrics.DeliveryFinished(&events.DeliveryAttempt{Topic: "orders.order-placed.v1", Result: events.DeliveryResultFailed, Disabled: true})
	metrics.DeliveryFinished(&events.DeliveryAttempt{Topic: "orders.order-placed.v1", Result: events.DeliveryResultDelivered})
	metrics.Received(&events.Reception{Subscription: "create-invoice", Result: events.ReceptionAccepted})
	metrics.Received(&events.Reception{Subscription: "random-from-outside", Result: events.ReceptionUnknownSubscription})
	metrics.Registered(&events.Registration{Inbound: true})
	metrics.Registered(&events.Registration{Err: errors.New("down")})
	metrics.Published(&events.Publication{Topic: "\xff"}) // refused label value: not counted, no panic

	expected := `
# HELP orders_events_deliveries_total Outbound delivery attempts by topic and result.
# TYPE orders_events_deliveries_total counter
orders_events_deliveries_total{result="delivered",topic="orders.order-placed.v1"} 1
orders_events_deliveries_total{result="failed",topic="orders.order-placed.v1"} 1
# HELP orders_events_published_total Published events by topic and outcome.
# TYPE orders_events_published_total counter
orders_events_published_total{outcome="failure",topic="orders.order-placed.v1"} 1
orders_events_published_total{outcome="success",topic="orders.order-placed.v1"} 1
# HELP orders_events_received_total Inbox requests by subscription and result.
# TYPE orders_events_received_total counter
orders_events_received_total{result="accepted",subscription="create-invoice"} 1
orders_events_received_total{result="unknown_subscription",subscription=""} 1
# HELP orders_events_registrations_total Registrations sent to publishers or received from subscribers.
# TYPE orders_events_registrations_total counter
orders_events_registrations_total{direction="received",outcome="success"} 1
orders_events_registrations_total{direction="sent",outcome="failure"} 1
# HELP orders_events_subscriptions_disabled_total Remote subscriptions disabled after failed deliveries in a row.
# TYPE orders_events_subscriptions_disabled_total counter
orders_events_subscriptions_disabled_total 1
`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expected),
		"orders_events_published_total", "orders_events_deliveries_total", "orders_events_received_total",
		"orders_events_registrations_total", "orders_events_subscriptions_disabled_total"))
	require.Equal(t, 1, testutil.CollectAndCount(registry, "orders_events_delivery_duration_seconds"))
}

func TestNewRegistersAllOrNone(t *testing.T) {
	_, err := prometheusadapter.New(nil)
	require.Error(t, err)
	_, err = prometheusadapter.New(&prometheusadapter.Options{Registerer: prometheus.NewRegistry()})
	require.Error(t, err)

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: "orders_events_received_total", Help: "taken"})))
	_, err = prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "orders"})
	require.Error(t, err)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1, "the earlier collectors were unregistered again")
}
