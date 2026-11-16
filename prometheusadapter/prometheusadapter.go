// Package prometheusadapter exports the metrics of govengers/events on a Prometheus registry the
// caller passes in (LIB-011). Metrics implements events.Observer; give it to events.Options.Observer
// and postgresadapter.Options.Observer. The jobs that carry the events report through
// govengers/jobs' own prometheusadapter.
package prometheusadapter

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/govengers/events"
)

const subsystem = "events"

// The values of the outcome label.
const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
)

// deliveryBuckets reach from 10 ms to about 30 s, the default delivery timeout.
var deliveryBuckets = prometheus.ExponentialBuckets(0.01, 2.5, 10)

// Options configures Metrics.
type Options struct {
	Registerer prometheus.Registerer // required; the caller's registry, never the default one
	Namespace  string                // required: the service name, e.g. "orders" for orders_events_published_total
}

// Metrics records publications, deliveries, inbox requests and registrations; it implements
// events.Observer and is safe for concurrent use. Every label is a closed set: topics and this
// service's subscription names are declared in code, results are the events constants; an
// unknown subscription requested from outside is counted with an empty subscription label.
//
//	<namespace>_events_published_total{topic,outcome}
//	<namespace>_events_deliveries_total{topic,result}
//	<namespace>_events_delivery_duration_seconds{topic}
//	<namespace>_events_subscriptions_disabled_total
//	<namespace>_events_received_total{subscription,result}
//	<namespace>_events_registrations_total{direction,outcome}
type Metrics struct {
	published         *prometheus.CounterVec
	deliveries        *prometheus.CounterVec
	deliveryDurations *prometheus.HistogramVec
	disabled          prometheus.Counter
	received          *prometheus.CounterVec
	registrations     *prometheus.CounterVec
}

var _ events.Observer = (*Metrics)(nil)

// New registers the metrics on options.Registerer, all or none. A failed registration, a
// duplicate for example, is returned as an error.
func New(options *Options) (*Metrics, error) {
	if options == nil || options.Registerer == nil {
		return nil, errors.New("prometheusadapter: Registerer is required")
	}
	if options.Namespace == "" {
		return nil, errors.New("prometheusadapter: Namespace is required")
	}
	metrics := newMetrics(options.Namespace)
	collectors := []prometheus.Collector{
		metrics.published, metrics.deliveries, metrics.deliveryDurations, metrics.disabled, metrics.received, metrics.registrations,
	}
	for index, collector := range collectors {
		if err := options.Registerer.Register(collector); err != nil {
			for _, registered := range collectors[:index] {
				options.Registerer.Unregister(registered)
			}
			return nil, fmt.Errorf("prometheusadapter: register the %s_%s metrics: %v", options.Namespace, subsystem, err) //nolint:errorlint // LIB-010: client_golang errors are not part of the API
		}
	}
	return metrics, nil
}

func newMetrics(namespace string) *Metrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Subsystem: subsystem, Name: name, Help: help}, labels)
	}
	return &Metrics{
		published:  counter("published_total", "Published events by topic and outcome.", "topic", "outcome"),
		deliveries: counter("deliveries_total", "Outbound delivery attempts by topic and result.", "topic", "result"),
		deliveryDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "delivery_duration_seconds",
			Help: "Duration of outbound delivery attempts by topic.", Buckets: deliveryBuckets,
		}, []string{"topic"}),
		disabled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "subscriptions_disabled_total",
			Help: "Remote subscriptions disabled after failed deliveries in a row.",
		}),
		received:      counter("received_total", "Inbox requests by subscription and result.", "subscription", "result"),
		registrations: counter("registrations_total", "Registrations sent to publishers or received from subscribers.", "direction", "outcome"),
	}
}

func outcomeOf(err error) string {
	if err != nil {
		return outcomeFailure
	}
	return outcomeSuccess
}

// Published implements events.Observer.
func (m *Metrics) Published(publication *events.Publication) {
	increment(m.published, publication.Topic, outcomeOf(publication.Err))
}

// DeliveryFinished implements events.Observer.
func (m *Metrics) DeliveryFinished(attempt *events.DeliveryAttempt) {
	increment(m.deliveries, attempt.Topic, string(attempt.Result))
	if histogram, err := m.deliveryDurations.GetMetricWithLabelValues(attempt.Topic); err == nil {
		histogram.Observe(attempt.Duration.Seconds())
	}
	if attempt.Disabled {
		m.disabled.Inc()
	}
}

// Received implements events.Observer.
func (m *Metrics) Received(reception *events.Reception) {
	subscription := reception.Subscription
	if reception.Result == events.ReceptionUnknownSubscription {
		subscription = "" // chosen by the caller: unbounded
	}
	increment(m.received, subscription, string(reception.Result))
}

// Registered implements events.Observer.
func (m *Metrics) Registered(registration *events.Registration) {
	direction := "sent"
	if registration.Inbound {
		direction = "received"
	}
	increment(m.registrations, direction, outcomeOf(registration.Err))
}

// increment skips a label value Prometheus refuses (invalid UTF-8): an observer must not panic.
func increment(counter *prometheus.CounterVec, labels ...string) {
	if metric, err := counter.GetMetricWithLabelValues(labels...); err == nil {
		metric.Inc()
	}
}
