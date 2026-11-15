package events

import "time"

// Observer receives what the library would otherwise have to log (LIB-003, LIB-012). It is
// called from many goroutines; implementations must be safe for concurrent use and fast. A
// panicking call is dropped. The interface may gain methods in minor versions, so an
// implementation outside this module should expect that.
type Observer interface {
	// Published reports one published event.
	Published(publication *Publication)
	// DeliveryFinished reports one outbound delivery attempt.
	DeliveryFinished(attempt *DeliveryAttempt)
	// Received reports one request to the inbox.
	Received(reception *Reception)
	// Registered reports one registration: sent to a publisher, or received from a subscriber.
	Registered(registration *Registration)
}

// Publication is one published event.
type Publication struct {
	Topic  string
	Local  int // jobs enqueued for local subscriptions
	Remote int // deliveries recorded for remote subscriptions
	Err    error
}

// DeliveryResult is how one outbound delivery attempt ended.
type DeliveryResult string

// The results of a delivery attempt.
const (
	DeliveryResultDelivered DeliveryResult = "delivered" // the subscriber answered 2xx
	DeliveryResultRetried   DeliveryResult = "retried"   // a retryable failure; the job retries after its backoff
	DeliveryResultSnoozed   DeliveryResult = "snoozed"   // 429 with Retry-After; the job waits that long
	DeliveryResultFailed    DeliveryResult = "failed"    // refused, or the last attempt failed
	DeliveryResultDropped   DeliveryResult = "dropped"   // the subscription or delivery is gone, or it was delivered before
	DeliveryResultSkipped   DeliveryResult = "skipped"   // the subscription is disabled; recorded as failed, nothing sent
)

// DeliveryAttempt is one outbound delivery attempt.
type DeliveryAttempt struct {
	Key        DeliveryKey
	Topic      string // empty when the delivery was dropped before it was loaded
	Result     DeliveryResult
	StatusCode int // zero without a response
	Duration   time.Duration
	Err        error
	Disabled   bool // this attempt disabled the subscription
}

// ReceptionResult is how the inbox answered one request.
type ReceptionResult string

// The results of an inbox request.
const (
	ReceptionAccepted            ReceptionResult = "accepted"             // new: the handler's job was enqueued
	ReceptionDuplicate           ReceptionResult = "duplicate"            // received before; answered 202, nothing enqueued
	ReceptionUnknownSubscription ReceptionResult = "unknown_subscription" // 404
	ReceptionInvalidSignature    ReceptionResult = "invalid_signature"    // 401
	ReceptionInvalidEvent        ReceptionResult = "invalid_event"        // 400: not a CloudEvent, or of another topic
	ReceptionTooLarge            ReceptionResult = "too_large"            // 413
	ReceptionFailed              ReceptionResult = "failed"               // 500: the store failed
)

// Reception is one request to the inbox.
type Reception struct {
	Subscription string // as requested; may be unknown
	EventID      string // empty when the body was not read
	Result       ReceptionResult
	Err          error
}

// Registration is one registration of a subscriber's set at a publisher.
type Registration struct {
	Publisher     string // the publisher's name; this service's own on the receiving side
	Subscriber    string
	Subscriptions int
	Inbound       bool // received from a subscriber, not sent to a publisher
	StatusCode    int  // the answer's HTTP status
	Err           error
}

// silentObserver is the Observer of a nil Options.Observer.
type silentObserver struct{}

func (silentObserver) Published(*Publication)            {}
func (silentObserver) DeliveryFinished(*DeliveryAttempt) {}
func (silentObserver) Received(*Reception)               {}
func (silentObserver) Registered(*Registration)          {}

// GuardObserver returns observer with every call's panic recovered and dropped, so an Observer
// bug never fails a delivery or a request; nil gives an Observer that reports nothing.
func GuardObserver(observer Observer) Observer {
	if observer == nil {
		return silentObserver{}
	}
	return guardedObserver{observer: observer}
}

type guardedObserver struct{ observer Observer }

func (g guardedObserver) Published(publication *Publication) {
	defer recoverObserver()
	g.observer.Published(publication)
}

func (g guardedObserver) DeliveryFinished(attempt *DeliveryAttempt) {
	defer recoverObserver()
	g.observer.DeliveryFinished(attempt)
}

func (g guardedObserver) Received(reception *Reception) {
	defer recoverObserver()
	g.observer.Received(reception)
}

func (g guardedObserver) Registered(registration *Registration) {
	defer recoverObserver()
	g.observer.Registered(registration)
}

func recoverObserver() { _ = recover() } // the Observer contract: a panicking report is dropped
