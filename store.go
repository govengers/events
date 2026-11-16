package events

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/govengers/jobs"
)

// ErrNotFound is returned by the store for a subscription or delivery that does not exist.
var ErrNotFound = errors.New("events: not found")

// ErrUnstorableEvent is returned by the store's Accept for an event that PostgreSQL's jsonb cannot
// hold: a lone surrogate escape, a number outside numeric's range, invalid UTF-8. The inbox
// answers 400, so the sender fails the delivery at once instead of retrying it.
var ErrUnstorableEvent = errors.New("events: the event cannot be stored as jsonb")

// ErrInvalidName is returned for a subscriber or subscription name that is not lower-case words
// joined by dots or hyphens, or longer than 100 characters.
var ErrInvalidName = errors.New("events: invalid name")

var namePattern = regexp.MustCompile(`^[a-z0-9]+([.-][a-z0-9]+)*$`)

const maximumNameLength = 100

// ValidateName returns ErrInvalidName unless name is a well-formed subscriber or subscription name.
func ValidateName(name string) error {
	if len(name) > maximumNameLength || !namePattern.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	return nil
}

// Sealer encrypts subscription secrets at rest. associatedData binds a sealed value to its row:
// Open fails for a value moved to another subscription. The seal package of
// github.com/govengers/auth has another method set (a struct of associated data, a text value);
// wrap its key ring in the ten lines docs/wiki/Security.md shows.
type Sealer interface {
	Seal(plaintext, associatedData []byte) ([]byte, error)
	Open(sealed, associatedData []byte) ([]byte, error)
}

// Subscription is one stored subscription at a publisher: which subscriber receives which topic,
// and how. A subscription with an Endpoint is remote, delivered by a signed POST; one without is
// local, delivered as a job on Queue in the publisher's own database.
type Subscription struct {
	Subscriber string // the subscribing service
	Name       string // unique per subscriber
	Topic      string
	Endpoint   string // remote: the subscriber's inbox URL for this subscription
	Secret     string // remote: the Standard Webhooks signing secret; sealed at rest
	// Queue and MaximumAttempts are the local subscription's job: Publish enqueues it directly.
	Queue           string
	MaximumAttempts int
}

// Remote reports whether the subscription is delivered over HTTP.
func (s *Subscription) Remote() bool { return s.Endpoint != "" }

// DeliveryKey identifies one delivery: one event to one remote subscription. The delivery ledger
// holds at most one delivery per key.
type DeliveryKey struct {
	Subscriber   string `json:"subscriber"`
	Subscription string `json:"subscription"`
	EventID      string `json:"eventId"`
}

// DeliveryStatus is the state of a delivery in the ledger.
type DeliveryStatus string

// The states of a delivery. A delivery is pending from Publish until the subscriber accepted it
// (delivered) or its attempts are used up or the subscriber refused it (failed). Redeliver
// returns it to pending.
const (
	DeliveryPending   DeliveryStatus = "pending"
	DeliveryDelivered DeliveryStatus = "delivered"
	DeliveryFailed    DeliveryStatus = "failed"
)

// OutboundDelivery is what the outbound job needs to send one delivery.
type OutboundDelivery struct {
	Key        DeliveryKey
	Generation int // counted up by every Redeliver; a job of another generation sends nothing
	Topic      string
	Body       []byte // the encoded Event
	Endpoint   string
	Secret     string // opened; empty unless the delivery is pending and can be sent
	Status     DeliveryStatus
	Attempts   int  // attempts recorded so far
	Disabled   bool // the subscription is disabled: nothing is sent
	// Obsolete says why the delivery cannot be sent any more: its subscription was removed, made
	// local or subscribes another topic now. Empty while it can be sent.
	Obsolete string
}

// DeliveryRecord is the result of one delivery attempt for the ledger.
type DeliveryRecord struct {
	Key        DeliveryKey
	Generation int // the attempt's generation; a record of another generation changes nothing
	Status     DeliveryStatus
	StatusCode int    // the subscriber's HTTP status; zero for a transport error or no request
	Error      string // empty when delivered
	// DisableAfter disables the subscription once this many deliveries in a row failed; a
	// delivered one resets the count. Only a transition from pending to failed counts.
	DisableAfter int
	// Uncounted keeps a failure out of the count: the subscriber did not cause it (the
	// subscription is disabled or obsolete, or the delivery could not be loaded).
	Uncounted bool
}

// RecordedDelivery is the ledger's state after RecordDelivery.
type RecordedDelivery struct {
	Attempts int
	Disabled bool // this record disabled the subscription
}

// Acceptance is one delivery the inbox accepted: Accept stores its key and enqueues Job, both
// or neither.
type Acceptance struct {
	Subscriber   string
	Subscription string
	EventID      string
	// Event is the event as received; Accept fails with ErrUnstorableEvent when PostgreSQL's
	// jsonb, which holds the handler's job, cannot store it.
	Event []byte
	Job   *jobs.PreparedJob
}

// SweepRequest asks the store to delete old ledger and inbox rows.
type SweepRequest struct {
	RetainDelivered time.Duration // delivered deliveries are kept this long after they were sent
	RetainFailed    time.Duration // failed deliveries are kept this long after they failed
	RetainInbox     time.Duration // inbox keys are kept this long; must outlast the sender's retries
	Limit           int           // rows per table and call
}

// Store persists subscriptions, the delivery ledger and the inbox; postgresadapter.Store
// implements it, and its Publish is the publishing side. The interface may gain methods in minor
// versions, so an implementation outside this module should expect that.
type Store interface {
	// PutSubscriptions replaces the subscriber's subscriptions with the given set in one
	// transaction (E5): new ones are added, changed ones updated, missing ones deleted.
	PutSubscriptions(ctx context.Context, subscriber string, subscriptions []Subscription) error
	// PutRegistration is PutSubscriptions for a subscriber's registration made at registeredAt
	// (the subscriber's clock). It changes nothing and reports false when a registration made
	// later was stored before, so a delayed or reordered registration cannot replace a newer
	// set (E31). A zero registeredAt is not ordered.
	PutRegistration(ctx context.Context, subscriber string, registeredAt time.Time, subscriptions []Subscription) (bool, error)
	// Enqueue enqueues jobs outside any caller's transaction.
	Enqueue(ctx context.Context, preparedJobs ...*jobs.PreparedJob) error
	// Accept stores the acceptance's key and enqueues its job in one transaction, and reports
	// false without enqueuing when the key was stored before (a redelivery).
	Accept(ctx context.Context, acceptance *Acceptance) (bool, error)
	// Delivery loads a delivery with its subscription; ErrNotFound when the delivery is gone, and
	// OutboundDelivery.Obsolete set when the subscription is.
	Delivery(ctx context.Context, key DeliveryKey) (*OutboundDelivery, error)
	// RecordDelivery stores the result of one attempt and updates the subscription's failure count.
	RecordDelivery(ctx context.Context, record *DeliveryRecord) (*RecordedDelivery, error)
	// Sweep deletes ledger and inbox rows past their retention and returns how many.
	Sweep(ctx context.Context, request *SweepRequest) (int, error)
}

// Job prepares the job of a local subscription's handler for event: kind
// events.<subscriber>.<name> on the subscription's Queue. Publish enqueues it for a local
// subscription, the inbox for a remote one.
func (s *Subscription) Job(event *Event) *jobs.PreparedJob {
	kind := jobs.NewKind[Event](subscriptionKind(s.Subscriber, s.Name), &jobs.KindOptions{Queue: s.Queue, MaximumAttempts: s.MaximumAttempts})
	return kind.New(*event, nil)
}

// deliveryPayload is the outbound job's payload: the delivery and the generation the job sends.
type deliveryPayload struct {
	DeliveryKey
	Generation int `json:"generation"`
}

// DeliveryJob prepares the outbound job of one generation of a delivery: Publish enqueues
// generation 0, Redeliver the next one. A job of an older generation sends nothing.
func DeliveryJob(key DeliveryKey, generation int) *jobs.PreparedJob {
	uniqueKey := fmt.Sprintf("%s/%s/%s/%d", key.Subscriber, key.Subscription, key.EventID, generation)
	return deliverKind.New(deliveryPayload{DeliveryKey: key, Generation: generation}, &jobs.EnqueueOptions{UniqueKey: uniqueKey})
}
