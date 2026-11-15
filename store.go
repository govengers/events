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

// Sealer encrypts subscription secrets at rest. The method set matches the seal package of
// github.com/govengers/auth, so its key ring can be passed in directly. associatedData binds a
// sealed value to its row: Open fails for a value moved to another subscription.
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
	Key      DeliveryKey
	Topic    string
	Body     []byte // the encoded Event
	Endpoint string
	Secret   string // opened
	Status   DeliveryStatus
	Attempts int  // attempts recorded so far
	Disabled bool // the subscription is disabled: nothing is sent
}

// DeliveryRecord is the result of one delivery attempt for the ledger.
type DeliveryRecord struct {
	Key        DeliveryKey
	Status     DeliveryStatus
	StatusCode int    // the subscriber's HTTP status; zero for a transport error or no request
	Error      string // empty when delivered
	// DisableAfter disables the subscription once this many deliveries in a row failed; a
	// delivered one resets the count. Only a transition to failed counts.
	DisableAfter int
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
	Job          *jobs.PreparedJob
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
	// Enqueue enqueues jobs outside any caller's transaction.
	Enqueue(ctx context.Context, preparedJobs ...*jobs.PreparedJob) error
	// Accept stores the acceptance's key and enqueues its job in one transaction, and reports
	// false without enqueuing when the key was stored before (a redelivery).
	Accept(ctx context.Context, acceptance *Acceptance) (bool, error)
	// Delivery loads a delivery with its subscription; ErrNotFound when either is gone.
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

// DeliveryJob prepares the outbound job of one delivery. Its unique key coalesces a redelivery
// with a pending job of the same delivery.
func DeliveryJob(key DeliveryKey) *jobs.PreparedJob {
	return deliverKind.New(key, &jobs.EnqueueOptions{UniqueKey: key.Subscriber + "/" + key.Subscription + "/" + key.EventID})
}
