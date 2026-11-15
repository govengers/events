package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/govengers/events/safehttp"
	"github.com/govengers/jobs"
)

// Queue is the jobs queue of the library's own jobs: outbound deliveries, registrations at
// publishers and the sweep. Every process whose worker should deliver, register or sweep consumes
// it (jobs.WorkerOptions.Queues).
const Queue = "govengers.events"

// DefaultDisableAfter is how many deliveries in a row must fail before a subscription is
// disabled when Options.DisableAfter is zero.
const DefaultDisableAfter = 5

// DeliveryAttempts is the number of attempts of one outbound delivery: with the jobs default
// backoff (doubling from one second, capped at one hour) about 19 hours (E6).
const DeliveryAttempts = 30

// registrationAttempts keeps a registration retrying for about four days at the one-hour cap.
const registrationAttempts = 100

// The library's own job kinds.
var (
	deliverKind  = jobs.NewKind[deliveryPayload]("govengers.events.deliver", &jobs.KindOptions{Queue: Queue, MaximumAttempts: DeliveryAttempts})
	registerKind = jobs.NewKind[registrationPayload]("govengers.events.register", &jobs.KindOptions{Queue: Queue, MaximumAttempts: registrationAttempts})
	sweepKind    = jobs.NewKind[SweepRequest]("govengers.events.sweep", &jobs.KindOptions{Queue: Queue, MaximumAttempts: 3})
)

// Publisher is a remote service this service subscribes to.
type Publisher struct {
	Name string // the publisher's service name, unique among this service's publishers
	// URL is the base of the publisher's events routes: registration is PUT URL/subscribers/{subscriber}.
	URL    string
	APIKey string // this service's API key at the publisher, sent as "Authorization: Bearer"
}

// AuthorizeFunc decides whether the caller of a registration may register subscriber with these
// topics. The request's context carries what the application's authentication middleware put
// there. A non-nil error answers 403.
type AuthorizeFunc func(request *http.Request, subscriber string, topics []string) error

// Options configures a Bus.
type Options struct {
	Name   string       // required: this service's name, the subscriber name at publishers
	Store  Store        // required; postgresadapter.Store
	Worker *jobs.Worker // required; the Bus registers its handlers on it, before Run
	// HTTPClient sends deliveries and registrations; nil is a safehttp client with its defaults,
	// which refuses private networks. Its Timeout bounds one delivery.
	HTTPClient *http.Client
	// InboxKey derives the signing secret of every remote subscription of this service (E8);
	// required for remote subscriptions, at least 32 bytes. Rotating it and calling Register
	// rotates every secret.
	InboxKey []byte
	// InboxURL is the public base URL of this service's inbox handler; a subscription's
	// endpoint is InboxURL/{subscription}. Required for remote subscriptions.
	InboxURL   string
	Publishers []Publisher // the remote services this service subscribes to
	// Authorize guards RegistrationHandler; nil refuses every registration.
	Authorize AuthorizeFunc
	// AllowHTTPEndpoints accepts registrations with http:// endpoints; by default only https.
	AllowHTTPEndpoints bool
	// DisableAfter disables a remote subscription after this many deliveries in a row failed;
	// zero means DefaultDisableAfter. Enable it again with the store's EnableSubscription, or the
	// subscriber's next registration.
	DisableAfter int
	// Propagators restore the trace context of a received event (its traceparent and
	// tracestate extensions) into the context the inbox enqueues the handler's job in; pass the
	// same as postgresadapter.Options.Propagators, e.g. jobs/oteladapter.NewPropagator().
	Propagators []jobs.Propagator
	Observer    Observer         // nil reports nothing
	Now         func() time.Time // the clock of signature timestamps, signing and verifying; nil is time.Now
}

// minimumInboxKeyBytes keeps derived secrets as strong as their HMAC.
const minimumInboxKeyBytes = 32

// Bus connects this service to the event traffic between services: it delivers the remote
// deliveries Publish recorded, receives deliveries in its inbox, registers this service's
// subscriptions at publishers and accepts registrations of subscribers. It is safe for
// concurrent use; Subscribe before the worker runs.
type Bus struct {
	name               string
	store              Store
	worker             *jobs.Worker
	client             *http.Client
	inboxKey           []byte
	inboxURL           string
	publishers         map[string]Publisher
	authorize          AuthorizeFunc
	allowHTTPEndpoints bool
	disableAfter       int
	propagators        []jobs.Propagator
	observer           Observer
	now                func() time.Time

	mutex         sync.RWMutex
	subscriptions map[string]*subscription // by name
}

// subscription is one of this service's own subscriptions, as Subscribe declared it.
type subscription struct {
	name            string
	topic           string
	publisher       string // empty: local
	queue           string
	maximumAttempts int
}

// New validates options, registers the Bus's job handlers on options.Worker and returns the Bus.
func New(options *Options) (*Bus, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	publishers, err := publishersByName(options.Publishers)
	if err != nil {
		return nil, err
	}
	client := options.HTTPClient
	if client == nil {
		client, err = safehttp.New(nil)
		if err != nil {
			return nil, err
		}
	}
	bus := &Bus{
		name: options.Name, store: options.Store, worker: options.Worker, client: client,
		inboxKey: append([]byte(nil), options.InboxKey...), inboxURL: strings.TrimSuffix(options.InboxURL, "/"),
		publishers: publishers, authorize: options.Authorize, allowHTTPEndpoints: options.AllowHTTPEndpoints,
		disableAfter: orDefault(options.DisableAfter, DefaultDisableAfter),
		propagators:  append([]jobs.Propagator(nil), options.Propagators...),
		observer:     GuardObserver(options.Observer), now: options.Now,
		subscriptions: map[string]*subscription{},
	}
	if bus.now == nil {
		bus.now = time.Now
	}
	if err := bus.registerHandlers(); err != nil {
		return nil, err
	}
	return bus, nil
}

func validateOptions(options *Options) error {
	if options == nil || options.Store == nil || options.Worker == nil {
		return errors.New("events: Options.Store and Options.Worker are required")
	}
	if err := ValidateName(options.Name); err != nil {
		return fmt.Errorf("events: Options.Name: %w", err)
	}
	if options.DisableAfter < 0 {
		return errors.New("events: negative Options.DisableAfter")
	}
	for index, propagator := range options.Propagators {
		if propagator == nil {
			return fmt.Errorf("events: Options.Propagators[%d] is nil", index)
		}
	}
	return nil
}

func publishersByName(publishers []Publisher) (map[string]Publisher, error) {
	byName := make(map[string]Publisher, len(publishers))
	for _, publisher := range publishers {
		if err := ValidateName(publisher.Name); err != nil {
			return nil, fmt.Errorf("events: publisher: %w", err)
		}
		if _, exists := byName[publisher.Name]; exists {
			return nil, fmt.Errorf("events: publisher %s is declared twice", publisher.Name)
		}
		if _, err := url.ParseRequestURI(publisher.URL); err != nil {
			return nil, fmt.Errorf("events: publisher %s: URL: %w", publisher.Name, err)
		}
		byName[publisher.Name] = publisher
	}
	return byName, nil
}

func (b *Bus) registerHandlers() error {
	if err := jobs.Handle(b.worker, deliverKind, b.deliver); err != nil {
		return fmt.Errorf("events: register the delivery handler: %w", err)
	}
	if err := jobs.Handle(b.worker, registerKind, b.register); err != nil {
		return fmt.Errorf("events: register the registration handler: %w", err)
	}
	if err := jobs.Handle(b.worker, sweepKind, b.sweep); err != nil {
		return fmt.Errorf("events: register the sweep handler: %w", err)
	}
	return nil
}

// SubscriberQueue is the default queue of a subscriber's handler jobs: events.<subscriber>.
func SubscriberQueue(subscriber string) string { return "events." + subscriber }

// subscriptionKind names the job kind of one subscription's handler.
func subscriptionKind(subscriber, name string) string { return "events." + subscriber + "." + name }

// SubscribeOptions configures one subscription.
type SubscribeOptions struct {
	Name string // required, unique in this service: lower-case words joined by dots or hyphens
	// Publisher is the name of one of Options.Publishers: the subscription is remote, delivered
	// over HTTP to the inbox. Empty means local: the publisher shares this service's database and
	// Publish enqueues the handler's job directly, in the publishing transaction.
	Publisher string
	// Queue is the handler's jobs queue; empty means SubscriberQueue(Options.Name). The worker
	// must consume it.
	Queue           string
	MaximumAttempts int          // the handler's attempts; zero means jobs.DefaultMaximumAttempts
	Backoff         jobs.Backoff // nil means jobs.DefaultBackoff
}

// Subscribe declares a subscription to topic and registers handler for its deliveries on the
// Bus's worker. The handler is a jobs handler: retries, jobs.Permanent and jobs.Snooze, wide
// events and metrics come from jobs. Delivery is at least once and unordered (E10), so the
// handler is idempotent and drops stale updates by the data's version. Data that does not decode
// into P is a permanent failure. Call Register after the last Subscribe.
func Subscribe[P any](bus *Bus, topic *Topic[P], handler func(ctx context.Context, message *Message[P]) error, options *SubscribeOptions) error {
	if bus == nil || topic == nil || handler == nil || options == nil {
		return errors.New("events: Subscribe needs a bus, a topic, a handler and options")
	}
	declared, err := bus.declare(topic.Name(), options)
	if err != nil {
		return err
	}
	kind := jobs.NewKind[Event](subscriptionKind(bus.name, declared.name), &jobs.KindOptions{
		Queue: declared.queue, MaximumAttempts: declared.maximumAttempts, Backoff: options.Backoff,
	})
	err = jobs.Handle(bus.worker, kind, func(ctx context.Context, job *jobs.Job[Event]) error {
		message, err := decodeMessage[P](&job.Payload, declared.topic)
		if err != nil {
			return jobs.Permanent(err)
		}
		message.Attempt = job.Attempt
		return handler(ctx, message)
	})
	if err != nil {
		bus.forget(declared.name)
		return fmt.Errorf("events: subscription %s: %w", declared.name, err)
	}
	return nil
}

func (b *Bus) declare(topic string, options *SubscribeOptions) (*subscription, error) {
	if err := ValidateTopic(topic); err != nil {
		return nil, err
	}
	if err := ValidateName(options.Name); err != nil {
		return nil, fmt.Errorf("events: subscription: %w", err)
	}
	if options.MaximumAttempts < 0 {
		return nil, fmt.Errorf("events: subscription %s: negative MaximumAttempts", options.Name)
	}
	if options.Publisher != "" {
		if _, known := b.publishers[options.Publisher]; !known {
			return nil, fmt.Errorf("events: subscription %s: unknown publisher %s", options.Name, options.Publisher)
		}
		if len(b.inboxKey) < minimumInboxKeyBytes || b.inboxURL == "" {
			return nil, fmt.Errorf("events: subscription %s: a remote subscription needs Options.InboxKey (32 bytes or more) and Options.InboxURL", options.Name)
		}
	}
	declared := &subscription{
		name: options.Name, topic: topic, publisher: options.Publisher,
		queue:           options.Queue,
		maximumAttempts: orDefault(options.MaximumAttempts, jobs.DefaultMaximumAttempts),
	}
	if declared.queue == "" {
		declared.queue = SubscriberQueue(b.name)
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, exists := b.subscriptions[declared.name]; exists {
		return nil, fmt.Errorf("events: subscription %s is declared twice", declared.name)
	}
	b.subscriptions[declared.name] = declared
	return declared, nil
}

func (b *Bus) forget(name string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	delete(b.subscriptions, name)
}

func (b *Bus) subscription(name string) (*subscription, bool) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	declared, found := b.subscriptions[name]
	return declared, found
}

func decodeMessage[P any](event *Event, topic string) (*Message[P], error) {
	if event.Type != topic {
		return nil, fmt.Errorf("event %s has type %s, the subscription's topic is %s", event.ID, event.Type, topic)
	}
	message := &Message[P]{ID: event.ID, Source: event.Source, Topic: event.Type, Time: event.Time, Actor: event.Actor}
	if err := json.Unmarshal(event.Data, &message.Data); err != nil {
		return nil, fmt.Errorf("decode data of event %s: %w", event.ID, err)
	}
	return message, nil
}

// Register writes this service's local subscriptions into its own database and enqueues one
// registration job per publisher, which sends that publisher the complete set of this
// service's subscriptions to it (E5) and retries until the publisher answers. Call it at
// startup after the last Subscribe; it returns once the jobs are enqueued, so the service starts
// while a publisher is down. A declared publisher without subscriptions gets the empty set,
// which removes earlier ones.
func (b *Bus) Register(ctx context.Context) error {
	if err := b.store.PutSubscriptions(ctx, b.name, b.localSubscriptions()); err != nil {
		return fmt.Errorf("events: register the local subscriptions: %w", err)
	}
	registrations := make([]*jobs.PreparedJob, 0, len(b.publishers))
	for name := range b.publishers {
		registrations = append(registrations, registerKind.New(registrationPayload{Publisher: name}, &jobs.EnqueueOptions{UniqueKey: name}))
	}
	if err := b.store.Enqueue(ctx, registrations...); err != nil {
		return fmt.Errorf("events: enqueue the registrations: %w", err)
	}
	return nil
}

func (b *Bus) localSubscriptions() []Subscription {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	local := []Subscription{}
	for _, declared := range b.subscriptions {
		if declared.publisher == "" {
			local = append(local, Subscription{
				Subscriber: b.name, Name: declared.name, Topic: declared.topic,
				Queue: declared.queue, MaximumAttempts: declared.maximumAttempts,
			})
		}
	}
	return local
}

// SweepSchedule returns the schedule that deletes delivered deliveries after a week, failed
// ones after a month and inbox keys after a week (E7), every hour. Add it to
// jobs.WorkerOptions.Schedules of the workers that consume Queue.
func SweepSchedule() jobs.Schedule {
	return jobs.Schedule{
		Name: "govengers.events.sweep",
		Next: jobs.Every(time.Hour),
		Job: sweepKind.New(SweepRequest{
			RetainDelivered: 7 * 24 * time.Hour,
			RetainFailed:    30 * 24 * time.Hour,
			RetainInbox:     7 * 24 * time.Hour,
			Limit:           1000,
		}, nil),
	}
}

func (b *Bus) sweep(ctx context.Context, job *jobs.Job[SweepRequest]) error {
	_, err := b.store.Sweep(ctx, &job.Payload)
	return err
}

func orDefault[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}
