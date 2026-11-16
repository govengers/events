package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/govengers/events"
	"github.com/govengers/jobs"
)

const selectActiveSubscriptions = `
SELECT subscriber, name, COALESCE(endpoint, ''), COALESCE(queue, ''), COALESCE(maximum_attempts, 0)
FROM govengers_event_subscriptions
WHERE topic = $1 AND disabled_at IS NULL
ORDER BY subscriber, name`

const insertDelivery = `
INSERT INTO govengers_event_deliveries (subscriber, subscription, event_id, topic, event)
VALUES ($1, $2, $3, $4, $5)`

// Publish publishes the events with executor, usually the caller's transaction, so they commit
// or roll back with the change they describe (E4). For each event it reads the topic's enabled
// subscriptions and, for a local one, enqueues the subscriber's handler job; for a remote one it
// records the delivery in the ledger and enqueues its outbound job. A topic without
// subscriptions stores nothing. It returns the events' IDs in order.
//
// Every event is checked and encoded before the first write: events.ErrInvalidTopic, a value a
// propagator cannot carry, data that does not encode, events.ErrNulCharacter and
// events.ErrEventTooLarge write nothing. JSON a payload type passes through unchecked
// (json.RawMessage, json.Number) must be storable as jsonb: a lone surrogate escape or a number
// outside numeric's range fails the job insert of a local subscription, which aborts the
// caller's transaction, and a remote subscriber's inbox refuses it (400, the delivery fails).
// A database error after some writes returns the IDs published so far with the error; pass a
// transaction for all or nothing.
func (s *Store) Publish(ctx context.Context, executor Executor, preparedEvents ...*events.PreparedEvent) ([]string, error) {
	if s.source == "" {
		return nil, errors.New("postgresadapter: Publish needs Options.Source")
	}
	envelopes, err := s.envelopes(ctx, preparedEvents)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(envelopes))
	subscriptionsByTopic := map[string][]events.Subscription{}
	for _, envelope := range envelopes {
		publication := &events.Publication{Topic: envelope.event.Type}
		publication.Err = s.publishOne(ctx, executor, envelope, subscriptionsByTopic, publication)
		s.observer.Published(publication)
		if publication.Err != nil {
			return ids, publication.Err
		}
		ids = append(ids, envelope.event.ID)
	}
	return ids, nil
}

// envelope is one event ready to publish.
type envelope struct {
	event   *events.Event
	encoded []byte
}

func (s *Store) envelopes(ctx context.Context, preparedEvents []*events.PreparedEvent) ([]envelope, error) {
	headers := map[string]string{}
	for _, propagator := range s.propagators {
		if err := propagator.Inject(ctx, headers); err != nil {
			return nil, fmt.Errorf("publish: carry the trace context: %w", err)
		}
	}
	actor := ""
	if s.actor != nil {
		actor = s.actor(ctx)
	}
	envelopes := make([]envelope, 0, len(preparedEvents))
	for _, prepared := range preparedEvents {
		data, err := prepared.EncodeData()
		if err != nil {
			return nil, err
		}
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("publish %s: event id: %w", prepared.Topic(), err)
		}
		event := &events.Event{
			SpecVersion: events.SpecVersion, ID: id.String(), Source: s.source, Type: prepared.Topic(),
			Time: time.Now().UTC(), DataContentType: "application/json", Data: data,
			TraceParent: headers["traceparent"], TraceState: headers["tracestate"], Actor: actor,
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("publish %s: encode the event: %w", prepared.Topic(), err)
		}
		if len(encoded) > events.MaximumEventBytes {
			return nil, fmt.Errorf("%w: %s is %d bytes", events.ErrEventTooLarge, prepared.Topic(), len(encoded))
		}
		envelopes = append(envelopes, envelope{event: event, encoded: encoded})
	}
	return envelopes, nil
}

func (s *Store) publishOne(ctx context.Context, executor Executor, envelope envelope, cache map[string][]events.Subscription, publication *events.Publication) error {
	topic := envelope.event.Type
	subscriptions, cached := cache[topic]
	if !cached {
		var err error
		if subscriptions, err = activeSubscriptions(ctx, executor, topic); err != nil {
			return err
		}
		cache[topic] = subscriptions
	}
	preparedJobs := make([]*jobs.PreparedJob, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		if !subscription.Remote() {
			preparedJobs = append(preparedJobs, subscription.Job(envelope.event))
			publication.Local++
			continue
		}
		key := events.DeliveryKey{Subscriber: subscription.Subscriber, Subscription: subscription.Name, EventID: envelope.event.ID}
		if _, err := executor.Exec(ctx, insertDelivery, key.Subscriber, key.Subscription, key.EventID, topic, envelope.encoded); err != nil {
			return wrapDriverError(err, "publish %s: record the delivery to %s/%s", topic, key.Subscriber, key.Subscription)
		}
		preparedJobs = append(preparedJobs, events.DeliveryJob(key, 0))
		publication.Remote++
	}
	if _, err := s.jobs.Enqueue(ctx, executor, preparedJobs...); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	return nil
}

func activeSubscriptions(ctx context.Context, executor Executor, topic string) ([]events.Subscription, error) {
	rows, err := executor.Query(ctx, selectActiveSubscriptions, topic)
	if err != nil {
		return nil, wrapDriverError(err, "publish %s: read the subscriptions", topic)
	}
	subscriptions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (events.Subscription, error) {
		subscription := events.Subscription{Topic: topic}
		err := row.Scan(&subscription.Subscriber, &subscription.Name, &subscription.Endpoint, &subscription.Queue, &subscription.MaximumAttempts)
		return subscription, err
	})
	if err != nil {
		return nil, wrapDriverError(err, "publish %s: read the subscriptions", topic)
	}
	return subscriptions, nil
}
