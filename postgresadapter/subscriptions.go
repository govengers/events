package postgresadapter

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/govengers/events"
)

// lockSubscriber serializes PutSubscriptions of one subscriber, so two replicas registering at
// once cannot deadlock on each other's row locks or interleave their deletes.
const lockSubscriber = `SELECT pg_advisory_xact_lock(hashtext('govengers_event_subscriptions'), hashtext($1))`

// deleteMissingSubscriptions keeps an administrator's disable: a subscriber that registers
// without the subscription and then with it again gets it back disabled.
const deleteMissingSubscriptions = `
DELETE FROM govengers_event_subscriptions
WHERE subscriber = $1 AND NOT (name = ANY($2)) AND disabled_by IS DISTINCT FROM 'admin'`

// upsertSubscription keeps an existing row's failure state, except that a subscription the
// failures disabled is enabled again: a new registration is the subscriber's sign of life, and a
// changed endpoint or secret is an attempt to fix it. An administrator's disable stays.
const upsertSubscription = `
INSERT INTO govengers_event_subscriptions (subscriber, name, topic, endpoint, sealed_secret, queue, maximum_attempts)
VALUES ($1, $2, $3, NULLIF($4, ''), $5, NULLIF($6, ''), NULLIF($7, 0))
ON CONFLICT (subscriber, name) DO UPDATE SET
    topic = EXCLUDED.topic,
    endpoint = EXCLUDED.endpoint,
    sealed_secret = EXCLUDED.sealed_secret,
    queue = EXCLUDED.queue,
    maximum_attempts = EXCLUDED.maximum_attempts,
    failed_deliveries = CASE WHEN govengers_event_subscriptions.disabled_by = 'failures' THEN 0
                             ELSE govengers_event_subscriptions.failed_deliveries END,
    disabled_at = CASE WHEN govengers_event_subscriptions.disabled_by = 'failures' THEN NULL
                       ELSE govengers_event_subscriptions.disabled_at END,
    disabled_by = CASE WHEN govengers_event_subscriptions.disabled_by = 'failures' THEN NULL
                       ELSE govengers_event_subscriptions.disabled_by END,
    updated_at = now()`

// PutSubscriptions replaces the subscriber's subscriptions with the given set in one
// transaction (E5); a subscription an administrator disabled stays, disabled, when the set no
// longer has it, until EnableSubscription. Every subscription is validated first: names (events.ErrInvalidName), topic
// (events.ErrInvalidTopic), and for a remote one the endpoint (events.ErrInvalidEndpoint, https
// or http) and the secret (events.ErrInvalidSecret); a local one needs a queue and a positive
// MaximumAttempts. Remote secrets are sealed with Options.Sealer (ErrNoSealer without one). An
// application creates its own webhooks for external receivers with it too, one subscriber per
// owner of the webhooks.
func (s *Store) PutSubscriptions(ctx context.Context, subscriber string, subscriptions []events.Subscription) error {
	rows, err := s.subscriptionRows(subscriber, subscriptions)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.name)
	}
	return s.inTransaction(ctx, func(transaction pgx.Tx) error {
		if _, err := transaction.Exec(ctx, lockSubscriber, subscriber); err != nil {
			return wrapDriverError(err, "put subscriptions of %s: lock", subscriber)
		}
		if _, err := transaction.Exec(ctx, deleteMissingSubscriptions, subscriber, names); err != nil {
			return wrapDriverError(err, "put subscriptions of %s: delete the removed ones", subscriber)
		}
		for _, row := range rows {
			_, err := transaction.Exec(ctx, upsertSubscription, subscriber, row.name, row.topic, row.endpoint, row.sealedSecret, row.queue, row.maximumAttempts)
			if err != nil {
				return wrapDriverError(err, "put subscription %s/%s", subscriber, row.name)
			}
		}
		return nil
	})
}

// subscriptionRow is one validated subscription as stored.
type subscriptionRow struct {
	name, topic, endpoint, queue string
	sealedSecret                 []byte
	maximumAttempts              int
}

func (s *Store) subscriptionRows(subscriber string, subscriptions []events.Subscription) ([]subscriptionRow, error) {
	if err := events.ValidateName(subscriber); err != nil {
		return nil, fmt.Errorf("put subscriptions: subscriber: %w", err)
	}
	rows := make([]subscriptionRow, 0, len(subscriptions))
	for index := range subscriptions {
		row, err := s.subscriptionRow(subscriber, &subscriptions[index])
		if err != nil {
			return nil, fmt.Errorf("put subscriptions of %s: %s: %w", subscriber, subscriptions[index].Name, err)
		}
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(left, right subscriptionRow) int { return strings.Compare(left.name, right.name) })
	for index := 1; index < len(rows); index++ {
		if rows[index].name == rows[index-1].name {
			return nil, fmt.Errorf("put subscriptions of %s: %s is listed twice", subscriber, rows[index].name)
		}
	}
	return rows, nil
}

func (s *Store) subscriptionRow(subscriber string, subscription *events.Subscription) (subscriptionRow, error) {
	row := subscriptionRow{name: subscription.Name, topic: subscription.Topic}
	if subscription.Subscriber != "" && subscription.Subscriber != subscriber {
		return row, fmt.Errorf("belongs to subscriber %s", subscription.Subscriber)
	}
	if err := events.ValidateName(subscription.Name); err != nil {
		return row, err
	}
	if err := events.ValidateTopic(subscription.Topic); err != nil {
		return row, err
	}
	if !subscription.Remote() {
		if subscription.Queue == "" || subscription.MaximumAttempts < 1 {
			return row, fmt.Errorf("a local subscription needs a queue and a positive MaximumAttempts")
		}
		row.queue, row.maximumAttempts = subscription.Queue, subscription.MaximumAttempts
		return row, nil
	}
	if err := events.ValidateEndpoint(subscription.Endpoint, true); err != nil {
		return row, err
	}
	if err := events.ValidateSecret(subscription.Secret); err != nil {
		return row, err
	}
	if s.sealer == nil {
		return row, ErrNoSealer
	}
	sealed, err := s.sealer.Seal([]byte(subscription.Secret), associatedData(subscriber, subscription.Name))
	if err != nil {
		return row, fmt.Errorf("seal the secret: %w", err)
	}
	row.endpoint, row.sealedSecret = subscription.Endpoint, sealed
	return row, nil
}

// SubscriptionRecord is a stored subscription as the admin view sees it, without its secret.
type SubscriptionRecord struct {
	Subscriber       string
	Name             string
	Topic            string
	Endpoint         string // empty for a local subscription
	Queue            string // empty for a remote subscription
	FailedDeliveries int    // failed deliveries in a row
	DisabledAt       *time.Time
	DisabledBy       string // "failures" or "admin"; empty while enabled
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// SubscriptionFilter selects subscriptions; empty fields match every subscription.
type SubscriptionFilter struct {
	Subscriber string
	Topic      string
}

const selectSubscriptions = `
SELECT subscriber, name, topic, COALESCE(endpoint, ''), COALESCE(queue, ''), failed_deliveries,
       disabled_at, COALESCE(disabled_by, ''), created_at, updated_at
FROM govengers_event_subscriptions
WHERE ($1 = '' OR subscriber = $1) AND ($2 = '' OR topic = $2)
ORDER BY subscriber, name`

// Subscriptions lists the stored subscriptions; a nil filter lists all.
func (s *Store) Subscriptions(ctx context.Context, filter *SubscriptionFilter) ([]*SubscriptionRecord, error) {
	if filter == nil {
		filter = &SubscriptionFilter{}
	}
	rows, err := s.pool.Query(ctx, selectSubscriptions, filter.Subscriber, filter.Topic)
	if err != nil {
		return nil, wrapDriverError(err, "list subscriptions")
	}
	records, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*SubscriptionRecord, error) {
		var record SubscriptionRecord
		err := row.Scan(&record.Subscriber, &record.Name, &record.Topic, &record.Endpoint, &record.Queue,
			&record.FailedDeliveries, &record.DisabledAt, &record.DisabledBy, &record.CreatedAt, &record.UpdatedAt)
		return &record, err
	})
	return records, wrapDriverError(err, "list subscriptions")
}

const disableSubscription = `
UPDATE govengers_event_subscriptions
SET disabled_at = COALESCE(disabled_at, now()), disabled_by = 'admin', updated_at = now()
WHERE subscriber = $1 AND name = $2`

const enableSubscription = `
UPDATE govengers_event_subscriptions
SET disabled_at = NULL, disabled_by = NULL, failed_deliveries = 0, updated_at = now()
WHERE subscriber = $1 AND name = $2`

// DisableSubscription stops deliveries to a subscription until EnableSubscription: Publish skips
// it, and pending deliveries are recorded as failed without being sent. A registration does not
// enable it again, and one without it does not remove it. It returns events.ErrNotFound for an
// unknown subscription.
func (s *Store) DisableSubscription(ctx context.Context, subscriber, name string) error {
	return s.updateSubscription(ctx, disableSubscription, "disable", subscriber, name)
}

// EnableSubscription enables a subscription disabled by failures or by DisableSubscription and
// resets its failure count (one its subscriber dropped meanwhile goes with the next registration); events published while it was disabled stay undelivered, failed
// deliveries can be sent again with Redeliver. It returns events.ErrNotFound for an unknown
// subscription.
func (s *Store) EnableSubscription(ctx context.Context, subscriber, name string) error {
	return s.updateSubscription(ctx, enableSubscription, "enable", subscriber, name)
}

func (s *Store) updateSubscription(ctx context.Context, statement, operation, subscriber, name string) error {
	tag, err := s.pool.Exec(ctx, statement, subscriber, name)
	if err != nil {
		return wrapDriverError(err, "%s subscription %s/%s", operation, subscriber, name)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s subscription %s/%s: %w", operation, subscriber, name, events.ErrNotFound)
	}
	return nil
}
