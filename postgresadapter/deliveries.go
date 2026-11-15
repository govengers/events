package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/govengers/events"
)

const selectOutboundDelivery = `
SELECT delivery.topic, delivery.event::text, delivery.status, delivery.attempts,
       subscription.endpoint, subscription.sealed_secret, subscription.disabled_at IS NOT NULL
FROM govengers_event_deliveries delivery
JOIN govengers_event_subscriptions subscription
  ON subscription.subscriber = delivery.subscriber AND subscription.name = delivery.subscription
WHERE delivery.subscriber = $1 AND delivery.subscription = $2 AND delivery.event_id = $3
  AND subscription.endpoint IS NOT NULL`

// Delivery loads a delivery with its subscription's endpoint and opened secret. It returns
// events.ErrNotFound when the delivery was swept or its subscription removed or made local.
func (s *Store) Delivery(ctx context.Context, key events.DeliveryKey) (*events.OutboundDelivery, error) {
	delivery := &events.OutboundDelivery{Key: key}
	var body string
	var sealed []byte
	err := s.pool.QueryRow(ctx, selectOutboundDelivery, key.Subscriber, key.Subscription, key.EventID).Scan(
		&delivery.Topic, &body, &delivery.Status, &delivery.Attempts, &delivery.Endpoint, &sealed, &delivery.Disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("delivery %s: %w", describe(key), events.ErrNotFound)
	}
	if err != nil {
		return nil, wrapDriverError(err, "load delivery %s", describe(key))
	}
	if s.sealer == nil {
		return nil, ErrNoSealer
	}
	secret, err := s.sealer.Open(sealed, associatedData(key.Subscriber, key.Subscription))
	if err != nil {
		return nil, fmt.Errorf("open the secret of %s/%s: %w", key.Subscriber, key.Subscription, err)
	}
	delivery.Body, delivery.Secret = []byte(body), string(secret)
	return delivery, nil
}

// recordAttempt never turns a delivered delivery back: a stale run of the job that failed after
// another run delivered changes nothing.
const recordAttempt = `
UPDATE govengers_event_deliveries
SET status = $4, attempts = attempts + 1, last_status_code = NULLIF($5, 0), last_error = NULLIF($6, ''),
    updated_at = now(), finished_at = CASE WHEN $4 = 'pending' THEN NULL ELSE now() END
WHERE subscriber = $1 AND subscription = $2 AND event_id = $3 AND (status <> 'delivered' OR $4 = 'delivered')
RETURNING attempts`

const resetFailures = `
UPDATE govengers_event_subscriptions SET failed_deliveries = 0, updated_at = now()
WHERE subscriber = $1 AND name = $2 AND failed_deliveries <> 0`

// countFailure counts a failed delivery of an enabled subscription and disables it at the
// threshold; it returns a row only for an enabled subscription, true when this call disabled it.
const countFailure = `
UPDATE govengers_event_subscriptions
SET failed_deliveries = failed_deliveries + 1,
    disabled_at = CASE WHEN $3 > 0 AND failed_deliveries + 1 >= $3 THEN now() END,
    disabled_by = CASE WHEN $3 > 0 AND failed_deliveries + 1 >= $3 THEN 'failures' END,
    updated_at = now()
WHERE subscriber = $1 AND name = $2 AND disabled_at IS NULL
RETURNING disabled_at IS NOT NULL`

// RecordDelivery stores one attempt's result in the ledger and, in the same transaction,
// updates the subscription: a delivered delivery resets its failure count, a failed one counts
// and disables the subscription after record.DisableAfter failures in a row. A delivery that is
// gone, or delivered before while this record is not, records nothing and reports zero attempts.
func (s *Store) RecordDelivery(ctx context.Context, record *events.DeliveryRecord) (*events.RecordedDelivery, error) {
	key := record.Key
	recorded := &events.RecordedDelivery{}
	err := s.inTransaction(ctx, func(transaction pgx.Tx) error {
		err := transaction.QueryRow(ctx, recordAttempt, key.Subscriber, key.Subscription, key.EventID,
			string(record.Status), record.StatusCode, record.Error).Scan(&recorded.Attempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return wrapDriverError(err, "record delivery %s", describe(key))
		}
		switch record.Status {
		case events.DeliveryDelivered:
			_, err = transaction.Exec(ctx, resetFailures, key.Subscriber, key.Subscription)
		case events.DeliveryFailed:
			err = transaction.QueryRow(ctx, countFailure, key.Subscriber, key.Subscription, record.DisableAfter).Scan(&recorded.Disabled)
			if errors.Is(err, pgx.ErrNoRows) {
				err = nil // disabled already, or gone
			}
		case events.DeliveryPending:
		}
		return wrapDriverError(err, "record delivery %s: update the subscription", describe(key))
	})
	if err != nil {
		return nil, err
	}
	return recorded, nil
}

func describe(key events.DeliveryKey) string {
	return key.Subscriber + "/" + key.Subscription + "/" + key.EventID
}

// DeliveryEntry is one delivery of the ledger as the admin view sees it.
type DeliveryEntry struct {
	Key            events.DeliveryKey
	Topic          string
	Event          json.RawMessage
	Status         events.DeliveryStatus
	Attempts       int
	LastStatusCode int // zero without a response
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

// DeliveryFilter selects deliveries, newest event first. Empty fields match every delivery.
type DeliveryFilter struct {
	Subscriber   string
	Subscription string
	Status       events.DeliveryStatus
	// After continues a listing after this key, the last entry of the previous page; nil
	// starts at the newest.
	After *events.DeliveryKey
	Limit int // zero means 100, at most 1000
}

const selectDeliveries = `
SELECT subscriber, subscription, event_id, topic, event, status, attempts, COALESCE(last_status_code, 0),
       COALESCE(last_error, ''), created_at, updated_at, finished_at
FROM govengers_event_deliveries
WHERE ($1 = '' OR subscriber = $1) AND ($2 = '' OR subscription = $2) AND ($3 = '' OR status = $3)
  AND ($4 = '' OR (event_id, subscriber, subscription) < ($4, $5, $6))
ORDER BY event_id DESC, subscriber DESC, subscription DESC
LIMIT $7`

// Deliveries lists ledger entries, newest event first (event IDs are UUIDv7); a nil filter
// lists the newest 100. Continue a listing with After set to the last entry's Key.
func (s *Store) Deliveries(ctx context.Context, filter *DeliveryFilter) ([]*DeliveryEntry, error) {
	if filter == nil {
		filter = &DeliveryFilter{}
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)
	after := events.DeliveryKey{}
	if filter.After != nil {
		after = *filter.After
	}
	rows, err := s.pool.Query(ctx, selectDeliveries, filter.Subscriber, filter.Subscription, string(filter.Status),
		after.EventID, after.Subscriber, after.Subscription, limit)
	if err != nil {
		return nil, wrapDriverError(err, "list deliveries")
	}
	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*DeliveryEntry, error) {
		var entry DeliveryEntry
		err := row.Scan(&entry.Key.Subscriber, &entry.Key.Subscription, &entry.Key.EventID, &entry.Topic, &entry.Event,
			&entry.Status, &entry.Attempts, &entry.LastStatusCode, &entry.LastError, &entry.CreatedAt, &entry.UpdatedAt, &entry.FinishedAt)
		return &entry, err
	})
	return entries, wrapDriverError(err, "list deliveries")
}

const resetDelivery = `
UPDATE govengers_event_deliveries
SET status = 'pending', attempts = 0, last_status_code = NULL, last_error = NULL, updated_at = now(), finished_at = NULL
WHERE subscriber = $1 AND subscription = $2 AND event_id = $3`

// Redeliver sends a delivery again, whatever its status: it resets the ledger entry to pending
// with no attempts and enqueues its outbound job, in one transaction. A pending outbound job of
// the same delivery is reused instead of a second one. It returns events.ErrNotFound for an
// unknown delivery. A delivery to a disabled subscription fails again until it is enabled.
func (s *Store) Redeliver(ctx context.Context, key events.DeliveryKey) error {
	return s.inTransaction(ctx, func(transaction pgx.Tx) error {
		tag, err := transaction.Exec(ctx, resetDelivery, key.Subscriber, key.Subscription, key.EventID)
		if err != nil {
			return wrapDriverError(err, "redeliver %s", describe(key))
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("redeliver %s: %w", describe(key), events.ErrNotFound)
		}
		if _, err := s.jobs.Enqueue(ctx, transaction, events.DeliveryJob(key)); err != nil {
			return fmt.Errorf("redeliver %s: %w", describe(key), err)
		}
		return nil
	})
}
