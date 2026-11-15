package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/govengers/events"
)

const insertInbox = `
INSERT INTO govengers_event_inbox (subscriber, subscription, event_id) VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
RETURNING true`

// Accept stores the delivery's key in the inbox and enqueues its job in one READ COMMITTED
// transaction (E7); a key stored before reports false and enqueues nothing. Two concurrent
// requests of one delivery enqueue once: the second waits for the first's key and then finds it.
func (s *Store) Accept(ctx context.Context, acceptance *events.Acceptance) (bool, error) {
	accepted := false
	err := s.inTransaction(ctx, func(transaction pgx.Tx) error {
		var inserted bool
		err := transaction.QueryRow(ctx, insertInbox, acceptance.Subscriber, acceptance.Subscription, acceptance.EventID).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return wrapDriverError(err, "accept %s/%s/%s", acceptance.Subscriber, acceptance.Subscription, acceptance.EventID)
		}
		if _, err := s.jobs.Enqueue(ctx, transaction, acceptance.Job); err != nil {
			return fmt.Errorf("accept %s/%s/%s: %w", acceptance.Subscriber, acceptance.Subscription, acceptance.EventID, err)
		}
		accepted = true
		return nil
	})
	return accepted, err
}

// sweepDeliveries skips the rows a Redeliver holds and repeats the predicate on the deleted row,
// so a delivery that turned pending since the selection stays.
const sweepDeliveries = `
DELETE FROM govengers_event_deliveries
WHERE (subscriber, subscription, event_id) IN (
    SELECT subscriber, subscription, event_id FROM govengers_event_deliveries
    WHERE (status = 'delivered' AND finished_at < now() - make_interval(secs => $1))
       OR (status = 'failed' AND finished_at < now() - make_interval(secs => $2))
    LIMIT $3
    FOR UPDATE SKIP LOCKED)
  AND ((status = 'delivered' AND finished_at < now() - make_interval(secs => $1))
       OR (status = 'failed' AND finished_at < now() - make_interval(secs => $2)))`

const sweepInbox = `
DELETE FROM govengers_event_inbox
WHERE (subscriber, subscription, event_id) IN (
    SELECT subscriber, subscription, event_id FROM govengers_event_inbox
    WHERE received_at < now() - make_interval(secs => $1)
    LIMIT $2)`

// Sweep deletes up to request.Limit delivered and failed ledger entries past their retention and
// up to request.Limit inbox keys past theirs, and returns how many rows it deleted. Pending
// deliveries are kept. A zero retention deletes nothing of its kind, a zero limit nothing at all.
func (s *Store) Sweep(ctx context.Context, request *events.SweepRequest) (int, error) {
	if request.Limit <= 0 {
		return 0, nil
	}
	deleted := 0
	if request.RetainDelivered > 0 || request.RetainFailed > 0 {
		delivered, failed := retention(request.RetainDelivered), retention(request.RetainFailed)
		tag, err := s.pool.Exec(ctx, sweepDeliveries, delivered, failed, request.Limit)
		if err != nil {
			return 0, wrapDriverError(err, "sweep deliveries")
		}
		deleted += int(tag.RowsAffected())
	}
	if request.RetainInbox > 0 {
		tag, err := s.pool.Exec(ctx, sweepInbox, request.RetainInbox.Seconds(), request.Limit)
		if err != nil {
			return deleted, wrapDriverError(err, "sweep the inbox")
		}
		deleted += int(tag.RowsAffected())
	}
	return deleted, nil
}

// retention turns a zero retention into NULL, which keeps that status forever in sweepDeliveries.
func retention(duration time.Duration) *float64 {
	if duration <= 0 {
		return nil
	}
	return new(duration.Seconds())
}
