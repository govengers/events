package events

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/govengers/events/safehttp"
	"github.com/govengers/jobs"
)

// maximumRetryAfter caps how long a subscriber's Retry-After may delay the next attempt: the
// backoff's own cap (E6).
const maximumRetryAfter = time.Hour

// discardedResponseBytes is how much of a response body is read to reuse the connection.
const discardedResponseBytes = 64 << 10

// errSubscriptionDisabled is the recorded error of a delivery to a disabled subscription.
var errSubscriptionDisabled = errors.New("subscription disabled")

// deliver is the handler of the outbound delivery job (E6): it POSTs the delivery's event to the
// subscriber's inbox, signed per Standard Webhooks, and records the attempt in the ledger. Only a
// pending delivery of the job's generation is sent, so a job that runs again after its delivery
// was recorded, or an older job after a Redeliver, sends nothing; a job that runs twice sends
// twice only when the first run died between the POST and the record.
func (b *Bus) deliver(ctx context.Context, job *jobs.Job[deliveryPayload]) error {
	started := time.Now()
	attempt := &DeliveryAttempt{Key: job.Payload.DeliveryKey}
	defer func() {
		attempt.Duration = time.Since(started)
		b.observer.DeliveryFinished(attempt)
	}()
	delivery, err := b.store.Delivery(ctx, job.Payload.DeliveryKey)
	switch {
	case errors.Is(err, ErrNotFound):
		attempt.Result = DeliveryResultDropped
		return nil
	case err != nil && job.Attempt >= job.MaximumAttempts:
		// the last attempt: without a record the delivery would stay pending, unswept, for good
		attempt.Result, attempt.Err = DeliveryResultFailed, err
		_, recordErr := b.store.RecordDelivery(ctx, &DeliveryRecord{
			Key: job.Payload.DeliveryKey, Generation: job.Payload.Generation, Status: DeliveryFailed, Error: err.Error(), Uncounted: true,
		})
		return jobs.Permanent(errors.Join(err, recordErr))
	case err != nil:
		attempt.Result, attempt.Err = DeliveryResultRetried, err
		return err
	}
	attempt.Topic = delivery.Topic
	switch {
	case delivery.Generation != job.Payload.Generation || delivery.Status != DeliveryPending:
		attempt.Result = DeliveryResultDropped
		return nil
	case delivery.Disabled || delivery.Obsolete != "":
		reason := delivery.Obsolete
		if reason == "" {
			reason = errSubscriptionDisabled.Error()
		}
		attempt.Result = DeliveryResultSkipped
		_, err := b.store.RecordDelivery(ctx, &DeliveryRecord{
			Key: delivery.Key, Generation: delivery.Generation, Status: DeliveryFailed, Error: reason, Uncounted: true,
		})
		attempt.Err = err
		return err
	}
	response := b.send(ctx, delivery)
	final := response.permanent || max(job.Attempt, delivery.Attempts+1) >= job.MaximumAttempts
	return b.record(ctx, delivery, response, final, attempt)
}

// sendResult is the subscriber's answer to one POST.
type sendResult struct {
	statusCode int
	err        error         // nil when delivered
	permanent  bool          // a retry cannot succeed
	retryAfter time.Duration // positive when the subscriber asked for a delay (429)
}

func (b *Bus) send(ctx context.Context, delivery *OutboundDelivery) sendResult {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.Endpoint, bytes.NewReader(delivery.Body))
	if err != nil {
		return sendResult{err: fmt.Errorf("build the request: %w", err), permanent: true}
	}
	request.Header.Set("Content-Type", ContentType)
	if err := Sign(request.Header, delivery.Secret, delivery.Key.EventID, b.now(), delivery.Body); err != nil {
		return sendResult{err: fmt.Errorf("sign: %w", err), permanent: true}
	}
	response, err := b.client.Do(request)
	if err != nil {
		// A refused address and a redirect stay what they are: retrying for a day only hides the
		// misconfiguration.
		permanent := errors.Is(err, safehttp.ErrBlockedAddress) || errors.Is(err, safehttp.ErrTooManyRedirects)
		return sendResult{err: fmt.Errorf("post to the subscriber: %w", err), permanent: permanent}
	}
	defer func() { _ = response.Body.Close() }() // the answer's body carries nothing we keep
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, discardedResponseBytes))
	return classify(response)
}

// classify maps the subscriber's status to E6's outcome: 2xx delivered; 401 and 403 (an inbox
// key rotation or a rollout: the secret reaches the publisher with the next registration), 404
// (mid-rollout), 408, 429 and 5xx retry; every other status is permanent.
func classify(response *http.Response) sendResult {
	status := response.StatusCode
	result := sendResult{statusCode: status}
	switch {
	case status >= 200 && status < 300:
		return result
	case status == http.StatusTooManyRequests:
		result.retryAfter = retryAfter(response.Header.Get("Retry-After"))
	case status == http.StatusUnauthorized, status == http.StatusForbidden, status == http.StatusNotFound,
		status == http.StatusRequestTimeout, status >= 500:
	default:
		result.permanent = true
	}
	result.err = fmt.Errorf("the subscriber answered %d %s", status, http.StatusText(status))
	return result
}

// retryAfter reads a Retry-After header in seconds; zero for none or a date (rare for 429).
func retryAfter(header string) time.Duration {
	seconds, err := strconv.Atoi(header)
	switch {
	case err != nil || seconds <= 0:
		return 0
	case seconds >= int(maximumRetryAfter/time.Second): // before the multiplication, which overflows
		return maximumRetryAfter
	}
	return time.Duration(seconds) * time.Second
}

func (b *Bus) record(ctx context.Context, delivery *OutboundDelivery, response sendResult, final bool, attempt *DeliveryAttempt) error {
	record := &DeliveryRecord{Key: delivery.Key, Generation: delivery.Generation, StatusCode: response.statusCode, DisableAfter: b.disableAfter}
	attempt.StatusCode, attempt.Err = response.statusCode, response.err
	switch {
	case response.err == nil:
		record.Status, attempt.Result = DeliveryDelivered, DeliveryResultDelivered
	case final:
		record.Status, record.Error, attempt.Result = DeliveryFailed, response.err.Error(), DeliveryResultFailed
	default:
		record.Status, record.Error, attempt.Result = DeliveryPending, response.err.Error(), DeliveryResultRetried
	}
	recorded, err := b.store.RecordDelivery(ctx, record)
	if err != nil {
		attempt.Result, attempt.Err = DeliveryResultRetried, errors.Join(response.err, err)
		return fmt.Errorf("record the delivery: %w", err)
	}
	attempt.Disabled = recorded.Disabled
	switch {
	case response.err == nil:
		return nil
	case final:
		return jobs.Permanent(response.err)
	case response.retryAfter > 0:
		attempt.Result = DeliveryResultSnoozed
		return jobs.Snooze(response.retryAfter)
	default:
		return response.err
	}
}
