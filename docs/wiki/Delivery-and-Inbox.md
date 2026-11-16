# Delivery and inbox

## The ledger

Every remote delivery is a row of `govengers_event_deliveries`, keyed by subscriber, subscription and event id, holding the encoded event (the exact bytes Publish checked, as `bytea`), its status (`pending`, `delivered`, `failed`), attempts, a generation, the last HTTP status and error. The outbound job (`govengers.events.deliver` on `events.Queue`) carries only the key and the generation.

Only a pending row of the job's generation is sent. `Redeliver` counts the generation up and enqueues that generation's job, so an older job still waiting for its backoff sends and records nothing when it runs, and a job that runs again after its record sends nothing. Only a run that dies between the POST and the record sends twice, and a `Redeliver` while an attempt is in flight sends the new generation alongside it; the inbox absorbs both. Only the transition from pending to failed counts towards the auto-disable.

A delivery whose subscription was removed, made local or subscribes another topic now is recorded failed without a request and without counting (`last_error` says why), so the sweep removes it. So is a delivery that still cannot be loaded at its last attempt (a sealer key gone).

## Outbound attempts

| Subscriber answers | Result |
|--------------------|--------|
| 2xx | delivered |
| 401, 403 (an inbox key rotation or a rollout), 404 (mid-rollout), 408, 5xx, transport error | retried with the jobs backoff (1 s doubling to 1 h), 30 attempts, about 19 hours |
| 429 with `Retry-After: <seconds>` | snoozed that long (at most 1 h); the ledger counts the attempt |
| any other status | failed at once |
| address refused by safehttp, a redirect past `MaximumRedirects` | failed at once |

The request is `POST` with `Content-Type: application/cloudevents+json` and the Standard Webhooks headers. The response body is read up to 64 KiB and dropped.

## Auto-disable

After `Options.DisableAfter` (default 5) deliveries of a subscription in a row ended failed, the subscription is disabled (`disabled_by = failures`); a delivered one resets the count. Disabled subscriptions are skipped by Publish and their pending deliveries are recorded as failed without a request. The subscriber's next registration enables a subscription the failures disabled; one an administrator disabled stays disabled until `EnableSubscription`. Failed deliveries are sent again with `Redeliver`.

## The inbox

`bus.InboxHandler()` serves `POST {InboxURL}/{subscription}`; the subscription is the last path segment.

1. Unknown or local subscription: 404 (the publisher retries).
2. Body above 32 KiB (`MaximumEventBytes`): 413.
3. Signature checked on the raw body with the subscription's derived secret: 401 when it fails or the timestamp is more than 5 minutes off.
4. Not a CloudEvent 1.0 of the subscription's topic: 400.
5. One READ COMMITTED transaction: insert the key into `govengers_event_inbox`; only when it is new, enqueue the handler job. 202 either way. The insert casts the event to jsonb first: an event jsonb cannot store (NUL character, lone surrogate, number outside numeric's range) answers 400, so the sender fails the delivery at once.

Inbox keys are kept 7 days, far longer than the sender retries.
