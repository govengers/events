# Operations

## Admin operations (`postgresadapter.Store`)

| Operation | Use |
|-----------|-----|
| `Subscriptions(ctx, &SubscriptionFilter{Subscriber, Topic})` | list subscriptions with failure count and disabled state, never secrets |
| `DisableSubscription`, `EnableSubscription` | stop and resume deliveries; enable resets the failure count |
| `Deliveries(ctx, &DeliveryFilter{Subscriber, Subscription, Status, After, Limit})` | the ledger, newest event first; continue with `After` = the last entry's `Key` |
| `Redeliver(ctx, key)` | send a delivery again whatever its status: attempts reset, job enqueued in the same transaction |
| `PutSubscriptions(ctx, subscriber, set)` | replace a subscriber's set; an application creates webhooks for external receivers with it |

The library ships no UI; an application builds its admin view on these, usually as livewire reads and mutations.

## Webhooks an application creates

```go
secret := events.NewSecret() // show it once
err := eventStore.PutSubscriptions(ctx, "webhooks."+ownerID, []events.Subscription{{
    Name: hookID, Topic: "tasks.task-moved.v1", Endpoint: userURL, Secret: secret,
}})
```

Validate the URL with `events.ValidateEndpoint` first, deliver with a safehttp client that refuses private networks, and give each owner one subscriber name so their set is replaced as a whole.

## Retention

Add `events.SweepSchedule()` to the worker's schedules: hourly it deletes delivered deliveries older than 7 days, failed ones older than 30 days and inbox keys older than 7 days, 1000 rows per table and run. Pending deliveries are never swept.

## Metrics

`prometheusadapter.New(&Options{Registerer, Namespace})` implements `events.Observer`; pass it to `events.Options.Observer` and `postgresadapter.Options.Observer`:

```
<namespace>_events_published_total{topic,outcome}
<namespace>_events_deliveries_total{topic,result}        result: delivered, retried, snoozed, failed, dropped, skipped
<namespace>_events_delivery_duration_seconds{topic}
<namespace>_events_subscriptions_disabled_total
<namespace>_events_received_total{subscription,result}   result: accepted, duplicate, unknown_subscription, invalid_signature, invalid_event, too_large, failed
<namespace>_events_registrations_total{direction,outcome}
```

The jobs behind them report through jobs' own `prometheusadapter` (queue `govengers.events`, kinds `govengers.events.deliver`, `events.<service>.<subscription>`). Alert on a growing `govengers.events` backlog, on `subscriptions_disabled_total` and on `received_total{result="invalid_signature"}`.

## Tracing

Give `jobs/oteladapter.NewPropagator()` to `postgresadapter.Options.Propagators` and `events.Options.Propagators` (and, as always, to the jobs store and worker): the trace travels from the publishing request through the delivery job, the HTTP request and the inbox into the handler job. Wrap `Options.HTTPClient`'s transport with otelhttp for a client span per delivery.
