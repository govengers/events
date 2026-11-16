# Publish and subscribe

## Topics

A topic is the typed contract of one kind of fact, declared once in the publisher's contract package:

```go
var OrderPlacedTopic = events.NewTopic[OrderPlaced]("orders.order-placed.v1")
```

Names are lower-case, dot-separated and end in the major version (`ValidateTopic`). Within a version only additive changes: decoding is tolerant, a subscriber ignores fields it does not know. A breaking change publishes `.v2` next to `.v1` until every subscriber moved. Data carries references and a version, not documents: an encoded event is at most 32 KiB (`MaximumEventBytes`): the handler's job carries it, and jobs caps a payload at 256 KiB of jsonb, which takes up to six times the JSON text. Data with a NUL character is refused (`ErrNulCharacter`), jsonb cannot store it. JSON a payload type passes through unchecked (`json.RawMessage`, `json.Number`) must be storable as jsonb too: no lone surrogate escape, no number outside numeric's range; a local subscription fails the job insert and aborts the caller's transaction, a remote subscriber's inbox refuses it.

## Publish

```go
ids, err := eventStore.Publish(ctx, transaction, OrderPlacedTopic.New(OrderPlaced{OrderID: id, Version: version}))
```

Publish reads the topic's enabled subscriptions in the caller's transaction and writes one job (local) or one ledger row plus one job (remote) per subscription. Everything is checked before the first write. Pass the transaction of the change the event describes, `livewire.TransactionFrom(ctx)` inside a livewire mutation. A topic without subscriptions stores nothing; events published before a subscription existed are never delivered.

The envelope is CloudEvents 1.0: `id` (UUIDv7), `source` (`Options.Source`), `type` (the topic), `time`, `data`, and the extensions `traceparent`/`tracestate` (from `Options.Propagators`) and `actor` (from `Options.Actor`, audit only).

## Subscribe

```go
err := events.Subscribe(bus, contract.OrderPlacedTopic, controller.CreateForOrder, &events.SubscribeOptions{
    Name:      "create-invoice", // unique in this service
    Publisher: "orders",         // one of Options.Publishers; empty: local, same database
})
```

The handler is a jobs handler (`func(ctx, *events.Message[P]) error`) of kind `events.<service>.<name>` on queue `events.<service>` (or `SubscribeOptions.Queue`). Return `jobs.Permanent(err)` for a failure no retry fixes and `jobs.Snooze(d)` for a precondition not met yet. Data that does not decode into `P` is permanent.

## Register

`bus.Register(ctx)` at startup, after the last `Subscribe`:

- writes this service's local subscriptions into its own database, replacing the earlier set;
- enqueues one registration job per declared publisher. The job sends `PUT {publisher URL}/subscribers/{service}` with the complete set for that publisher (name, topic, endpoint `InboxURL/{name}`, secret derived from `InboxKey`) and retries until the publisher answers, so the service starts while a publisher is down. It carries the time of the `Register` call; the publisher ignores a registration older than the one it stored (`govengers_event_subscribers`), so an older job that retries after a newer one changes nothing.

A removed `Subscribe` disappears at the publisher with the next registration. Keep a publisher in `Options.Publishers` after removing its last subscription until the service registered once: a declared publisher is sent the empty set.

## The publisher's registration route

`bus.RegistrationHandler()` replaces the subscriber's set in one transaction. Mount it behind the application's authentication; `Options.Authorize(request, registration)` decides whether the caller may register that subscriber with those subscriptions: `registration.Subscriptions` carry name, topic and endpoint, no secret (with govengers/auth's apikey the identity is in the request's context). Without `Authorize` every registration is refused, and a registration naming the publisher itself always is. Endpoints must be https unless `AllowHTTP`.
