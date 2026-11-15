# events

[![CI](https://github.com/govengers/events/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/govengers/events/actions/workflows/ci.yml)
[![unit tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/events/badges/unit-tests.json)](https://github.com/govengers/events/actions/workflows/ci.yml)
[![e2e tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/events/badges/e2e-tests.json)](test/e2e)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/events/badges/coverage.json)](https://github.com/govengers/events/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/govengers/events.svg)](https://pkg.go.dev/github.com/govengers/events)
[![License](https://img.shields.io/github/license/govengers/events)](LICENSE)

Publish and subscribe between govengers services, on top of [govengers/jobs](https://github.com/govengers/jobs). Module `github.com/govengers/events`, Go 1.26, PostgreSQL 18, Apache 2.0. Not released yet (v0).

- **Publish inside your transaction.** The event commits or rolls back with the order it describes. No broker, no outbox poller: the fan-out is a set of jobs in the same transaction.
- **An event arrives as a job.** Handlers are jobs handlers: retries with backoff, `jobs.Permanent`, `jobs.Snooze`, wide events, metrics and traces come from jobs.
- **Local or remote, same code.** A module in the same database gets its job enqueued directly; another service gets a signed HTTP delivery into its inbox. Moving a module into its own service changes wiring, not code.
- **Standards on the wire.** CloudEvents 1.0 envelope, Standard Webhooks signatures (any of its libraries can receive), versioned topic names.
- **Safe by default.** The inbox verifies the signature on the raw body and deduplicates; outbound requests go through an SSRF-safe client (address checked after DNS resolution, metadata and loopback always refused, no proxy, bounded redirects and responses); subscription secrets are sealed at rest.
- **Operable.** A delivery ledger (exactly once per subscription and event on the sender side), redelivery, auto-disable after repeated failures, retention, Prometheus metrics.

Delivery is at least once and unordered: handlers are idempotent and drop stale updates by a version in the data.

```sh
go get github.com/govengers/events
```

## Usage

```go
// The publisher's contract package, imported by subscribers.
type OrderPlaced struct {
    OrderID string `json:"orderId"`
    Version int    `json:"version"`
}

var OrderPlacedTopic = events.NewTopic[OrderPlaced]("orders.order-placed.v1")
```

```go
// main of every service: one store, one worker, one bus.
jobStore, err := jobspostgres.New(&jobspostgres.Options{Pool: pool})
eventStore, err := postgresadapter.New(&postgresadapter.Options{
    Pool: pool, Jobs: jobStore, Source: "orders", Sealer: sealer, // govengers/auth's seal key ring in a wrapper, docs/wiki/Security.md
})
worker, err := jobs.NewWorker(&jobs.WorkerOptions{
    Store: jobStore,
    Queues: map[string]jobs.QueueOptions{
        events.Queue:                   {Concurrency: 8}, // deliveries, registrations, the sweep; a slow subscriber holds a slot per attempt
        events.SubscriberQueue("orders"): {},
    },
    Schedules: []jobs.Schedule{events.SweepSchedule()},
})
bus, err := events.New(&events.Options{
    Name: "orders", Store: eventStore, Worker: worker,
    HTTPClient: deliveries, // safehttp.New(&safehttp.Options{AllowPrivateNetworks: true}) inside a cluster
    Authorize:  mayRegister, // which subscriber may register which topics and endpoints
})
router.PUT("/events/subscribers/:subscriber", apiKeyMiddleware, gin.WrapH(bus.RegistrationHandler()))
```

```go
// orders repository: the event commits with the order.
_, err = r.options.Events.Publish(ctx, transaction, contract.OrderPlacedTopic.New(contract.OrderPlaced{OrderID: id, Version: version}))
```

```go
// billing main: a remote subscription, registered at orders at startup and retried until orders answers.
bus, err := events.New(&events.Options{
    Name: "billing", Store: eventStore, Worker: worker,
    InboxKey: inboxKey, InboxURL: "https://billing.internal/events",
    Publishers: []events.Publisher{{Name: "orders", URL: "https://orders.internal/events", APIKey: ordersAPIKey}},
})
err = events.Subscribe(bus, contract.OrderPlacedTopic, invoiceController.CreateForOrder, &events.SubscribeOptions{
    Name: "create-invoice", Publisher: "orders", // empty Publisher: a local subscription in this database
})
err = bus.Register(ctx)
router.POST("/events/:subscription", gin.WrapH(bus.InboxHandler())) // no auth middleware: the signature authenticates

func (c *InvoiceEventController) CreateForOrder(ctx context.Context, message *events.Message[contract.OrderPlaced]) error {
    return c.options.InvoiceService.CreateForOrder(ctx, message.Data.OrderID, message.Data.Version)
}
```

Add `postgresadapter.MigrationV1` to your goose migrations after jobs' `MigrationV1`; nothing runs DDL at startup.

## Packages

| Package | What |
|---------|------|
| `events` | topics, the CloudEvents envelope, Standard Webhooks signing, the `Bus` (delivery, inbox, registration, sweep), `Store`, `Sealer`, `Observer` |
| `events/postgresadapter` | Publish, subscriptions, the delivery ledger, the inbox, admin operations (`Subscriptions`, `Deliveries`, `Redeliver`, `DisableSubscription`, `EnableSubscription`), `MigrationV1` |
| `events/safehttp` | an `http.Client` for URLs someone else chose: SSRF checks at dial time, no proxy, bounded redirects and responses |
| `events/prometheusadapter` | the Observer as Prometheus metrics on your registry |

`examples/shop` is a tested pair of services (orders publishes, billing subscribes remotely); the end-to-end suite runs both.

## Docs

- [docs/wiki/Home.md](docs/wiki/Home.md): model, package map, wiring order.
- [docs/wiki/Publish-and-Subscribe.md](docs/wiki/Publish-and-Subscribe.md): topics, Publish, local and remote subscriptions, registration.
- [docs/wiki/Delivery-and-Inbox.md](docs/wiki/Delivery-and-Inbox.md): the ledger, retries, the inbox, deduplication, auto-disable.
- [docs/wiki/Security.md](docs/wiki/Security.md): signatures, secrets, the SSRF-safe client, identities.
- [docs/wiki/Operations.md](docs/wiki/Operations.md): admin operations, webhooks an application creates, retention, metrics, tracing.
- [docs/wiki/Testing.md](docs/wiki/Testing.md): testing services that publish and subscribe.
- [docs/wiki/Gotchas.md](docs/wiki/Gotchas.md): the rules that are easy to get wrong.
- [docs/design.md](docs/design.md): the binding design, decisions E1 onwards.
- [llms.txt](llms.txt): the same for agents.
