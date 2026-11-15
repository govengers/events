# govengers/events

Publish and subscribe between govengers services on top of govengers/jobs. Jobs stay inside a service, events cross services, and an event arrives as a job: the library adds routing and transport, the job runtime does the processing.

## Model

| Situation | Subscription | What Publish does |
|-----------|--------------|-------------------|
| a module in the same database reacts | local | enqueues the subscriber's handler job in the publishing transaction |
| another service with its own database reacts | remote | records a delivery in the ledger and enqueues its outbound job; the job POSTs a signed CloudEvent to the subscriber's inbox, which enqueues the handler job there |

Delivery is at least once and unordered. Handlers are idempotent and drop stale updates by a version in the data.

## Packages

| Package | Use it for |
|---------|-----------|
| `events` | topics (`NewTopic`), the `Bus`, `Subscribe`, the envelope (`Event`, `Message`), `Sign`/`Verify`, `NewSecret`/`DeriveSecret` |
| `events/postgresadapter` | `Publish`, `PutSubscriptions`, admin operations, `MigrationV1` |
| `events/safehttp` | outbound requests to URLs someone else chose |
| `events/prometheusadapter` | metrics |

## Wiring order in `main`

1. Pool, `jobs/postgresadapter.New`, `events/postgresadapter.New` (Source, Sealer, Propagators, Actor).
2. `jobs.NewWorker` with queues `events.Queue` and `events.SubscriberQueue(name)` and `events.SweepSchedule()`.
3. `events.New` (registers its handlers on the worker).
4. `events.Subscribe` for every subscription, then `bus.Register(ctx)`.
5. Mount `bus.InboxHandler()` (no authentication) and `bus.RegistrationHandler()` (behind the API key middleware).
6. Run the worker and the HTTP server.

See [Publish-and-Subscribe](Publish-and-Subscribe.md), [Delivery-and-Inbox](Delivery-and-Inbox.md), [Security](Security.md), [Operations](Operations.md), [Testing](Testing.md), [Gotchas](Gotchas.md), and the decisions in [docs/design.md](../design.md).
