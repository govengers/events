# Gotchas

- **Publish with the transaction of the change.** Publish on the pool writes outside it: a rolled-back order still announces itself, and a failure between the ledger row and the job leaves a pending delivery without a job (`Redeliver` fixes it).
- **Handlers are idempotent and order-blind.** Delivery is at least once and unordered; key effects by the aggregate and drop updates with an older version.
- **Consume the queues.** The worker must consume `events.Queue` (deliveries, registrations, sweep) and `events.SubscriberQueue(name)` (or every `SubscribeOptions.Queue`), otherwise nothing moves.
- **Subscribe before Run, Register after the last Subscribe.** jobs refuses handler registration while the worker runs.
- **No authentication on the inbox, authentication on the registration.** The signature authenticates the inbox; the registration route needs the API key middleware and `Authorize`.
- **The default client refuses private networks.** Inside a cluster pass a safehttp client with `AllowPrivateNetworks` or `AllowedNetworks`; a refused address fails the delivery at once.
- **Disabled means skipped.** Events published while a subscription is disabled are not recorded and never delivered; only failed deliveries can be redelivered.
- **Rotating `InboxKey` needs a registration.** Until the publisher stored the new secrets, its deliveries are refused with 401 and retried.
- **A subscription name is local to its service** (`create-invoice`); the job kind is `events.<service>.<name>`. Renaming a subscription is removing one and adding another: deliveries in flight for the old name are retried on the inbox's 404 until the publisher stored the new set, then dropped.
- **Events before the subscription are lost for it.** There is no event log in v0.
