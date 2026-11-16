# Testing

## In a service

Handlers are job handlers, so `jobstest.Drain(t, worker)` runs them synchronously:

```go
ids, err := eventStore.Publish(ctx, pool, contract.OrderPlacedTopic.New(contract.OrderPlaced{OrderID: "o-1"}))
jobstest.Drain(t, worker) // runs the local subscriptions' handlers
```

For a remote subscription, run both services in one test: two schemas of one embedded PostgreSQL, two `httptest` servers with the inbox and registration handlers, `Register` then `Drain` on each side. A retry waits for its backoff; make it due instead of sleeping:

```go
_, err := pool.Exec(ctx, "UPDATE govengers_jobs SET run_at = now() WHERE status = 'pending'")
```

`safehttp` refuses loopback whatever its options say; tests pass a plain `*http.Client` as `Options.HTTPClient`.

## In this repository

- `just test`: unit and integration tests, one embedded PostgreSQL per test binary, a schema per test, a few seconds per package.
- `just test-e2e`: the shop example's two binaries on two databases, billing stopped and started again; skipped when nothing it covers changed.
- `just check`: lint, tests, e2e, vulncheck: what CI runs.
