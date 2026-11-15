package events_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/events"
	"github.com/govengers/events/postgresadapter"
	"github.com/govengers/events/safehttp"
	"github.com/govengers/jobs"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

func ExampleSign() {
	secret := events.DeriveSecret([]byte("billing-inbox-key-0123456789abcdef"), "create-invoice")
	body := []byte(`{"specversion":"1.0","id":"e-1","source":"orders","type":"orders.order-placed.v1"}`)
	signedAt := time.Unix(1731600000, 0)

	header := http.Header{}
	if err := events.Sign(header, secret, "e-1", signedAt, body); err != nil {
		panic(err)
	}
	fmt.Println(header.Get(events.HeaderTimestamp))
	fmt.Println(events.Verify(header, secret, body, signedAt.Add(time.Minute)))
	fmt.Println(events.Verify(header, secret, []byte(`{"tampered":true}`), signedAt) != nil)
	// Output:
	// 1731600000
	// <nil>
	// true
}

// ExampleNew wires a service that publishes and subscribes; compiled, not run (it needs a database).
func ExampleNew() {
	ctx := context.Background()
	var pool *pgxpool.Pool   // the service's database, both MigrationV1 applied by goose
	var sealer events.Sealer // the key ring of govengers/auth's seal package
	jobStore, _ := jobspostgres.New(&jobspostgres.Options{Pool: pool})
	eventStore, _ := postgresadapter.New(&postgresadapter.Options{Pool: pool, Jobs: jobStore, Source: "billing", Sealer: sealer})
	worker, _ := jobs.NewWorker(&jobs.WorkerOptions{
		Store:     jobStore,
		Queues:    map[string]jobs.QueueOptions{events.Queue: {}, events.SubscriberQueue("billing"): {}},
		Schedules: []jobs.Schedule{events.SweepSchedule()},
	})
	client, _ := safehttp.New(&safehttp.Options{AllowPrivateNetworks: true})
	bus, err := events.New(&events.Options{
		Name: "billing", Store: eventStore, Worker: worker, HTTPClient: client,
		InboxKey: []byte("from-configuration-32-bytes-or-more"), InboxURL: "https://billing.internal/events",
		Publishers: []events.Publisher{{Name: "orders", URL: "https://orders.internal/events", APIKey: "billing-key-at-orders"}},
	})
	if err != nil {
		panic(err)
	}
	err = events.Subscribe(bus, orderPlacedTopic, func(_ context.Context, message *events.Message[OrderPlaced]) error {
		fmt.Println("invoice for", message.Data.OrderID, "version", message.Data.Version)
		return nil
	}, &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"})
	if err != nil {
		panic(err)
	}
	if err := bus.Register(ctx); err != nil { // the registration job retries until orders answers
		panic(err)
	}
	http.Handle("POST /events/{subscription}", bus.InboxHandler())
	// main then runs worker.Run(ctx) and the HTTP server in its errgroup.
}
