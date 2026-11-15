// Command billing is the subscribing service of the shop example: it subscribes to the orders
// service's orders.order-placed.v1 as create-invoice, registers that at startup (retried until
// orders answers), receives deliveries in its inbox at POST /events/{subscription} and creates
// one invoice per order. GET /invoices lists them.
//
// Configuration: DATABASE_URL, LISTEN, INBOX_URL (this service's public /events base),
// INBOX_KEY (hex, 32 bytes or more), ORDERS_URL (the orders service's /events base),
// ORDERS_API_KEY, SHOP_SEAL_KEY. Simplifications as in cmd/orders.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/events"
	"github.com/govengers/events/examples/shop/contract"
	"github.com/govengers/events/examples/shop/internal/platform"
)

const tables = `CREATE TABLE IF NOT EXISTS invoices (order_id text PRIMARY KEY, total integer NOT NULL, event_id text NOT NULL)`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	inboxKey, err := hex.DecodeString(os.Getenv("INBOX_KEY"))
	if err != nil {
		return errors.New("INBOX_KEY must be hex")
	}
	pool, err := platform.Database(ctx, os.Getenv("DATABASE_URL"), tables)
	if err != nil {
		return err
	}
	defer pool.Close()
	components, err := platform.NewComponents(pool, "billing")
	if err != nil {
		return err
	}
	client, err := platform.HTTPClient()
	if err != nil {
		return err
	}
	bus, err := events.New(&events.Options{
		Name: "billing", Store: components.Events, Worker: components.Worker, HTTPClient: client,
		InboxKey: inboxKey, InboxURL: os.Getenv("INBOX_URL"), AllowHTTP: os.Getenv("SHOP_LOOPBACK") == "1",
		Publishers: []events.Publisher{{Name: "orders", URL: os.Getenv("ORDERS_URL"), APIKey: os.Getenv("ORDERS_API_KEY")}},
	})
	if err != nil {
		return err
	}
	err = events.Subscribe(bus, contract.OrderPlacedTopic, createInvoice(pool), &events.SubscribeOptions{Name: "create-invoice", Publisher: "orders"})
	if err != nil {
		return err
	}
	if err := bus.Register(ctx); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("POST /events/{subscription}", bus.InboxHandler())
	mux.Handle("GET /invoices", listInvoices(pool))

	group, ctx := errgroup.WithContext(ctx)
	group.Go(platform.Recovering(func() error { return components.Worker.Run(ctx) }))
	group.Go(platform.Recovering(func() error { return platform.Serve(ctx, os.Getenv("LISTEN"), mux) }))
	return group.Wait()
}

// createInvoice is the job controller of the subscription: idempotent, because delivery is at
// least once, and keyed by the order, so a redelivered event changes nothing.
func createInvoice(pool *pgxpool.Pool) func(ctx context.Context, message *events.Message[contract.OrderPlaced]) error {
	return func(ctx context.Context, message *events.Message[contract.OrderPlaced]) error {
		_, err := pool.Exec(ctx, "INSERT INTO invoices (order_id, total, event_id) VALUES ($1, $2, $3) ON CONFLICT (order_id) DO NOTHING",
			message.Data.OrderID, message.Data.Total, message.ID)
		if err != nil {
			return fmt.Errorf("create the invoice of order %s: %w", message.Data.OrderID, err)
		}
		return nil
	}
}

type invoice struct {
	OrderID string `json:"orderId"`
	Total   int    `json:"total"`
	EventID string `json:"eventId"`
}

func listInvoices(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		rows, err := pool.Query(request.Context(), "SELECT order_id, total, event_id FROM invoices ORDER BY order_id")
		if err != nil {
			http.Error(writer, "could not list the invoices", http.StatusInternalServerError)
			return
		}
		invoices, err := pgx.CollectRows(rows, pgx.RowToStructByPos[invoice])
		if err != nil {
			http.Error(writer, "could not list the invoices", http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(invoices)
	})
}
