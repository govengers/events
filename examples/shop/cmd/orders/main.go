// Command orders is the publishing service of the shop example: POST /orders stores an order
// and publishes orders.order-placed.v1 in the same transaction; subscribers register their sets
// at PUT /events/subscribers/{subscriber} with an API key from ORDERS_API_KEYS
// ("subscriber=key,..."); the worker delivers to their inboxes.
//
// Simplifications, each one line in a real service: net/http instead of Gin, SQL inline instead
// of sqlc behind a repository, the schema applied at startup instead of goose, API keys from the
// environment instead of govengers/auth's apikey.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/events"
	"github.com/govengers/events/examples/shop/contract"
	"github.com/govengers/events/examples/shop/internal/platform"
	"github.com/govengers/events/postgresadapter"
)

const tables = `CREATE TABLE IF NOT EXISTS orders (id uuid PRIMARY KEY DEFAULT uuidv7(), total integer NOT NULL, version integer NOT NULL DEFAULT 1)`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	pool, err := platform.Database(ctx, os.Getenv("DATABASE_URL"), tables)
	if err != nil {
		return err
	}
	defer pool.Close()
	components, err := platform.NewComponents(pool, "orders")
	if err != nil {
		return err
	}
	client, err := platform.HTTPClient()
	if err != nil {
		return err
	}
	bus, err := events.New(&events.Options{
		Name: "orders", Store: components.Events, Worker: components.Worker, HTTPClient: client,
		Authorize: authorizeByAPIKey(os.Getenv("ORDERS_API_KEYS")), AllowHTTP: os.Getenv("SHOP_LOOPBACK") == "1",
	})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("PUT /events/subscribers/{subscriber}", bus.RegistrationHandler())
	mux.Handle("POST /orders", placeOrder(pool, components.Events))

	group, ctx := errgroup.WithContext(ctx)
	group.Go(platform.Recovering(func() error { return components.Worker.Run(ctx) }))
	group.Go(platform.Recovering(func() error { return platform.Serve(ctx, os.Getenv("LISTEN"), mux) }))
	return group.Wait()
}

// authorizeByAPIKey lets a caller register the subscriber its API key belongs to, for any topic.
func authorizeByAPIKey(configured string) events.AuthorizeFunc {
	keys := map[string]string{} // subscriber -> key
	for entry := range strings.SplitSeq(configured, ",") {
		if subscriber, key, found := strings.Cut(entry, "="); found {
			keys[subscriber] = key
		}
	}
	return func(request *http.Request, registration *events.RegistrationRequest) error {
		presented, _ := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
		key, known := keys[registration.Subscriber]
		if !known || subtle.ConstantTimeCompare([]byte(presented), []byte(key)) != 1 {
			return errors.New("unknown API key")
		}
		return nil
	}
}

type placeOrderRequest struct {
	Total int `json:"total"`
}

func placeOrder(pool *pgxpool.Pool, eventStore *postgresadapter.Store) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input placeOrderRequest
		if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 4096)).Decode(&input); err != nil || input.Total <= 0 {
			http.Error(writer, "want {\"total\": cents > 0}", http.StatusBadRequest)
			return
		}
		id, err := storeAndPublish(request.Context(), pool, eventStore, input.Total)
		if err != nil {
			http.Error(writer, "could not place the order", http.StatusInternalServerError)
			fmt.Fprintln(os.Stderr, err)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(map[string]string{"id": id})
	})
}

// storeAndPublish is the repository: the event commits or rolls back with the order (E4).
func storeAndPublish(ctx context.Context, pool *pgxpool.Pool, eventStore *postgresadapter.Store, total int) (string, error) {
	transaction, err := pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin place order: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	var id string
	var version int
	if err := transaction.QueryRow(ctx, "INSERT INTO orders (total) VALUES ($1) RETURNING id::text, version", total).Scan(&id, &version); err != nil {
		return "", fmt.Errorf("insert order: %w", err)
	}
	placed := contract.OrderPlaced{OrderID: id, Total: total, Version: version}
	if _, err := eventStore.Publish(ctx, transaction, contract.OrderPlacedTopic.New(placed)); err != nil {
		return "", fmt.Errorf("publish order %s: %w", id, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit order %s: %w", id, err)
	}
	return id, nil
}
