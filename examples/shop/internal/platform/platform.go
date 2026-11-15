// Package platform holds the wiring both example services share: the database with its
// self-applied schema, the job worker, the HTTP server and the sealer.
package platform

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/events"
	"github.com/govengers/events/postgresadapter"
	"github.com/govengers/events/safehttp"
	"github.com/govengers/jobs"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

// Database opens the pool, applies the jobs and events tables when they are missing and then
// tables, which must be idempotent. Simplification: a real service runs jobs' and events'
// MigrationV1 and its own tables from goose migrations.
func Database(ctx context.Context, url, tables string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("open the database: %w", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('govengers_jobs') IS NOT NULL").Scan(&exists); err != nil {
		pool.Close()
		return nil, fmt.Errorf("inspect the schema: %w", err)
	}
	migrations := []string{tables}
	if !exists {
		migrations = []string{jobspostgres.MigrationV1, postgresadapter.MigrationV1, tables}
	}
	for _, migration := range migrations {
		if _, err := pool.Exec(ctx, "BEGIN;"+migration+";COMMIT;"); err != nil {
			pool.Close()
			return nil, fmt.Errorf("apply the schema: %w", err)
		}
	}
	return pool, nil
}

// Components are what both services build the same way.
type Components struct {
	Jobs   *jobspostgres.Store
	Events *postgresadapter.Store
	Worker *jobs.Worker
}

// NewComponents builds the job store, the event store and a worker consuming the library's
// queue and the service's subscriber queue, with the sweep schedule.
func NewComponents(pool *pgxpool.Pool, service string) (*Components, error) {
	jobStore, err := jobspostgres.New(&jobspostgres.Options{Pool: pool})
	if err != nil {
		return nil, err
	}
	sealer, err := NewSealer(os.Getenv("SHOP_SEAL_KEY"))
	if err != nil {
		return nil, err
	}
	eventStore, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Jobs: jobStore, Source: service, Sealer: sealer})
	if err != nil {
		return nil, err
	}
	poll, err := time.ParseDuration(orDefault(os.Getenv("SHOP_POLL_INTERVAL"), "1s"))
	if err != nil {
		return nil, fmt.Errorf("SHOP_POLL_INTERVAL: %w", err)
	}
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{
		Store: jobStore,
		Name:  service,
		Queues: map[string]jobs.QueueOptions{
			// one slow subscriber holds a slot for up to the client's timeout per attempt
			events.Queue:                    {PollInterval: poll, Concurrency: 8},
			events.SubscriberQueue(service): {PollInterval: poll},
		},
		Schedules: []jobs.Schedule{events.SweepSchedule()},
	})
	if err != nil {
		return nil, err
	}
	return &Components{Jobs: jobStore, Events: eventStore, Worker: worker}, nil
}

// HTTPClient is the client of deliveries and registrations. Inside a cluster the receivers have
// private addresses; SHOP_LOOPBACK=1 (local runs, the end-to-end suite) uses a plain client,
// because safehttp refuses loopback whatever its options say.
func HTTPClient() (*http.Client, error) {
	if os.Getenv("SHOP_LOOPBACK") == "1" {
		return &http.Client{Timeout: 10 * time.Second}, nil
	}
	return safehttp.New(&safehttp.Options{AllowPrivateNetworks: true})
}

// Serve serves handler on address until ctx ends, then shuts down within five seconds. It
// prints "ready" once it listens, which the end-to-end suite waits for.
func Serve(ctx context.Context, address string, handler http.Handler) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- Recovering(func() error { return server.Serve(listener) })() }()
	fmt.Println("ready")
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		return fmt.Errorf("shut down: %w", err)
	}
	if err := <-done; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Sealer seals secrets with AES-256-GCM. Simplification: a real service uses the key ring of
// govengers/auth's seal package, which rotates keys.
type Sealer struct{ aead cipher.AEAD }

// NewSealer reads a 32-byte key in hex.
func NewSealer(hexKey string) (*Sealer, error) {
	key, err := hex.DecodeString(hexKey)
	if err != nil || len(key) != 32 {
		return nil, errors.New("SHOP_SEAL_KEY must be 32 bytes in hex")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: aead}, nil
}

// Seal encrypts plaintext bound to associatedData.
func (s *Sealer) Seal(plaintext, associatedData []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, plaintext, associatedData), nil
}

// Open decrypts what Seal returned for the same associatedData.
func (s *Sealer) Open(sealed, associatedData []byte) ([]byte, error) {
	size := s.aead.NonceSize()
	if len(sealed) < size {
		return nil, errors.New("sealed value too short")
	}
	return s.aead.Open(nil, sealed[:size], sealed[size:], associatedData)
}

// Recovering turns a panic in work into an error for its errgroup or channel (CON-001).
func Recovering(work func() error) func() error {
	return func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())
			}
		}()
		return work()
	}
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
