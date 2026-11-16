// Package postgresadapter is the events.Store on PostgreSQL 18 with pgx/v5 (E11): Publish in the
// caller's transaction, subscriptions, the delivery ledger, the inbox, the admin operations and
// the DDL (MigrationV1). Jobs are enqueued through govengers/jobs' postgresadapter in the same
// database.
package postgresadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/events"
	"github.com/govengers/jobs"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

// ErrNoSealer is returned when a remote subscription's secret must be sealed or opened and
// Options.Sealer is nil.
var ErrNoSealer = errors.New("postgresadapter: a remote subscription needs Options.Sealer")

// Store is the events.Store on PostgreSQL. It is safe for concurrent use.
type Store struct {
	pool        *pgxpool.Pool
	jobs        *jobspostgres.Store
	source      string
	sealer      events.Sealer
	propagators []jobs.Propagator
	actor       func(ctx context.Context) string
	observer    events.Observer
}

// Options configures a Store.
type Options struct {
	Pool *pgxpool.Pool       // required; its search_path reaches the tables of both migrations
	Jobs *jobspostgres.Store // required; enqueues the handler, delivery and registration jobs
	// Source is the CloudEvents source of published events: this service's name. Required to Publish.
	Source string
	// Sealer encrypts remote subscriptions' secrets at rest (govengers/auth's seal package
	// through a small wrapper, see events.Sealer). Required as soon as a remote subscription is
	// stored or delivered.
	Sealer events.Sealer
	// Propagators write the trace context of the publishing context into the event's
	// traceparent and tracestate extensions (jobs/oteladapter.NewPropagator()).
	Propagators []jobs.Propagator
	// Actor names the acting identity of the publishing context for the actor extension
	// ("provider:subject"), audit information only (E9); nil sets none.
	Actor    func(ctx context.Context) string
	Observer events.Observer // reports every Publish; nil reports nothing
}

var _ events.Store = (*Store)(nil)

// New returns a Store on options.Pool.
func New(options *Options) (*Store, error) {
	if options == nil || options.Pool == nil || options.Jobs == nil {
		return nil, errors.New("postgresadapter: Options.Pool and Options.Jobs are required")
	}
	for index, propagator := range options.Propagators {
		if propagator == nil {
			return nil, fmt.Errorf("postgresadapter: Options.Propagators[%d] is nil", index)
		}
	}
	return &Store{
		pool: options.Pool, jobs: options.Jobs, source: options.Source, sealer: options.Sealer,
		propagators: append([]jobs.Propagator(nil), options.Propagators...),
		actor:       options.Actor, observer: events.GuardObserver(options.Observer),
	}, nil
}

// Executor runs Publish's statements: a pgx.Tx (usually the caller's), a *pgxpool.Pool or a
// *pgx.Conn.
type Executor interface {
	Exec(ctx context.Context, statement string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, statement string, arguments ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, statement string, arguments ...any) pgx.Row
}

// Enqueue enqueues jobs on the pool, outside any caller's transaction.
func (s *Store) Enqueue(ctx context.Context, preparedJobs ...*jobs.PreparedJob) error {
	_, err := s.jobs.Enqueue(ctx, s.pool, preparedJobs...)
	return err
}

// inTransaction runs work in a READ COMMITTED transaction: the multi-statement paths read rows
// other transactions committed after a lock, whatever the server's default isolation is.
func (s *Store) inTransaction(ctx context.Context, work func(transaction pgx.Tx) error) error {
	transaction, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return wrapDriverError(err, "begin")
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	if err := work(transaction); err != nil {
		return err
	}
	return wrapDriverError(transaction.Commit(ctx), "commit")
}

// wrapDriverError adds context to a driver error without making the driver's error types part
// of the API (LIB-010); nil stays nil.
func wrapDriverError(err error, format string, arguments ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %v", fmt.Sprintf(format, arguments...), err) //nolint:errorlint // LIB-010: pgx errors are not part of the API
}

// associatedData binds a sealed secret to its subscription's row.
func associatedData(subscriber, name string) []byte {
	return []byte("govengers_event_subscriptions/" + subscriber + "/" + name)
}
