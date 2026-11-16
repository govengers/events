// Package postgrestest gives the tests of this repository a real PostgreSQL 18 through
// github.com/govengers/postgrestest: one server per test binary (or per test run under its
// launcher), and per test a fresh database copied from a template with the MigrationV1 of jobs
// and of events, dropped when the test ends.
//
// Every package that uses New runs its tests through Main:
//
//	func TestMain(m *testing.M) { postgrestest.Main(m) }
package postgrestest

import (
	"testing"

	"github.com/govengers/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/govengers/events/postgresadapter"
	jobspostgres "github.com/govengers/jobs/postgresadapter"
)

// server is the PostgreSQL of this test binary, started by the first New and closed by Main.
var server = postgrestest.New(&postgrestest.Options{
	Migrate:     postgrestest.Statements(jobspostgres.MigrationV1, postgresadapter.MigrationV1),
	Fingerprint: jobspostgres.MigrationV1 + postgresadapter.MigrationV1,
})

// Main runs the package's tests, closes the PostgreSQL and fails on leaked goroutines (TST-009).
func Main(m *testing.M) { goleak.VerifyTestMain(server.Main(m)) }

// New returns a pool on a fresh database with the MigrationV1 of jobs and of events applied; the
// pool is closed and the database dropped when the test ends. The pool's connection string,
// pool.Config().ConnString(), reaches the same database, for processes the test starts.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return server.Pool(t)
}
