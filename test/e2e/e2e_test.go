//go:build e2e

// Package e2e_test runs the shop example's two services, the binaries of examples/shop/cmd built
// by `just e2e-build` into E2E_BINARIES, on two databases of one PostgreSQL, and checks what only
// real processes show: billing registers at orders at startup, an order placed at orders
// becomes an invoice at billing, and an order placed while billing is down arrives once billing
// is back.
package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/events/internal/postgrestest"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

const (
	waitFor   = 30 * time.Second
	pollEvery = 50 * time.Millisecond
	sealKey   = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	inboxKey  = "f0e0d0c0b0a090807060504030201000f0e0d0c0b0a090807060504030201000"
	apiKey    = "billing-secret-api-key"
)

// process is one service process.
type process struct {
	name    string
	command *exec.Cmd
	output  *lockedBuffer
	exited  chan struct{}
	err     error // Wait's result, set before exited is closed
}

type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

// start runs a service binary and waits for its "ready" line. The cleanup stops it with SIGTERM
// unless the test did, and fails the test on an exit status other than 0, a race report or a panic.
func start(t *testing.T, binary string, environment ...string) *process {
	t.Helper()
	directory := os.Getenv("E2E_BINARIES")
	if directory == "" {
		t.Fatal("E2E_BINARIES is not set; run the suite with just test-e2e")
	}
	command := exec.CommandContext(context.WithoutCancel(t.Context()), filepath.Join(directory, binary)) //nolint:gosec // G702: the binaries just e2e-build built
	command.Env = append(append(os.Environ(), "SHOP_LOOPBACK=1", "SHOP_POLL_INTERVAL=50ms", "SHOP_SEAL_KEY="+sealKey), environment...)
	started := &process{name: binary, command: command, output: &lockedBuffer{}, exited: make(chan struct{})}
	command.Stderr = started.output
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "ready" {
				close(ready)
				continue
			}
			_, _ = started.output.Write(append(scanner.Bytes(), '\n'))
		}
		started.err = command.Wait()
		close(started.exited)
	}()
	t.Cleanup(func() { started.stop(t) })
	select {
	case <-ready:
	case <-started.exited:
		t.Fatalf("%s exited before it was ready: %v\n%s", binary, started.err, started.output)
	case <-time.After(waitFor):
		t.Fatalf("%s did not start:\n%s", binary, started.output)
	}
	return started
}

// stop sends SIGTERM and checks a clean exit.
func (p *process) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.exited:
	default:
		_ = p.command.Process.Signal(syscall.SIGTERM) // fails only when it exited meanwhile
		select {
		case <-p.exited:
		case <-time.After(waitFor):
			t.Errorf("%s did not stop:\n%s", p.name, p.output)
			return
		}
	}
	output := p.output.String()
	if p.err != nil {
		t.Errorf("%s exited with %v:\n%s", p.name, p.err, output)
	}
	for _, marker := range []string{"WARNING: DATA RACE", "panic:", "fatal error:"} {
		if strings.Contains(output, marker) {
			t.Errorf("%s reported %q:\n%s", p.name, marker, output)
		}
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	return listener.Addr().String()
}

// shop is the two services and their databases.
type shop struct {
	ordersDatabase, billingDatabase *pgxpool.Pool
	ordersAddress, billingAddress   string
}

func (s *shop) startOrders(t *testing.T) *process {
	t.Helper()
	return start(t, "orders", "DATABASE_URL="+s.ordersDatabase.Config().ConnString(), "LISTEN="+s.ordersAddress, "ORDERS_API_KEYS=billing="+apiKey)
}

func (s *shop) startBilling(t *testing.T) *process {
	t.Helper()
	return start(t, "billing", "DATABASE_URL="+s.billingDatabase.Config().ConnString(), "LISTEN="+s.billingAddress,
		"INBOX_URL=http://"+s.billingAddress+"/events", "INBOX_KEY="+inboxKey,
		"ORDERS_URL=http://"+s.ordersAddress+"/events", "ORDERS_API_KEY="+apiKey)
}

func (s *shop) placeOrder(t *testing.T, total int) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+s.ordersAddress+"/orders", strings.NewReader(fmt.Sprintf(`{"total": %d}`, total)))
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	require.Equal(t, http.StatusCreated, response.StatusCode, string(body))
}

func count(t *testing.T, pool *pgxpool.Pool, query string) int {
	t.Helper()
	var rows int
	require.NoError(t, pool.QueryRow(t.Context(), query).Scan(&rows))
	return rows
}

func (s *shop) awaitInvoices(t *testing.T, invoices int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return count(t, s.billingDatabase, "SELECT count(*) FROM invoices") == invoices
	}, waitFor, pollEvery, "never %d invoices", invoices)
}

func TestAnOrderBecomesAnInvoiceAlsoWhenBillingWasDown(t *testing.T) {
	shop := &shop{
		ordersDatabase: postgrestest.New(t), billingDatabase: postgrestest.New(t),
		ordersAddress: freeAddress(t), billingAddress: freeAddress(t),
	}
	shop.startOrders(t)
	billing := shop.startBilling(t)
	require.Eventually(t, func() bool {
		return count(t, shop.ordersDatabase, "SELECT count(*) FROM govengers_event_subscriptions WHERE subscriber = 'billing'") == 1
	}, waitFor, pollEvery, "billing never registered")

	shop.placeOrder(t, 1200)
	shop.awaitInvoices(t, 1)

	billing.stop(t)
	shop.placeOrder(t, 3400)
	require.Eventually(t, func() bool {
		return count(t, shop.ordersDatabase, "SELECT count(*) FROM govengers_event_deliveries WHERE status = 'pending' AND attempts > 0") == 1
	}, waitFor, pollEvery, "the delivery to the stopped billing was never tried")

	shop.startBilling(t)
	// The retry waits for its backoff; the suite moves it to now instead of sleeping.
	_, err := shop.ordersDatabase.Exec(t.Context(), "UPDATE govengers_jobs SET run_at = now() WHERE status = 'pending'")
	require.NoError(t, err)
	shop.awaitInvoices(t, 2)
	require.Equal(t, 2, count(t, shop.ordersDatabase, "SELECT count(*) FROM govengers_event_deliveries WHERE status = 'delivered'"))
}
