// Package safehttp builds an http.Client for requests to URLs that someone else chose (webhook
// endpoints, callback URLs) and that must not reach into the sender's own network (server-side
// request forgery).
//
// The address check runs in the dialer, on the address the connection really goes to, after DNS
// resolution: a host name that resolves to an internal address is refused even when it resolved
// to a public one a moment earlier (DNS rebinding), and every redirect is dialed through the same
// check. Loopback, link-local (which holds the cloud metadata services), unspecified, multicast
// and the known metadata addresses are always refused; private networks (RFC 1918, unique local,
// shared address space) are refused unless Options.AllowPrivateNetworks is set. The client uses
// no proxy from the environment, follows at most Options.MaximumRedirects redirects and fails a
// response body read past Options.MaximumResponseBytes.
package safehttp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

// Defaults of a zero Options value.
const (
	DefaultTimeout              = 30 * time.Second
	DefaultMaximumResponseBytes = 1 << 20
)

var (
	// ErrBlockedAddress is returned (wrapped in the client's url.Error) when a request would
	// connect to a refused address.
	ErrBlockedAddress = errors.New("safehttp: blocked address")
	// ErrTooManyRedirects is returned when a response redirects more often than allowed.
	ErrTooManyRedirects = errors.New("safehttp: too many redirects")
	// ErrResponseTooLarge is returned by a response body's Read past the size cap.
	ErrResponseTooLarge = errors.New("safehttp: response too large")
)

// Options configures the client. A nil Options is the zero value: defaults, private networks refused.
type Options struct {
	Timeout              time.Duration // the whole request including the body; zero means DefaultTimeout
	MaximumRedirects     int           // redirects followed; zero follows none
	MaximumResponseBytes int64         // zero means DefaultMaximumResponseBytes
	// AllowPrivateNetworks allows RFC 1918, IPv6 unique local and the 100.64.0.0/10 shared
	// address space, for deliveries inside a cluster. Loopback, link-local and metadata stay refused.
	AllowPrivateNetworks bool
	// AllowedNetworks, when set, is the only destinations allowed besides the refusals above,
	// which it cannot lift: an allowlist for a deployment that knows its receivers' networks.
	AllowedNetworks []netip.Prefix

	allowLoopback bool // tests only: their servers listen on loopback
}

// New returns the client. It fails for negative durations or limits.
func New(options *Options) (*http.Client, error) {
	if options == nil {
		options = &Options{}
	}
	if options.Timeout < 0 || options.MaximumRedirects < 0 || options.MaximumResponseBytes < 0 {
		return nil, errors.New("safehttp: negative Timeout, MaximumRedirects or MaximumResponseBytes")
	}
	policy := &policy{
		allowPrivate:  options.AllowPrivateNetworks,
		allowed:       append([]netip.Prefix(nil), options.AllowedNetworks...),
		allowLoopback: options.allowLoopback,
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, Control: policy.control}
	transport := &http.Transport{
		Proxy:                 nil, // an environment proxy would dial for us, past the address check
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	maximumRedirects := options.MaximumRedirects
	return &http.Client{
		Timeout: orDefault(options.Timeout, DefaultTimeout),
		Transport: &cappedTransport{
			next:    transport,
			maximum: orDefault(options.MaximumResponseBytes, DefaultMaximumResponseBytes),
		},
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) > maximumRedirects {
				return ErrTooManyRedirects
			}
			return nil
		},
	}, nil
}

// policy decides which addresses a connection may go to.
type policy struct {
	allowPrivate  bool
	allowed       []netip.Prefix
	allowLoopback bool
}

// control runs after DNS resolution with the address the socket connects to.
func (p *policy) control(_, address string, _ syscall.RawConn) error {
	addressPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlockedAddress, address)
	}
	if err := p.check(addressPort.Addr()); err != nil {
		return fmt.Errorf("%w: %s", err, address)
	}
	return nil
}

// alwaysBlocked are refused whatever the options say: cloud metadata services and NAT64 and
// 6to4 prefixes, which would smuggle an IPv4 address of the list past the checks.
var alwaysBlocked = []netip.Prefix{
	netip.MustParsePrefix("169.254.0.0/16"),     // link-local, AWS/GCP/Azure metadata 169.254.169.254
	netip.MustParsePrefix("100.100.100.200/32"), // Alibaba Cloud metadata
	netip.MustParsePrefix("fd00:ec2::254/128"),  // AWS metadata over IPv6
	netip.MustParsePrefix("0.0.0.0/8"),          // "this network"
	netip.MustParsePrefix("64:ff9b::/96"),       // NAT64
	netip.MustParsePrefix("64:ff9b:1::/48"),     // local-use NAT64
	netip.MustParsePrefix("2002::/16"),          // 6to4
	netip.MustParsePrefix("240.0.0.0/4"),        // reserved, incl. broadcast
	netip.MustParsePrefix("::/96"),              // IPv4-compatible IPv6, deprecated
}

// privateNetworks are refused unless AllowPrivateNetworks is set.
var privateNetworks = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"), // shared address space (carrier-grade NAT)
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
}

func (p *policy) check(address netip.Addr) error {
	address = address.Unmap()
	switch {
	case address.IsLoopback() && p.allowLoopback:
		return nil
	case address.IsLoopback(), address.IsLinkLocalUnicast(), address.IsLinkLocalMulticast(),
		address.IsInterfaceLocalMulticast(), address.IsMulticast(), address.IsUnspecified(),
		containsAny(alwaysBlocked, address):
		return ErrBlockedAddress
	case !p.allowPrivate && (address.IsPrivate() || containsAny(privateNetworks, address)):
		return ErrBlockedAddress
	case len(p.allowed) > 0 && !containsAny(p.allowed, address):
		return ErrBlockedAddress
	}
	return nil
}

func containsAny(prefixes []netip.Prefix, address netip.Addr) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

// cappedTransport caps every response body.
type cappedTransport struct {
	next    http.RoundTripper
	maximum int64
}

func (t *cappedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.ContentLength > t.maximum {
		_ = response.Body.Close() // refused before reading
		return nil, ErrResponseTooLarge
	}
	response.Body = &cappedBody{ReadCloser: response.Body, remaining: t.maximum}
	return response, nil
}

// cappedBody fails a read past its limit instead of truncating silently.
type cappedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *cappedBody) Read(buffer []byte) (int, error) {
	if b.remaining <= 0 {
		var probe [1]byte
		count, err := b.ReadCloser.Read(probe[:])
		if count > 0 {
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(buffer)) > b.remaining {
		buffer = buffer[:b.remaining]
	}
	count, err := b.ReadCloser.Read(buffer)
	b.remaining -= int64(count)
	return count, err
}

func orDefault[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}
