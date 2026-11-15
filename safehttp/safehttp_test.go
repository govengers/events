package safehttp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestCheck(t *testing.T) {
	strict := &policy{}
	private := &policy{allowPrivate: true}
	allowlisted := &policy{allowPrivate: true, allowed: []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16"), netip.MustParsePrefix("203.0.113.0/24")}}
	cases := []struct {
		address                      string
		strict, private, allowlisted bool // allowed under each policy
	}{
		{"93.184.216.34", true, true, false},
		{"2606:2800:220:1::1", true, true, false},
		{"203.0.113.7", true, true, true},
		{"10.20.1.1", false, true, true},
		{"10.21.1.1", false, true, false},
		{"192.168.1.1", false, true, false},
		{"172.16.0.1", false, true, false},
		{"100.64.0.1", false, true, false},
		{"fd12::1", false, true, false},
		{"127.0.0.1", false, false, false},
		{"::1", false, false, false},
		{"::ffff:127.0.0.1", false, false, false},
		{"::ffff:169.254.169.254", false, false, false},
		{"169.254.169.254", false, false, false},
		{"fe80::1", false, false, false},
		{"fd00:ec2::254", false, false, false},
		{"100.100.100.200", false, false, false},
		{"0.0.0.0", false, false, false},
		{"::", false, false, false},
		{"224.0.0.1", false, false, false},
		{"ff02::1", false, false, false},
		{"255.255.255.255", false, false, false},
		{"64:ff9b::a9fe:a9fe", false, false, false},
		{"2002:a9fe:a9fe::1", false, false, false},
		{"::7f00:1", false, false, false},
	}
	for _, testCase := range cases {
		address := netip.MustParseAddr(testCase.address)
		require.Equal(t, testCase.strict, strict.check(address) == nil, "strict %s", testCase.address)
		require.Equal(t, testCase.private, private.check(address) == nil, "private %s", testCase.address)
		require.Equal(t, testCase.allowlisted, allowlisted.check(address) == nil, "allowlisted %s", testCase.address)
	}
}

func get(t *testing.T, client *http.Client, url string) (*http.Response, error) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	return client.Do(request)
}

func TestTheDialerRefusesLoopbackAfterResolution(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) }))
	defer server.Close()
	client, err := New(&Options{AllowPrivateNetworks: true, AllowedNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}})
	require.NoError(t, err)
	defer client.CloseIdleConnections()

	// "localhost" resolves to loopback: the name passes, the dialed address does not.
	for _, url := range []string{server.URL, strings.Replace(server.URL, "127.0.0.1", "localhost", 1)} {
		response, err := get(t, client, url)
		if response != nil {
			_ = response.Body.Close()
		}
		require.ErrorIs(t, err, ErrBlockedAddress, url)
	}
}

func loopbackClient(t *testing.T, options *Options) *http.Client {
	t.Helper()
	options.allowLoopback = true
	client, err := New(options)
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestRedirectsAreBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/final" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(writer, request, "/final", http.StatusFound)
	}))
	defer server.Close()

	response, err := get(t, loopbackClient(t, &Options{}), server.URL+"/start")
	if response != nil {
		_ = response.Body.Close()
	}
	require.ErrorIs(t, err, ErrTooManyRedirects)

	response, err = get(t, loopbackClient(t, &Options{MaximumRedirects: 1}), server.URL+"/start")
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
}

func TestARedirectToABlockedAddressIsRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer server.Close()
	response, err := get(t, loopbackClient(t, &Options{MaximumRedirects: 3}), server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	require.ErrorIs(t, err, ErrBlockedAddress)
}

func TestResponsesAreCapped(t *testing.T) {
	body := strings.Repeat("x", 100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/chunked" {
			writer.(http.Flusher).Flush() // no Content-Length: the cap must hold while reading
		}
		_, _ = io.WriteString(writer, body)
	}))
	defer server.Close()

	response, err := get(t, loopbackClient(t, &Options{MaximumResponseBytes: 100}), server.URL)
	require.NoError(t, err)
	read, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, body, string(read))

	response, err = get(t, loopbackClient(t, &Options{MaximumResponseBytes: 99}), server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	require.ErrorIs(t, err, ErrResponseTooLarge, "a declared length above the cap")

	response, err = get(t, loopbackClient(t, &Options{MaximumResponseBytes: 99}), server.URL+"/chunked")
	require.NoError(t, err)
	_, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
	require.ErrorIs(t, err, ErrResponseTooLarge, "a body that grows past the cap")
}

func TestNoProxyFromTheEnvironment(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
	t.Setenv("HTTP_PROXY", "http://proxy.invalid:3128")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	response, err := get(t, loopbackClient(t, &Options{}), server.URL)
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Equal(t, http.StatusNoContent, response.StatusCode)
}

func TestNewRejectsNegativeOptions(t *testing.T) {
	for _, options := range []*Options{{Timeout: -1}, {MaximumRedirects: -1}, {MaximumResponseBytes: -1}} {
		_, err := New(options)
		require.Error(t, err)
	}
	client, err := New(nil)
	require.NoError(t, err)
	require.Equal(t, DefaultTimeout, client.Timeout)
}
