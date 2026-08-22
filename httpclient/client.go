package httpclient

// Package provides the shared HTTP clients Philharmonic's own components
// use to talk to each other:
//
// - Manager(): used by the CLI commands to reach the manager API
// - Worker(): used by the manager (and the "nodes" CLI command) to
//   reach worker APIs
// - Plain(): an unauthenticated plain-HTTP client for probing user
//   workloads (task health checks). It never applies TLS or credentials.
//
// Manager() and Worker() start out as plain HTTP with the package-level
// ClientTimeout. A process configures them once at startup via
// ConfigureManagerClient / ConfigureWorkerClient, typically from the
// "client", "manager" or "worker" TLS/auth config sections.
//
// URL builders (ManagerURL / WorkerURL) pick up the configured scheme
// so callers never hardcode "http://" or "https://"

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"
)

// overridden by client config if specified
var ClientTimeout = 10 * time.Second

// bounds long-running operations against worker apis, such as image pulls
var LongOpTimeout = 15 * time.Minute

// zero value = plan http, no config
type Options struct {
	// enables HTTPS, nil keeps plain HTTP
	TLSConfig *tls.Config

	// when set, is sent as "Authorization: Bearer <token>" on
	// every request made by the configured client
	BearerToken string
}

type roleClient struct {
	mu        sync.RWMutex
	transport http.RoundTripper
	tlsOn     bool
}

func (rc *roleClient) configure(opts Options) {
	tr := http.DefaultTransport
	if opts.TLSConfig != nil || opts.BearerToken != "" {
		tr = cloneTransport(tr) // tr now should be *http.Transport
		if tc, ok := tr.(*http.Transport); ok && opts.TLSConfig != nil {
			tc.TLSClientConfig = opts.TLSConfig
		}
		if opts.BearerToken != "" {
			tr = bearerTransport{base: tr, token: opts.BearerToken}
		}
	}

	rc.mu.Lock()
	rc.transport = tr
	rc.tlsOn = opts.TLSConfig != nil
	rc.mu.Unlock()
}

func cloneTransport(tr http.RoundTripper) http.RoundTripper {
	if tr == nil {
		tr = http.DefaultTransport
	}
	base, ok := tr.(*http.Transport)
	if !ok {
		return tr
	}
	return base.Clone()
}

func (rc *roleClient) reset() {
	rc.mu.Lock()
	rc.transport = nil
	rc.tlsOn = false
	rc.mu.Unlock()
}

func (rc *roleClient) client(timeout time.Duration) *http.Client {
	rc.mu.RLock()
	tr := rc.transport
	rc.mu.RUnlock()

	// the transport (and its connection pool) is shared,
	// so constructing the client itself is cheap
	return &http.Client{Timeout: timeout, Transport: tr}
}

func (rc *roleClient) scheme() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	if rc.tlsOn {
		return "https"
	}
	return "http"
}

var (
	managerClient roleClient // CLI -> manager API
	workerClient  roleClient // manager/CLI -> worker APIs
)

func ConfigureManagerClient(opts Options) {
	managerClient.configure(opts)
}

func ConfigureWorkerClient(opts Options) {
	workerClient.configure(opts)
}

// intended for testing
func Reset() {
	managerClient.reset()
	workerClient.reset()
}

// client for reaching the manager api
func Manager() *http.Client {
	return managerClient.client(ClientTimeout)
}

// used by the manager to reach worker apis
func Worker() *http.Client {
	return workerClient.client(ClientTimeout)
}

// client for long-running operations against worker apis
// shares the worker client's TLS/auth transport, but uses LongOpTimeout
func WorkerLongOp() *http.Client {
	return workerClient.client(LongOpTimeout)
}

// no tls no auth
func Plain() *http.Client {
	return &http.Client{Timeout: ClientTimeout}
}

// reports the URL scheme ("http" or "https") currently
// configured for manager API URLs
func ManagerScheme() string {
	return managerClient.scheme()
}

// reports the URL scheme ("http" or "https") currently
// configured for worker API URLs
func WorkerScheme() string {
	return workerClient.scheme()
}

// builds a manager API URL in the configured scheme:
// "<scheme>://<address><path>". address is a "host:port" pair
func ManagerURL(address, path string) string {
	return managerClient.scheme() + "://" + address + path
}

// builds a worker API URL in the configured scheme:
// "<scheme>://<address><path>". address is a "host:port" pair
func WorkerURL(address, path string) string {
	return workerClient.scheme() + "://" + address + path
}

// adds an Authorization header to every request.
type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (t bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// RoundTrippers must not mutate the caller's request by contract
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+t.token)
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(r)
}
