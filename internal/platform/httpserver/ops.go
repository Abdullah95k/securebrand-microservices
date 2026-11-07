// Package httpserver provides the operations server every binary exposes:
// /metrics, /healthz (liveness, no dependency checked) and /readyz
// (readiness: the stores, checked at most once a minute).
package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Check reports whether a dependency is usable.
type Check func(ctx context.Context) error

// Readiness timings: the checks run at most once per ReadyEvery, the probe
// answering from the last result in between (a failure included), and
// each check gets its own CheckTimeout, so the probe's timeout (5s in the
// chart) never cuts one short into a false failure. A pod that pinged
// PostgreSQL on every probe (every 10s) kept a server connection busy
// while idle; production's PostgreSQL has few to share.
const (
	ReadyEvery   = time.Minute
	CheckTimeout = 3 * time.Second
)

// Ops is the operations HTTP server.
type Ops struct {
	addr   string
	mu     sync.RWMutex
	checks map[string]Check
	now    func() time.Time

	readyMu sync.Mutex
	readyAt time.Time
	readyOK bool
	readyIs string // the failure, when not ready
}

func NewOps(addr string) *Ops { return &Ops{addr: addr, checks: map[string]Check{}, now: time.Now} }

// AddCheck registers a readiness check.
func (o *Ops) AddCheck(name string, c Check) {
	o.mu.Lock()
	o.checks[name] = c
	o.mu.Unlock()
}

func (o *Ops) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ok, why := o.ready(); !ok {
			http.Error(w, why, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// ready returns the last readiness result, running the checks again when
// it is older than ReadyEvery. Checks run at once, each with its own
// timeout and not the probe's, so they all finish within CheckTimeout.
func (o *Ops) ready() (bool, string) {
	o.readyMu.Lock()
	defer o.readyMu.Unlock()
	now := o.now()
	if !o.readyAt.IsZero() && now.Sub(o.readyAt) < ReadyEvery {
		return o.readyOK, o.readyIs
	}
	o.mu.RLock()
	checks := make(map[string]Check, len(o.checks))
	for name, c := range o.checks {
		checks[name] = c
	}
	o.mu.RUnlock()
	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(checks))
	for name, c := range checks {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), CheckTimeout)
			defer cancel()
			results <- result{name, c(ctx)}
		}()
	}
	o.readyOK, o.readyIs = true, ""
	for range checks {
		if r := <-results; r.err != nil && o.readyOK {
			o.readyOK, o.readyIs = false, r.name+": "+r.err.Error()
		}
	}
	o.readyAt = now
	return o.readyOK, o.readyIs
}

// Run serves until ctx is done.
func (o *Ops) Run(ctx context.Context) error {
	return Serve(ctx, o.addr, o.Handler())
}

// Option configures the server Serve runs.
type Option func(*http.Server)

// WithReadTimeout bounds reading a whole request, body included.
func WithReadTimeout(d time.Duration) Option {
	return func(s *http.Server) {
		if d > 0 {
			s.ReadTimeout = d
		}
	}
}

// Serve runs an HTTP server and shuts it down gracefully when ctx is done.
// Idle keep-alive connections are closed after 2 minutes, so clients that
// hold connections open (authenticated or not) can't exhaust descriptors.
func Serve(ctx context.Context, addr string, h http.Handler, opts ...Option) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	for _, o := range opts {
		o(srv)
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("http listening", "addr", addr)
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
