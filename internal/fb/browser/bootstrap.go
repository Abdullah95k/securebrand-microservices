package browser

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Errors of a capped fallback.
var (
	ErrCapped = errors.New("browser: share cap reached")
	ErrBusy   = errors.New("browser: all browsers busy")
)

// Bootstrapper is the session.BrowserFallback: it renders the page through
// the identity's own exit and fingerprint, copies the cookies into the
// identity's transport and installs the tokens found in the rendered DOM.
// At most MaxShare of HTML loads may use it (a credit that every observed
// load adds to, capped at a small burst) and MaxConcurrent run at once; a
// capped call fails at once rather than holding a crawl slot.
type Bootstrapper struct {
	Binary         string
	AcceptLanguage string
	Timeout        time.Duration
	MaxShare       float64
	NoSandbox      bool

	sem    chan struct{}
	mu     sync.Mutex
	credit float64
	burst  float64
	render func(ctx context.Context, binary string, opts Options) (*Result, error)
}

// NewBootstrapper returns a fallback for cfg, or an error when no browser
// binary is found.
func NewBootstrapper(cfg config.Browser, acceptLanguage string) (*Bootstrapper, error) {
	bin, err := FindBinary(cfg.Binary)
	if err != nil {
		return nil, err
	}
	n := max(cfg.MaxConcurrent, 1)
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	// The burst lets the first pages of a fresh pod through; the credit
	// then grows by MaxShare per HTML load.
	return &Bootstrapper{Binary: bin, AcceptLanguage: acceptLanguage, Timeout: timeout, MaxShare: cfg.MaxShare, NoSandbox: cfg.NoSandbox,
		sem: make(chan struct{}, n), credit: float64(n), burst: float64(n), render: Render}, nil
}

// Observe counts one HTTP page load.
func (b *Bootstrapper) Observe() {
	b.mu.Lock()
	b.credit = min(b.credit+b.MaxShare, b.burst)
	b.mu.Unlock()
}

func (b *Bootstrapper) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.credit < 1-1e-9 { // MaxShare sums are not exact in floating point
		return false
	}
	b.credit--
	return true
}

// Bootstrap renders rawURL for id.
func (b *Bootstrapper) Bootstrap(ctx context.Context, id *session.Identity, rawURL string) (*session.PageLoad, error) {
	if !b.take() {
		metrics.Bootstraps.WithLabelValues("browser", "capped").Inc()
		return nil, ErrCapped
	}
	select {
	case b.sem <- struct{}{}:
	default:
		metrics.Bootstraps.WithLabelValues("browser", "busy").Inc()
		return nil, ErrBusy
	}
	defer func() { <-b.sem }()
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	opts := Options{URL: rawURL, UserAgent: id.FP.UserAgent, AcceptLanguage: b.AcceptLanguage,
		Cookies: id.Doer.Cookies(id.Base), Capture: func(string) bool { return false }, Idle: 300 * time.Millisecond,
		NoSandbox: b.NoSandbox}
	if id.Endpoint.URL != "" {
		p, err := url.Parse(id.Endpoint.URL)
		if err != nil {
			return nil, fmt.Errorf("browser: proxy: %w", err)
		}
		opts.Proxy = p
	}
	res, err := b.render(ctx, b.Binary, opts)
	if err != nil {
		metrics.Bootstraps.WithLabelValues("browser", "error").Inc()
		return nil, err
	}
	for _, c := range res.Cookies {
		id.Doer.SetCookie(id.Base, c.Name, c.Value)
	}
	tok := htmlx.ExtractTokens(res.HTML)
	if tok.JSDatr != "" && id.Doer.Cookie(id.Base, "datr") == "" {
		id.Doer.SetCookie(id.Base, "datr", tok.JSDatr)
	}
	pl := &session.PageLoad{URL: res.URL, Status: res.Status, HTML: res.HTML, Class: detect.OK, Bytes: int64(len(res.HTML))}
	if !tok.Valid() {
		metrics.Bootstraps.WithLabelValues("browser", "no_tokens").Inc()
		session.Outcome(id, detect.SoftError)
		pl.Class, pl.Reason = detect.SoftError, "no tokens after a browser render"
		return pl, errors.New(pl.Reason)
	}
	id.SetTokens(tok)
	session.Outcome(id, detect.OK)
	metrics.Bootstraps.WithLabelValues("browser", "ok").Inc()
	return pl, nil
}

// Capture renders rawURL through id's exit and fingerprint, scrolling
// twice, and returns the GraphQL requests the page made. It shares the
// concurrency limit with bootstraps but not the share cap: callers use it
// rarely (doc_id discovery).
func (b *Bootstrapper) Capture(ctx context.Context, id *session.Identity, rawURL string) ([]Exchange, error) {
	select {
	case b.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-b.sem }()
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	opts := Options{URL: rawURL, UserAgent: id.FP.UserAgent, AcceptLanguage: b.AcceptLanguage,
		Cookies: id.Doer.Cookies(id.Base), Scrolls: 2, NoSandbox: b.NoSandbox}
	if id.Endpoint.URL != "" {
		p, err := url.Parse(id.Endpoint.URL)
		if err != nil {
			return nil, fmt.Errorf("browser: proxy: %w", err)
		}
		opts.Proxy = p
	}
	res, err := b.render(ctx, b.Binary, opts)
	if err != nil {
		return nil, err
	}
	return res.Captured, nil
}
