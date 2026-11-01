package session

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// PoolConfig configures identity creation.
type PoolConfig struct {
	BaseURL        string
	Profile        string // default fingerprint profile
	Timeout        time.Duration
	IdentityRPS    float64
	IdentityBurst  int
	MaxRequestsMin int
	MaxRequestsMax int
	// MaxPerTier caps live identities per tier; Acquire waits at the cap.
	MaxPerTier int
}

// DoerFactory builds a transport for an identity (swappable in tests).
type DoerFactory func(o transport.Options) (transport.Doer, error)

// Pool owns this pod's identities. Each identity is leased exclusively.
type Pool struct {
	cfg     PoolConfig
	base    *url.URL
	proxies *proxy.Pool
	fps     map[string]Fingerprint
	newDoer DoerFactory

	mu   sync.Mutex
	cond *sync.Cond
	idle map[string][]*Identity
	live map[string]int
}

func NewPool(cfg PoolConfig, proxies *proxy.Pool, fps map[string]Fingerprint, f DoerFactory) (*Pool, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("session: base url: %w", err)
	}
	if _, ok := fps[cfg.Profile]; !ok {
		return nil, fmt.Errorf("session: no fingerprint for profile %q", cfg.Profile)
	}
	if f == nil {
		f = transport.NewTLS
	}
	if cfg.MaxPerTier <= 0 {
		cfg.MaxPerTier = 512
	}
	p := &Pool{cfg: cfg, base: base, proxies: proxies, fps: fps, newDoer: f,
		idle: map[string][]*Identity{}, live: map[string]int{}}
	p.cond = sync.NewCond(&p.mu)
	return p, nil
}

// Base returns the Facebook base URL.
func (p *Pool) Base() *url.URL { return p.base }

// Acquire returns an identity on tier for exclusive use. Fresh identities
// are not bootstrapped yet; the caller bootstraps them on first use.
func (p *Pool) Acquire(ctx context.Context, tier string) (*Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		list := p.idle[tier]
		for i := len(list) - 1; i >= 0; i-- {
			id := list[i]
			if p.proxies.IsOpen(id.Endpoint) {
				// Its exit bucket is cooling down: retire rather than reuse.
				list = append(list[:i], list[i+1:]...)
				p.idle[tier] = list
				p.live[tier]--
				metrics.IdentityRetired.WithLabelValues(tier, "breaker").Inc()
				go id.Doer.Close()
				continue
			}
			if id.coolingDown() {
				continue
			}
			p.idle[tier] = append(list[:i], list[i+1:]...)
			return id, nil
		}
		if p.live[tier] < p.cfg.MaxPerTier {
			p.live[tier]++
			p.mu.Unlock()
			id, err := p.create(tier)
			p.mu.Lock()
			if err != nil {
				p.live[tier]--
				return nil, err
			}
			metrics.IdentityActive.WithLabelValues(tier).Set(float64(p.live[tier]))
			return id, nil
		}
		// At the cap: wait for a release (or a cooldown to end).
		stop := context.AfterFunc(ctx, func() { p.mu.Lock(); p.cond.Broadcast(); p.mu.Unlock() })
		timer := time.AfterFunc(time.Second, func() { p.mu.Lock(); p.cond.Broadcast(); p.mu.Unlock() })
		p.cond.Wait()
		stop()
		timer.Stop()
	}
}

// Release returns id to the pool, or retires it.
func (p *Pool) Release(id *Identity) {
	tier := id.Endpoint.Tier
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.cond.Broadcast()
	if reason := id.RetireReason(); reason != "" {
		p.live[tier]--
		metrics.IdentityActive.WithLabelValues(tier).Set(float64(p.live[tier]))
		metrics.IdentityRetired.WithLabelValues(tier, reason).Inc()
		slog.Debug("session: identity retired", "identity", id.ID, "tier", tier, "reason", reason)
		go id.Doer.Close()
		return
	}
	p.idle[tier] = append(p.idle[tier], id)
}

// Retire marks id as unusable and releases it.
func (p *Pool) Retire(id *Identity, reason string) {
	id.mu.Lock()
	if id.retired == "" {
		id.retired = reason
	}
	id.mu.Unlock()
	p.Release(id)
}

func (p *Pool) create(tier string) (*Identity, error) {
	ep, err := p.proxies.Next(tier)
	if err != nil {
		return nil, err
	}
	fp := p.fps[p.cfg.Profile]
	d, err := p.newDoer(transport.Options{Profile: fp.Profile, ProxyURL: ep.URL, Timeout: p.cfg.Timeout})
	if err != nil {
		return nil, err
	}
	maxReq := randRange(p.cfg.MaxRequestsMin, p.cfg.MaxRequestsMax)
	id := newIdentity(newID(), ep, fp, d, p.base, p.cfg.IdentityRPS, p.cfg.IdentityBurst, maxReq)
	id.reserved = true // proxies.Next reserved its first page load
	id.observe = func(c detect.Class, pageLoad bool) {
		if pageLoad {
			p.proxies.ObservePageLoad(ep, c)
		} else {
			p.proxies.Observe(ep, c)
		}
	}
	return id, nil
}

// Outcome records a GraphQL request's outcome on id and its exit's breaker.
func Outcome(id *Identity, c detect.Class) { outcome(id, c, false) }

// PageOutcome records a page load's outcome on id and its exit's page-load
// gate (not its breaker: see proxy.Pool.ObservePageLoad).
func PageOutcome(id *Identity, c detect.Class) { outcome(id, c, true) }

func outcome(id *Identity, c detect.Class, pageLoad bool) {
	if id.observe != nil {
		id.observe(c, pageLoad)
	}
	if retire, reason := id.Record(c); retire {
		slog.Debug("session: identity marked for retirement", "identity", id.ID, "reason", reason, "class", c.String())
	}
}
