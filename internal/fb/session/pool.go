package session

import (
	"context"
	"errors"
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
	// Store, when set, keeps sessions across a restart (Save, Resume).
	Store Store
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
	id.markRetired(reason)
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
	id.observe = p.observer(ep)
	return id, nil
}

// observer reports an identity's outcomes on ep to its exit's breaker or,
// for page loads, its page-load gate.
func (p *Pool) observer(ep proxy.Endpoint) func(detect.Class, bool) {
	return func(c detect.Class, pageLoad bool) {
		if pageLoad {
			p.proxies.ObservePageLoad(ep, c)
		} else {
			p.proxies.Observe(ep, c)
		}
	}
}

// Save hands the idle identities' sessions (tokens and cookies) and the
// exits' page-load pacing to the store, for the next crawler to resume, and
// retires the identities. Call it once the crawler has stopped using the
// pool. Identities still in use, without tokens or on a gateway are not
// saved. It returns how many sessions were saved.
func (p *Pool) Save(ctx context.Context) (int, error) {
	if p.cfg.Store == nil {
		return 0, nil
	}
	p.mu.Lock()
	var ids []*Identity
	for tier, list := range p.idle {
		ids = append(ids, list...)
		p.live[tier] -= len(list)
		delete(p.idle, tier)
	}
	p.mu.Unlock()
	var sessions []Saved
	for _, id := range ids {
		if s, ok := id.saved(); ok {
			sessions = append(sessions, s)
		}
		id.Doer.Close()
	}
	if err := p.cfg.Store.Save(ctx, sessions, p.proxies.PageGateState()); err != nil {
		return 0, err
	}
	return len(sessions), nil
}

// Resume restores what the last crawler saved when it stopped (Save): the
// exits' page-load pauses, and its sessions (ResumeSessions). Call it once,
// at start.
func (p *Pool) Resume(ctx context.Context) (int, error) {
	if p.cfg.Store == nil {
		return 0, nil
	}
	exits, err := p.cfg.Store.TakeExits(ctx)
	p.proxies.RestorePageGate(exits)
	n, serr := p.ResumeSessions(ctx)
	return n, errors.Join(err, serr)
}

// ResumeSessions takes the sessions crawlers saved when they stopped, which
// become idle identities with their tokens and cookies, so the tasks they
// serve need no page load. Sessions it can't rebuild are dropped: an exit
// no longer configured or cooling down, another fingerprint profile, or a
// tier at its cap. It returns how many sessions resumed.
func (p *Pool) ResumeSessions(ctx context.Context) (int, error) {
	if p.cfg.Store == nil {
		return 0, nil
	}
	saved, err := p.cfg.Store.TakeSessions(ctx)
	n := 0
	for _, s := range saved {
		id := p.rebuild(s)
		if id == nil {
			continue
		}
		p.mu.Lock()
		if p.live[s.Tier] >= p.cfg.MaxPerTier {
			p.mu.Unlock()
			id.Doer.Close()
			continue
		}
		p.live[s.Tier]++
		p.idle[s.Tier] = append(p.idle[s.Tier], id)
		metrics.IdentityActive.WithLabelValues(s.Tier).Set(float64(p.live[s.Tier]))
		p.cond.Broadcast()
		p.mu.Unlock()
		metrics.Bootstraps.WithLabelValues("resumed", "ok").Inc()
		n++
	}
	if len(saved) > n {
		metrics.Bootstraps.WithLabelValues("resumed", "dropped").Add(float64(len(saved) - n))
	}
	return n, err
}

// RunResume resumes, every interval, the sessions of crawlers that stopped
// after this one started (a rolling update starts the new pod first).
func (p *Pool) RunResume(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if n, err := p.ResumeSessions(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("session: saved sessions not resumed", "resumed", n, "err", err)
			} else if n > 0 {
				slog.Info("session: saved sessions resumed", "sessions", n)
			}
		}
	}
}

// rebuild makes a saved session an identity again, on its exit, or returns
// nil when it can't.
func (p *Pool) rebuild(s Saved) *Identity {
	fp := p.fps[p.cfg.Profile]
	if s.Profile != fp.Profile || !s.Tokens.Valid() {
		return nil
	}
	ep, ok := p.proxies.Resume(s.Tier, s.Exit)
	if !ok {
		return nil
	}
	d, err := p.newDoer(transport.Options{Profile: fp.Profile, ProxyURL: ep.URL, Timeout: p.cfg.Timeout})
	if err != nil {
		return nil
	}
	for name, v := range s.Cookies {
		d.SetCookie(p.base, name, v)
	}
	id := newIdentity(s.ID, ep, fp, d, p.base, p.cfg.IdentityRPS, p.cfg.IdentityBurst, s.MaxRequests)
	id.tokens, id.bootstrapped = s.Tokens, s.Bootstrapped
	id.sessionID, id.requests = s.SessionID, s.Requests
	id.req.Store(s.Req)
	id.observe = p.observer(ep)
	return id
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
	if retire, reason := id.record(c, pageLoad); retire {
		slog.Debug("session: identity marked for retirement", "identity", id.ID, "reason", reason, "class", c.String())
	}
}
