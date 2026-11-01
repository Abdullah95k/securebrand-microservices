// Package proxy hands out proxy endpoints by tier. Only Facebook HTML and
// GraphQL traffic goes through these; media and static files use direct
// datacenter egress.
package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Endpoint is one proxy exit to use for an identity's lifetime.
type Endpoint struct {
	Tier      string
	URL       string // empty for direct
	SessionID string
	Country   string
	ExpiresAt time.Time // zero = no sticky expiry
	// Bucket groups exits that Facebook likely treats alike: the /24 of a
	// static endpoint, or a gateway and country. Breakers work per bucket.
	Bucket string
}

// Redacted returns the URL without credentials, for logs.
func (e Endpoint) Redacted() string {
	if e.URL == "" {
		return "direct"
	}
	u, err := url.Parse(e.URL)
	if err != nil {
		return "invalid"
	}
	u.User = nil
	return u.String()
}

// Tier produces endpoints. Next returns the next endpoint for which allow
// is true, or ErrTierOpen.
type Tier interface {
	Name() string
	Next(allow func(bucket string) bool) (Endpoint, error)
	// Buckets lists the tier's buckets.
	Buckets() []string
}

// ErrTierOpen means every bucket of the tier has an open breaker.
var ErrTierOpen = errors.New("proxy: every exit of the tier is cooling down")

// Pool routes query classes to tiers and keeps a circuit breaker per exit
// bucket: one that is blocked too often stops getting new identities.
type Pool struct {
	tiers    map[string]Tier
	routing  map[string][]string
	breakers *breaker.Set
	pages    *pageGate
}

// New builds a pool from config. A "direct" tier always exists.
func New(cfg config.Proxy) (*Pool, error) {
	p := &Pool{tiers: map[string]Tier{"direct": direct{}}, routing: cfg.Routing, breakers: breaker.New(cfg.Breaker),
		pages: newPageGate(cfg.PageLoads)}
	p.breakers.OnChange = func(bucket string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("proxy:" + bucket).Set(float64(to))
		slog.Info("proxy: breaker", "bucket", bucket, "from", from.String(), "to", to.String())
	}
	for _, t := range cfg.Tiers {
		switch t.Kind {
		case "static_list":
			if len(t.Endpoints) == 0 {
				return nil, fmt.Errorf("proxy: tier %s has no endpoints", t.Name)
			}
			sl := &staticList{name: t.Name, endpoints: t.Endpoints, country: t.Country}
			for _, e := range t.Endpoints {
				b := staticBucket(t.Name, e)
				sl.buckets = append(sl.buckets, b)
				p.pages.gate(b)
			}
			p.tiers[t.Name] = sl
		case "gateway":
			if t.Gateway == "" {
				return nil, fmt.Errorf("proxy: tier %s has no gateway", t.Name)
			}
			if _, err := url.Parse(t.Gateway); err != nil {
				return nil, fmt.Errorf("proxy: tier %s: %w", t.Name, err)
			}
			p.tiers[t.Name] = &gateway{cfg: t}
		case "direct", "":
			p.tiers[t.Name] = direct{}
		default:
			return nil, fmt.Errorf("proxy: tier %s: unknown kind %q", t.Name, t.Kind)
		}
	}
	if len(p.routing) == 0 {
		p.routing = map[string][]string{"default": {"direct"}}
	}
	for class, tiers := range p.routing {
		for _, t := range tiers {
			if _, ok := p.tiers[t]; !ok {
				return nil, fmt.Errorf("proxy: routing %s references unknown tier %s", class, t)
			}
		}
	}
	return p, nil
}

// TiersFor returns the tier names to try for a query class, cheapest first.
func (p *Pool) TiersFor(class string) []string {
	if t, ok := p.routing[class]; ok && len(t) > 0 {
		return t
	}
	return p.routing["default"]
}

// Next returns a fresh endpoint from tier for a new identity, skipping
// buckets whose breaker is open or whose page-load gate is closed: a new
// identity starts with a page load, which Next reserves. In a half-open
// bucket the endpoint is the probe. When an exit is only waiting out its
// page-load interval the error is ErrExitsPaced (a short wait); when every
// exit is open or backing off after walls, ErrTierOpen.
func (p *Pool) Next(tier string) (Endpoint, error) {
	t, ok := p.tiers[tier]
	if !ok {
		return Endpoint{}, fmt.Errorf("proxy: unknown tier %s", tier)
	}
	paced, backoff := false, false
	ep, err := t.Next(func(b string) bool {
		if p.breakers.IsOpen(b) {
			return false
		}
		ok, why, restore := p.pages.take(b)
		if !ok {
			paced, backoff = paced || why == "paced", backoff || why == "backoff"
			return false
		}
		if !p.breakers.Allow(b) {
			restore()
			return false
		}
		return true
	})
	if errors.Is(err, ErrTierOpen) {
		switch {
		case paced:
			// An exit loads a page again within the interval.
			deferred(tier, "paced")
			return Endpoint{}, ErrExitsPaced
		case backoff:
			// Every waiting exit backs off after walls (15m and up): as
			// long a wait as an open breaker.
			deferred(tier, "backoff")
		}
	}
	return ep, err
}

// TakePageLoad reserves a page load on ep's exit for an identity that
// already has tokens (a profile read). It is false while the exit waits
// between page loads or backs off after a walled one; the caller skips the
// page load rather than risk walling an identity that serves GraphQL.
func (p *Pool) TakePageLoad(ep Endpoint) bool {
	if ep.Bucket == "" {
		return true
	}
	ok, why, _ := p.pages.take(ep.Bucket)
	if !ok {
		deferred(ep.Tier, why)
	}
	return ok
}

// Available reports whether tier has a bucket that is not cooling down.
func (p *Pool) Available(tier string) bool {
	t, ok := p.tiers[tier]
	if !ok {
		return false
	}
	if len(t.Buckets()) == 0 {
		return true // direct egress has no breaker
	}
	for _, b := range t.Buckets() {
		if !p.breakers.IsOpen(b) {
			return true
		}
	}
	return false
}

// IsOpen reports whether ep's bucket is cooling down.
func (p *Pool) IsOpen(ep Endpoint) bool { return ep.Bucket != "" && p.breakers.IsOpen(ep.Bucket) }

// Observe records a GraphQL response class for ep's bucket. Blocks and
// transport failures count against it.
func (p *Pool) Observe(ep Endpoint, c detect.Class) {
	if ep.Bucket == "" {
		return
	}
	p.breakers.Observe(ep.Bucket, c.Blocking() || c == detect.Transport)
}

// ObservePageLoad records a page load's class for ep's exit. A walled or
// blocked page load pauses the exit's page loads (its gate) but does not
// count against its breaker: Facebook walls page loads from an exit whose
// GraphQL still works, and an open breaker would retire every identity
// there. A transport failure means the exit itself is failing, so it
// counts against both.
func (p *Pool) ObservePageLoad(ep Endpoint, c detect.Class) {
	if ep.Bucket == "" {
		return
	}
	p.pages.observe(ep.Bucket, c.Blocking() || c == detect.Transport)
	if c == detect.Transport {
		p.breakers.Observe(ep.Bucket, true)
	}
}

// staticBucket is the /24 of an IPv4 endpoint (/48 for IPv6). An endpoint
// named by hostname is its host and port: providers that sell static IPs
// behind one hostname give each IP its own port, so one breaker per host
// would stop every IP when one of them is blocked.
// The name never carries credentials: it is logged and used as a metric
// label, so an endpoint that is not a URL (host:port:user:pass) is read as
// host and port, after any user:pass@.
func staticBucket(tier, raw string) string {
	var host, port string
	if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
		host, port = u.Hostname(), u.Port()
	} else {
		rest := raw
		if i := strings.LastIndex(rest, "@"); i >= 0 {
			rest = rest[i+1:]
		}
		host, rest, _ = strings.Cut(rest, ":")
		if p, _, _ := strings.Cut(rest, ":"); p != "" && strings.Trim(p, "0123456789") == "" {
			port = p
		}
	}
	ip := net.ParseIP(host)
	if ip == nil && port != "" {
		return tier + "|" + net.JoinHostPort(host, port)
	}
	if ip != nil {
		if v4 := ip.To4(); v4 != nil {
			host = fmt.Sprintf("%d.%d.%d.0/24", v4[0], v4[1], v4[2])
		} else {
			host = (&net.IPNet{IP: ip.Mask(net.CIDRMask(48, 128)), Mask: net.CIDRMask(48, 128)}).String()
		}
	}
	return tier + "|" + host
}

type direct struct{}

func (direct) Name() string                             { return "direct" }
func (direct) Buckets() []string                        { return nil }
func (direct) Next(func(string) bool) (Endpoint, error) { return Endpoint{Tier: "direct"}, nil }

type staticList struct {
	name      string
	endpoints []string
	buckets   []string // parallel to endpoints
	country   string
	next      atomic.Uint64
}

func (s *staticList) Name() string      { return s.name }
func (s *staticList) Buckets() []string { return s.buckets }

func (s *staticList) Next(allow func(string) bool) (Endpoint, error) {
	for range s.endpoints {
		i := (s.next.Add(1) - 1) % uint64(len(s.endpoints))
		if allow(s.buckets[i]) {
			return Endpoint{Tier: s.name, URL: s.endpoints[i], Country: s.country, Bucket: s.buckets[i]}, nil
		}
	}
	return Endpoint{}, ErrTierOpen
}

// gateway is a rotating residential gateway: the exit IP is chosen by the
// session id embedded in the proxy username and stays sticky for StickyTTL.
type gateway struct {
	cfg config.ProxyTier
}

func (g *gateway) Name() string { return g.cfg.Name }

func (g *gateway) bucket() string {
	host := g.cfg.Gateway
	if u, err := url.Parse(g.cfg.Gateway); err == nil && u.Host != "" {
		host = u.Host
	}
	return g.cfg.Name + "|" + host + "|" + g.cfg.Country
}

func (g *gateway) Buckets() []string { return []string{g.bucket()} }

func (g *gateway) Next(allow func(string) bool) (Endpoint, error) {
	if !allow(g.bucket()) {
		return Endpoint{}, ErrTierOpen
	}
	sess := randomSession()
	ttl := g.cfg.StickyTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	user := g.cfg.UsernameTemplate
	if user == "" {
		user = "{user}-session-{session}"
	}
	user = strings.NewReplacer(
		"{user}", g.cfg.Username,
		"{country}", g.cfg.Country,
		"{session}", sess,
		"{ttl_min}", strconv.Itoa(int(ttl.Minutes())),
	).Replace(user)
	u, _ := url.Parse(g.cfg.Gateway)
	u.User = url.UserPassword(user, g.cfg.Password)
	return Endpoint{
		Tier:      g.cfg.Name,
		URL:       u.String(),
		SessionID: sess,
		Country:   g.cfg.Country,
		ExpiresAt: time.Now().Add(ttl),
		Bucket:    g.bucket(),
	}, nil
}

func randomSession() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
