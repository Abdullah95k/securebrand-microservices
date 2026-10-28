// Package proxy hands out proxy endpoints by tier. Only Facebook HTML and
// GraphQL traffic goes through these; media and static files use direct
// datacenter egress.
package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
)

// Endpoint is one proxy exit to use for an identity's lifetime.
type Endpoint struct {
	Tier      string
	URL       string // empty for direct
	SessionID string
	Country   string
	ExpiresAt time.Time // zero = no sticky expiry
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

// Tier produces endpoints.
type Tier interface {
	Name() string
	Next() (Endpoint, error)
}

// Pool routes query classes to tiers.
type Pool struct {
	tiers   map[string]Tier
	routing map[string][]string
}

// New builds a pool from config. A "direct" tier always exists.
func New(cfg config.Proxy) (*Pool, error) {
	p := &Pool{tiers: map[string]Tier{"direct": direct{}}, routing: cfg.Routing}
	for _, t := range cfg.Tiers {
		switch t.Kind {
		case "static_list":
			if len(t.Endpoints) == 0 {
				return nil, fmt.Errorf("proxy: tier %s has no endpoints", t.Name)
			}
			p.tiers[t.Name] = &staticList{name: t.Name, endpoints: t.Endpoints, country: t.Country}
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

// Next returns a fresh endpoint from tier.
func (p *Pool) Next(tier string) (Endpoint, error) {
	t, ok := p.tiers[tier]
	if !ok {
		return Endpoint{}, fmt.Errorf("proxy: unknown tier %s", tier)
	}
	return t.Next()
}

type direct struct{}

func (direct) Name() string            { return "direct" }
func (direct) Next() (Endpoint, error) { return Endpoint{Tier: "direct"}, nil }

type staticList struct {
	name      string
	endpoints []string
	country   string
	next      atomic.Uint64
}

func (s *staticList) Name() string { return s.name }

func (s *staticList) Next() (Endpoint, error) {
	i := s.next.Add(1) - 1
	return Endpoint{Tier: s.name, URL: s.endpoints[i%uint64(len(s.endpoints))], Country: s.country}, nil
}

// gateway is a rotating residential gateway: the exit IP is chosen by the
// session id embedded in the proxy username and stays sticky for StickyTTL.
type gateway struct {
	cfg config.ProxyTier
}

func (g *gateway) Name() string { return g.cfg.Name }

func (g *gateway) Next() (Endpoint, error) {
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
	}, nil
}

func randomSession() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
