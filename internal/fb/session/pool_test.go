package session

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/configs"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/breaker"
)

func TestBreakerRetiresIdleIdentitiesAndFeedsOutcomes(t *testing.T) {
	proxies, err := proxy.New(config.Proxy{
		Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
			Endpoints: []string{"http://203.0.113.10:8000", "http://198.51.100.10:8000"}}},
		Breaker: breaker.Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 4}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a, err := pool.Acquire(ctx, "isp")
	if err != nil || a.Endpoint.Bucket != "isp|203.0.113.0/24" {
		t.Fatalf("%+v %v", a.Endpoint, err)
	}
	pool.Release(a)

	// Outcomes recorded on an identity reach its bucket's breaker.
	b, _ := pool.Acquire(ctx, "isp")
	if b != a {
		t.Fatal("the idle identity is reused while its bucket is closed")
	}
	Outcome(b, detect.Transport)
	Outcome(b, detect.Transport)
	if !proxies.IsOpen(a.Endpoint) {
		t.Fatal("two transport failures of two must open the bucket")
	}
	pool.Release(b)

	c, err := pool.Acquire(ctx, "isp")
	if err != nil || c == a || c.Endpoint.Bucket != "isp|198.51.100.0/24" {
		t.Fatalf("want a fresh identity on the other /24: %+v %v", c.Endpoint, err)
	}
	if pool.live["isp"] != 1 {
		t.Fatalf("the identity on the open bucket must be retired: live=%d", pool.live["isp"])
	}
}

// An exit that stopped answering retires its identity; failures broken up
// by answers do not.
func TestTransportFailuresRetireAnIdentity(t *testing.T) {
	id := newIdentity("x", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, nil, 1, 1, 0)
	for range 20 {
		for range MaxTransportFailures - 1 {
			if retire, _ := id.Record(detect.Transport); retire {
				t.Fatal("retired on failures broken up by answers")
			}
		}
		id.Record(detect.OK)
	}
	for i := 1; i <= MaxTransportFailures; i++ {
		retire, reason := id.Record(detect.Transport)
		if retire != (i == MaxTransportFailures) || (retire && reason != "transport") {
			t.Fatalf("failure %d: retire=%v reason=%q", i, retire, reason)
		}
	}
	if id.RetireReason() != "transport" {
		t.Fatalf("reason %q", id.RetireReason())
	}
}

// doer fails every request with the error fail returns.
type doer struct {
	transport.Doer
	fail func() error
}

func (d doer) Do(context.Context, *transport.Request) (*transport.Response, error) {
	return nil, d.fail()
}

// A load that got no response is marked ErrTransport and counts against the
// exit; one cancelled by its caller does not.
func TestCancelledLoadsDoNotRetireAnIdentity(t *testing.T) {
	base, _ := url.Parse("https://www.facebook.com")
	l := &Loader{Rules: detect.DefaultRules()}
	cancelled := newIdentity("c", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, base, 1000, 10, 0)
	dead := newIdentity("d", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, base, 1000, 10, 0)
	std := transport.NewStd(time.Second)
	for range MaxTransportFailures {
		ctx, cancel := context.WithCancel(context.Background())
		cancelled.Doer = doer{Doer: std, fail: func() error { cancel(); return ctx.Err() }}
		if _, err := l.Load(ctx, cancelled, "https://www.facebook.com/x"); !errors.Is(err, ErrTransport) {
			t.Fatalf("cancelled load: %v", err)
		}
		dead.Doer = doer{Doer: std, fail: func() error { return errors.New("awaiting headers") }}
		if _, err := l.Load(context.Background(), dead, "https://www.facebook.com/x"); !errors.Is(err, ErrTransport) {
			t.Fatalf("dead load: %v", err)
		}
	}
	if r := cancelled.RetireReason(); r != "" {
		t.Fatalf("cancelled loads retired the identity: %q", r)
	}
	if r := dead.RetireReason(); r != "transport" {
		t.Fatalf("dead exit: %q", r)
	}
}

// serves answers every request with a 200 and body.
type serves struct {
	transport.Doer
	body string
}

func (s serves) Do(context.Context, *transport.Request) (*transport.Response, error) {
	return &transport.Response{Status: 200, Header: http.Header{"Content-Type": {"text/html"}}, Body: []byte(s.body)}, nil
}

// A page that names nothing (restricted to logged-in viewers, or removed,
// whatever else it holds) is unavailable, marked as such, and its session
// tokens are kept; a public page is left as it was.
func TestLoadPagesThatNameNothing(t *testing.T) {
	const tokens = `<script>{"define":[["LSD",[],{"token":"AVq"},323],["SiteData",[],{"hsi":"7431000000000000001"},317]]}</script>`
	base, _ := url.Parse("https://www.facebook.com")
	l := &Loader{Rules: detect.DefaultRules()}
	for name, c := range map[string]struct {
		body         string
		want         detect.Class
		bootstrapped bool
	}{
		"restricted":  {`<html><title>Facebook</title>` + tokens + `</html>`, detect.ContentUnavailable, true},
		"escaped ids": {tokens + `<script>require("x","{\"id\":\"100064000000777\"}")</script>`, detect.OK, true},
		"public page": {tokens + `<script>{"user":{"id":"100064000000777"}}</script>`, detect.OK, true},
		"login form":  {`<form id="login_form"></form>` + tokens, detect.ContentUnavailable, true},
	} {
		id := newIdentity(name, proxy.Endpoint{Tier: "isp"}, Fingerprint{}, serves{Doer: transport.NewStd(time.Second), body: c.body}, base, 1000, 10, 0)
		pl, err := l.Load(context.Background(), id, "https://www.facebook.com/x")
		if err != nil || pl.Class != c.want || pl.NamesNothing != (c.want == detect.ContentUnavailable) || id.Bootstrapped() != c.bootstrapped {
			t.Errorf("%s: class %v (%v), bootstrapped %v, err %v", name, pl.Class, pl.Reason, id.Bootstrapped(), err)
		}
	}
}

// Walls on page loads pause the exit's page loads but leave its breaker
// alone, so identities that already have tokens keep serving GraphQL there;
// GraphQL blocks still open it.
func TestPageLoadWallsLeaveTheExitServing(t *testing.T) {
	proxies, err := proxy.New(config.Proxy{
		Tiers:     []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://isp.proxy.example:10001"}}},
		Breaker:   breaker.Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Hour},
		PageLoads: config.PageLoads{Interval: time.Minute, Backoff: time.Hour, MaxBackoff: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 4}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "isp")
	if err != nil {
		t.Fatal(err)
	}
	if !id.TakeReservation() || id.TakeReservation() {
		t.Fatal("a new identity holds one reserved page load")
	}
	for range 5 {
		PageOutcome(id, detect.LoginWall)
	}
	if proxies.IsOpen(id.Endpoint) {
		t.Fatal("page-load walls opened the exit")
	}
	if proxies.TakePageLoad(id.Endpoint) {
		t.Fatal("a walled exit must back off its page loads")
	}
	Outcome(id, detect.RateLimited)
	Outcome(id, detect.RateLimited)
	if !proxies.IsOpen(id.Endpoint) {
		t.Fatal("GraphQL blocks must open the exit")
	}
}
