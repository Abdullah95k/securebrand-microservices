package session

import (
	"context"
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
