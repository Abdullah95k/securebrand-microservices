package proxy

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/breaker"
)

func testPool(t *testing.T) *Pool {
	t.Helper()
	p, err := New(config.Proxy{
		Tiers: []config.ProxyTier{
			{Name: "isp", Kind: "static_list", Endpoints: []string{
				"http://u:p@203.0.113.10:8000", "http://u:p@203.0.113.11:8000", "http://u:p@198.51.100.7:8000"}},
			{Name: "res", Kind: "gateway", Gateway: "http://gw.example:7777", Username: "u", Password: "p", Country: "us"},
		},
		Routing: map[string][]string{"default": {"isp", "res"}},
		Breaker: breaker.Config{MinEvents: 4, OpenRate: 0.5, Cooldown: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func block(p *Pool, ep Endpoint, n int) {
	for i := 0; i < n; i++ {
		p.Observe(ep, detect.RateLimited)
	}
}

// Exits are named by their place in the list, buckets by the k-th /24 the
// list names (or the exit, for an endpoint named by hostname), gateways by
// country: no name, bucket or tag carries an address or a credential.
func TestBuckets(t *testing.T) {
	p, err := New(config.Proxy{
		Tiers: []config.ProxyTier{
			{Name: "isp", Kind: "static_list", Endpoints: []string{
				"http://user1:secret1@203.0.113.10:8000", "http://user1:secret1@203.0.113.11:8001",
				"http://user1:secret1@198.51.100.7:8000", "http://user1:secret1@isp.proxy.example:10002",
				"socks5://[2001:db8::1]:1080"}},
			{Name: "res", Kind: "gateway", Gateway: "http://gw.example:7777", Username: "u", Password: "p", Country: "us"},
		},
		Routing: map[string][]string{"default": {"isp"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	st := p.tiers["isp"].(*staticList)
	want := []struct{ exit, bucket string }{
		{"isp#1", "isp/net#1"}, {"isp#2", "isp/net#1"}, {"isp#3", "isp/net#2"}, {"isp#4", "isp#4"}, {"isp#5", "isp/net#3"},
	}
	tags := map[string]bool{}
	for i, w := range want {
		ep := st.endpoint(i)
		if ep.Exit != w.exit || ep.Bucket != w.bucket || len(ep.Tag) != 12 {
			t.Errorf("endpoint %d: %s %s %s, want %s %s", i+1, ep.Exit, ep.Bucket, ep.Tag, w.exit, w.bucket)
		}
		tags[ep.Tag] = true
		for _, s := range []string{ep.Exit, ep.Bucket, ep.Tag} {
			for _, leak := range []string{"203.0.113", "198.51", "2001:db8", "proxy.example", "user1", "secret1", ":80"} {
				if strings.Contains(s, leak) {
					t.Errorf("%q names %q", s, leak)
				}
			}
		}
	}
	if len(tags) != len(want) {
		t.Error("two endpoints share a tag")
	}
	ep, err := p.Next("res")
	if err != nil || ep.Bucket != "res@us" {
		t.Fatalf("%+v %v", ep, err)
	}
}

func TestStaticListSkipsOpenBuckets(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("isp")
	if ep.Bucket != "isp/net#1" {
		t.Fatalf("first endpoint: %+v", ep)
	}
	block(p, ep, 4)
	if !p.IsOpen(ep) || !p.Available("isp") {
		t.Fatal("one bucket open, the tier still has another")
	}
	for i := 0; i < 5; i++ {
		next, err := p.Next("isp")
		if err != nil || next.Bucket != "isp/net#2" {
			t.Fatalf("Next must skip the open /24: %+v %v", next, err)
		}
	}
	other, _ := p.Next("isp")
	block(p, other, 4)
	if p.Available("isp") {
		t.Fatal("every bucket open")
	}
	if _, err := p.Next("isp"); !errors.Is(err, ErrTierOpen) {
		t.Fatalf("want ErrTierOpen, got %v", err)
	}
}

func TestGatewayBreaker(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("res")
	for i := 0; i < 4; i++ {
		p.Observe(ep, detect.OK)
	}
	p.Observe(ep, detect.Transport)
	p.Observe(ep, detect.LoginWall)
	p.Observe(ep, detect.Checkpoint)
	if p.IsOpen(ep) {
		t.Fatal("3 of 7 is below 50%")
	}
	p.Observe(ep, detect.RateLimited)
	if !p.IsOpen(ep) || p.Available("res") {
		t.Fatal("4 of 8 opens the gateway bucket")
	}
	if _, err := p.Next("res"); !errors.Is(err, ErrTierOpen) {
		t.Fatalf("want ErrTierOpen, got %v", err)
	}
	// Direct never opens.
	d, err := p.Next("direct")
	if err != nil || d.Bucket != "" || !p.Available("direct") {
		t.Fatal(d, err)
	}
	block(p, d, 10)
	if p.IsOpen(d) {
		t.Fatal("direct has no breaker")
	}
}

func TestNonExitClassesCountAsSuccess(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("res")
	for _, c := range []detect.Class{detect.SchemaDrift, detect.RegistryStale, detect.SoftError, detect.ContentUnavailable,
		detect.SchemaDrift, detect.RegistryStale, detect.SoftError, detect.ContentUnavailable} {
		p.Observe(ep, c)
	}
	if p.IsOpen(ep) {
		t.Fatal("parser and registry problems are not the exit's fault")
	}
}

// Static IPs sold behind one hostname, one port each: blocking one IP stops
// only that port, and the others keep serving.
func TestPortsOfOneHostAreSeparateExits(t *testing.T) {
	var eps []string
	for port := 10001; port <= 10010; port++ {
		eps = append(eps, "http://u:p@isp.proxy.example:"+strconv.Itoa(port))
	}
	p, err := New(config.Proxy{Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: eps}},
		Routing: map[string][]string{"default": {"isp"}}, Breaker: breaker.Config{MinEvents: 4, OpenRate: 0.5, Cooldown: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := p.Next("isp")
	block(p, first, 4)
	if !p.IsOpen(first) || !p.Available("isp") {
		t.Fatal("one port blocked: the tier still has nine")
	}
	seen := map[string]bool{}
	for i := 0; i < 30; i++ {
		ep, err := p.Next("isp")
		if err != nil || ep.Bucket == first.Bucket {
			t.Fatalf("Next must skip the blocked port: %+v %v", ep, err)
		}
		seen[ep.Bucket] = true
	}
	if len(seen) != 9 {
		t.Fatalf("the other ports serve: %d", len(seen))
	}
}
