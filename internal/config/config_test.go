package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadLayers(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.yaml")
	b := filepath.Join(dir, "b.yaml")
	_ = os.WriteFile(a, []byte("crawler:\n  yield_pages: 7\nfb:\n  base_url: http://a\n"), 0o644)
	_ = os.WriteFile(b, []byte("fb:\n  base_url: http://b\n  proxy:\n    tiers:\n      - {name: isp, kind: static_list, endpoints: [\"http://u:p@1.2.3.4:1\"]}\n"), 0o644)
	t.Setenv("FBS_KAFKA__BROKERS", "k1:9092,k2:9092")
	t.Setenv("FBS_MEDIA__TTL", "48h")
	c, err := Load(a + "," + b)
	if err != nil {
		t.Fatal(err)
	}
	if c.Crawler.YieldPages != 7 || c.FB.BaseURL != "http://b" || len(c.FB.Proxy.Tiers) != 1 {
		t.Fatalf("layering wrong: %+v %+v", c.Crawler, c.FB)
	}
	if len(c.Kafka.Brokers) != 2 || c.Media.TTL != 48*time.Hour {
		t.Fatalf("env override wrong: %v %v", c.Kafka.Brokers, c.Media.TTL)
	}
	// Defaults survive.
	if c.Limits.MaxAgeDays != 365 || c.Scheduler.FinalizerInterval != 2*time.Second {
		t.Fatalf("defaults lost: %+v", c.Limits)
	}
}

func TestValidate(t *testing.T) {
	c, _ := Load("")
	c.Service.Env = "prod"
	if err := c.Validate(); err == nil {
		t.Fatal("prod with the dev HMAC key must be rejected")
	}
}
