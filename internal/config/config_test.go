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

// The author key must be a real secret wherever authors are pseudonymised
// outside dev: not empty, not the dev key, not short. In full mode (the
// owner's D-003: author ids and names are ordinary data) it hides nothing.
func TestValidate(t *testing.T) {
	for name, tc := range map[string]struct {
		env, mode, key string
		ok             bool
	}{
		"prod, full, dev key":            {"prod", "full", "dev-only-change-me", true},
		"prod, pseudonymised":            {"prod", "pseudonymized", "dev-only-change-me", false},
		"prod, pseudonymised, short key": {"prod", "pseudonymized", "0123456789", false},
		"prod, pseudonymised, real key":  {"prod", "pseudonymized", "0123456789abcdef0123456789abcdef", true},
		"pilot, pseudonymised":           {"pilot", "pseudonymized", "dev-only-change-me", false},
		"pilot, full names stored":       {"pilot", "full", "dev-only-change-me", true},
		"dev":                            {"dev", "pseudonymized", "dev-only-change-me", true},
	} {
		c, _ := Load("")
		c.Service.Env, c.Privacy.PIIMode, c.Privacy.AuthorHMACKey = tc.env, tc.mode, tc.key
		if err := c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestSchedulingDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	s := c.Scheduler
	if len(s.Tiers) != 4 || s.Tiers[0].Name != "T0" || s.Tiers[0].Interval != 3*time.Hour || s.Tiers[2].MinPostsPerDay < 0.14 {
		t.Fatalf("tiers: %+v", s.Tiers)
	}
	if s.DormantAfter != 90*24*time.Hour || s.RateWindow != 14*24*time.Hour {
		t.Fatalf("dormancy: %+v", s)
	}
	pr := s.PostRefresh
	if !pr.Enabled || len(pr.Windows) != 3 || pr.Windows[2].MaxAge != 30*24*time.Hour || len(pr.Windows[2].Tiers) != 2 ||
		len(pr.RecrawlAt) != 2 || pr.RecrawlMinComments != 20 {
		t.Fatalf("post refresh: %+v", pr)
	}
	if !s.Budget.Enabled || s.Budget.Stretch != 2 || len(s.Budget.StretchTiers) != 3 {
		t.Fatalf("budget: %+v", s.Budget)
	}
	c.Scheduler.Tiers[1].MinPostsPerDay = 50
	if err := c.Validate(); err == nil {
		t.Fatal("unordered tiers must fail validation")
	}
}

// Discovery's defaults are off and valid when turned on; a window shorter
// than the interval would leave gaps between searches.
func TestDiscoveryConfig(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	d := c.Discovery
	if d.Enabled || d.Interval != time.Hour || d.Window != 6*time.Hour || d.MaxNewPerDay != 200 || !d.IncludeProfiles || d.Concurrency != 2 {
		t.Fatalf("defaults: %+v", d)
	}
	c.Discovery.Enabled = true
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Discovery){
		"window shorter than interval": func(d *Discovery) { d.Window = 30 * time.Minute },
		"no interval":                  func(d *Discovery) { d.Interval = 0 },
		"no concurrency":               func(d *Discovery) { d.Concurrency = 0 },
		"negative cap":                 func(d *Discovery) { d.MaxNewPerDay = -1 },
	} {
		c, _ := Load("")
		c.Discovery.Enabled = true
		mutate(&c.Discovery)
		if c.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// Mentions are off by default, with the agreed defaults, valid when
// turned on; the caps' day starts in Iraq time (UTC+3).
func TestMentionsConfig(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	m := c.Mentions
	if m.Enabled || !m.IncludeCommentAuthors || m.MaxChecksPerDay != 300 || m.MaxNewPerDay != 50 || m.TimeZone != "Asia/Baghdad" ||
		m.RecheckAfter != 4380*time.Hour || m.RetryBackoff != time.Hour || m.RetryMaxBackoff != 168*time.Hour || m.Concurrency != 2 ||
		m.CandidateWeight != 2 || m.RetentionDays != 0 || m.MaxFailures != 10 {
		t.Fatalf("defaults: %+v", m)
	}
	at := time.Date(2026, 11, 2, 22, 30, 0, 0, time.UTC) // 01:30 the next day in Iraq
	if _, off := at.In(m.Location()).Zone(); off != 3*3600 || at.In(m.Location()).Day() != 3 {
		t.Fatalf("Iraq time: %v", at.In(m.Location()))
	}
	c.Mentions.Enabled = true
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Mentions){
		"negative check cap":        func(m *Mentions) { m.MaxChecksPerDay = -1 },
		"negative add cap":          func(m *Mentions) { m.MaxNewPerDay = -1 },
		"no concurrency":            func(m *Mentions) { m.Concurrency = 0 },
		"no recheck":                func(m *Mentions) { m.RecheckAfter = 0 },
		"no backoff":                func(m *Mentions) { m.RetryBackoff = 0 },
		"max backoff below backoff": func(m *Mentions) { m.RetryMaxBackoff = time.Minute },
		"unknown zone":              func(m *Mentions) { m.TimeZone = "Mars/Olympus" },
		"no zone":                   func(m *Mentions) { m.TimeZone = "" },
		"negative retention":        func(m *Mentions) { m.RetentionDays = -1 },
		"negative max failures":     func(m *Mentions) { m.MaxFailures = -1 },
	} {
		c, _ := Load("")
		c.Mentions.Enabled = true
		mutate(&c.Mentions)
		if c.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// Every retention default is 0, which keeps that data for good (D-004).
func TestDefaultsKeepEverything(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	r := c.Privacy.Retention
	for name, days := range map[string]int{
		"privacy.retention.comments_days": r.CommentsDays, "privacy.retention.authors_inactive_days": r.AuthorsInactiveDays,
		"privacy.retention.metrics_days": r.MetricsDays, "privacy.retention.raw_payload_days": r.RawPayloadDays,
		"privacy.retention.media_rows_days": r.MediaRowsDays, "privacy.retention.jobs_days": r.JobsDays,
		"discovery.mentions_days": c.Discovery.MentionsDays, "mentions.retention_days": c.Mentions.RetentionDays,
	} {
		if days != 0 {
			t.Errorf("%s defaults to %d; it must be 0 (keep for good)", name, days)
		}
	}
}
