package stage

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
)

// Only a saved checkpoint that moved counts as progress.
func TestAdvanced(t *testing.T) {
	at := checkpoint.State{Cursor: "c1", Pages: 2}
	for _, tc := range []struct {
		after checkpoint.State
		want  bool
	}{
		{at, false},
		{checkpoint.State{Cursor: "c2", Pages: 3}, true},
		{checkpoint.State{Cursor: "c1", Pages: 3}, true},
		{checkpoint.State{Cursor: "c2", Pages: 2}, true},
	} {
		if got := advanced(at, tc.after); got != tc.want {
			t.Errorf("advanced(%+v, %+v) = %v", at, tc.after, got)
		}
	}
	if advanced(checkpoint.State{}, checkpoint.State{}) {
		t.Error("a stream without a checkpoint never advances")
	}
}

// Stalls (retries without progress) hit max_attempts; a task that keeps
// advancing still stops at the total cap.
func TestExhausted(t *testing.T) {
	w := &Worker{MaxAttempts: 5}
	for _, tc := range []struct {
		attempt, stalls uint32
		want            string
	}{
		{4, 4, ""},
		{5, 5, "max_attempts"},
		{30, 0, ""}, // advancing: retries go on
		{49, 4, ""}, // under both caps
		{50, 0, "max_total_attempts"},
	} {
		if got := w.exhausted(&taskv1.Task{Attempt: tc.attempt, Stalls: tc.stalls}); got != tc.want {
			t.Errorf("attempt %d stalls %d: %q, want %q", tc.attempt, tc.stalls, got, tc.want)
		}
	}
	w.MaxTotalAttempts = 8
	if got := w.exhausted(&taskv1.Task{Attempt: 8}); got != "max_total_attempts" {
		t.Errorf("configured total cap: %q", got)
	}
}

// A retried attempt adds what it cost to its job, not what it collected
// (the retry collects that again).
func TestCostCounters(t *testing.T) {
	got := costCounters(map[string]int64{"requests": 3, "wire_bytes": 9000, "blocked": 1, "posts": 10, "comments": 40, "drift": 0})
	if len(got) != 3 || got["requests"] != 3 || got["wire_bytes"] != 9000 || got["blocked"] != 1 {
		t.Fatalf("cost counters: %v", got)
	}
	if len(costCounters(nil)) != 0 {
		t.Fatal("nil counters")
	}
}

// Only the work after the last saved checkpoint is added on a retry: the
// saved counters resume into the retry and are reported when it finishes.
func TestUnsaved(t *testing.T) {
	got := unsaved(map[string]int64{"requests": 8, "wire_bytes": 9000, "blocked": 1}, map[string]int64{"requests": 6, "wire_bytes": 7000, "posts": 30})
	if len(got) != 3 || got["requests"] != 2 || got["wire_bytes"] != 2000 || got["blocked"] != 1 {
		t.Fatalf("unsaved: %v", got)
	}
	if len(unsaved(map[string]int64{"requests": 6}, map[string]int64{"requests": 6})) != 0 {
		t.Fatal("nothing new")
	}
}

func TestProfileFresh(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	cfg := &config.Config{}
	cfg.Crawler.ProfileRefresh = 24 * time.Hour
	d := &Deps{Cfg: cfg, Now: func() time.Time { return now }}
	at := func(ago time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(-ago)) }
	for name, c := range map[string]struct {
		sc   *taskv1.PageScope
		want bool
	}{
		"read an hour ago":  {&taskv1.PageScope{PageId: 7, ProfileSeenAt: at(time.Hour)}, true},
		"read two days ago": {&taskv1.PageScope{PageId: 7, ProfileSeenAt: at(48 * time.Hour)}, false},
		"never read":        {&taskv1.PageScope{PageId: 7}, false},
		"page id not known": {&taskv1.PageScope{ProfileSeenAt: at(time.Hour)}, false},
	} {
		if got := d.profileFresh(c.sc); got != c.want {
			t.Errorf("%s: %v", name, got)
		}
	}
	cfg.Crawler.ProfileRefresh = 0
	if d.profileFresh(&taskv1.PageScope{PageId: 7, ProfileSeenAt: at(time.Minute)}) {
		t.Error("profile_refresh 0 reads the HTML on every run")
	}
}

// A task that got no identity because every exit is cooling down waits 5
// minutes, and the wait does not count toward its stalls; other failures to
// get an identity retry soon and do count.
func TestNoIdentityWaitsForExits(t *testing.T) {
	tk := &taskv1.Task{TaskId: "t", Attempt: 2, Stalls: 3}
	o := noIdentity(tk, proxy.ErrTierOpen, map[string]int64{})
	if o.Result != Retry || o.Delay != "5m" || !o.Waiting || o.Next.Attempt != 3 {
		t.Fatalf("every exit cooling down: %+v", o)
	}
	if got := stallsAfter(tk.Stalls, false, o.Waiting); got != 3 {
		t.Fatalf("a wait counted as a stall: %d", got)
	}
	o = noIdentity(tk, errors.New("dial"), map[string]int64{})
	if o.Delay != "30s" || o.Waiting {
		t.Fatalf("other errors: %+v", o)
	}
	if got := stallsAfter(tk.Stalls, false, o.Waiting); got != 4 {
		t.Fatalf("a stall not counted: %d", got)
	}
	o = noIdentity(tk, proxy.ErrExitsPaced, map[string]int64{})
	if o.Delay != "30s" || !o.Waiting {
		t.Fatalf("exits pacing their page loads: %+v", o)
	}
	if got := stallsAfter(tk.Stalls, true, false); got != 0 {
		t.Fatalf("progress resets the stalls: %d", got)
	}
}

// The profile is read on a page task's first segment when it is stale. An
// identity that already has tokens reads it only if its exit can load a
// page now; otherwise the read is deferred, never risking a wall on an
// identity that serves GraphQL. An identity without tokens loads the page
// anyway, so the gate does not apply.
func TestReadProfile(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	cfg := &config.Config{}
	cfg.Crawler.ProfileRefresh = 24 * time.Hour
	pool, err := proxy.New(config.Proxy{
		Tiers:     []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://isp.proxy.example:10001"}}},
		PageLoads: config.PageLoads{Interval: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := &Deps{Cfg: cfg, Proxies: pool, Now: func() time.Time { return now }}
	ep := proxy.Endpoint{Tier: "isp", Bucket: "isp|isp.proxy.example:10001"}
	withTokens := &session.Identity{Endpoint: ep}
	withTokens.SetTokens(htmlx.Tokens{LSD: "x"})
	fresh := &taskv1.PageScope{PageId: 7, ProfileSeenAt: timestamppb.New(now.Add(-time.Hour))}
	stale := &taskv1.PageScope{PageId: 7, ProfileSeenAt: timestamppb.New(now.Add(-48 * time.Hour))}
	first, later := &taskv1.Task{Seq: 0}, &taskv1.Task{Seq: 1}

	c := map[string]int64{}
	if d.readProfile(later, stale, withTokens, c) || d.readProfile(first, fresh, withTokens, c) || c["profile_skips"] != 1 {
		t.Fatalf("later segments and fresh profiles skip the read: %v", c)
	}
	if !d.readProfile(first, stale, withTokens, c) {
		t.Fatal("a stale profile is read when the exit can load a page")
	}
	if d.readProfile(first, stale, withTokens, c) || c["profile_deferred"] != 1 {
		t.Fatalf("the exit just loaded a page: the read waits: %v", c)
	}
	if !d.readProfile(first, stale, &session.Identity{Endpoint: ep}, c) {
		t.Fatal("an identity without tokens loads the page anyway")
	}
}
