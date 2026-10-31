package stage

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/checkpoint"
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
