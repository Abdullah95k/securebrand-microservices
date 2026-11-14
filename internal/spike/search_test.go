package spike

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/fbtest"
)

// The probe loads each surface for each keyword at its interval, records
// counts only (never the keywords), and stops at the first wall.
func TestSearchProbe(t *testing.T) {
	env := fbtest.New(t, fbtest.Options{})
	var waits []time.Duration
	sleep := func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }
	kws := []string{"synthetic brand one", "synthetic brand two"}
	res := SearchProbe(context.Background(), env.Pool, env.Loader, SearchProbeOptions{Keywords: kws, Tier: "direct",
		Interval: time.Minute, Sleep: sleep})
	if len(res.Loads) != 4 || res.Verdict == SearchError || res.Verdict == SearchWalled {
		t.Fatalf("%+v", res)
	}
	if len(waits) != 3 || waits[0] != time.Minute {
		t.Fatalf("waits %v", waits)
	}
	out := FormatSearchProbe(res)
	for _, k := range kws {
		if strings.Contains(out, k) {
			t.Fatalf("a keyword is printed: %s", out)
		}
	}
	// Walls, rate limits and checkpoints end the probe; nothing else does.
	for c, want := range map[detect.Class]bool{detect.LoginWall: true, detect.Checkpoint: true, detect.RateLimited: true,
		detect.OK: false, detect.SoftError: false, detect.ContentUnavailable: false} {
		if searchStops(c) != want {
			t.Errorf("searchStops(%s) = %v", c, !want)
		}
	}
	if got := countPageIDs([]byte(`{"__typename":"Page","id":"11"},{"id":"12","__typename":"Page"},{"__typename":"Page","id":"11"}`)); got != 2 {
		t.Fatalf("page ids %d", got)
	}
}
