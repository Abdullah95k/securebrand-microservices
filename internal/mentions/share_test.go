package mentions

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// The share's size: share of the exits' page loads in a window, at least
// one; none without a share, static exits or pacing.
func TestSharePerWindow(t *testing.T) {
	for _, c := range []struct {
		share            float64
		exits            int
		interval, window time.Duration
		want             int
	}{
		{0.2, 10, 2 * time.Minute, 10 * time.Minute, 10}, // 50 page loads a window
		{0.2, 1, 2 * time.Minute, 10 * time.Minute, 1},   // 5 a window: one
		{0.2, 100, 2 * time.Minute, 10 * time.Minute, 100},
		{0, 10, 2 * time.Minute, 10 * time.Minute, 0},
		{0.2, 0, 2 * time.Minute, 10 * time.Minute, 0}, // a gateway or direct tier
		{0.2, 10, 0, 10 * time.Minute, 0},              // no page-load pacing
	} {
		if got := SharePerWindow(c.share, c.exits, c.interval, c.window); got != c.want {
			t.Errorf("SharePerWindow(%v, %d, %s, %s) = %d, want %d", c.share, c.exits, c.interval, c.window, got, c.want)
		}
	}
	if NewShare(nil, "", 0.2, 0, 2*time.Minute, 10*time.Minute) != nil {
		t.Error("a share limit without static exits")
	}
}

// With every exit's page-load slots wanted by both, checks take at most
// their share (20%), and collection keeps the rest: here, ten exits each
// loading one page per two minutes for six hours, checks asking first for
// every slot. A check that sent nothing is given back to its window.
func TestShareHoldsChecksToTheirShare(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	const exits = 10
	interval, window := 2*time.Minute, 10*time.Minute
	clock := time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)
	s := NewShare(rdb, testenv.TopicPrefix(), 0.2, exits, interval, window)
	s.Now = func() time.Time { return clock }

	var checks, collection int
	for slot := range int(6 * time.Hour / interval) {
		clock = time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC).Add(time.Duration(slot) * interval)
		for range exits {
			if _, ok, err := s.Take(ctx); err != nil {
				t.Fatal(err)
			} else if ok {
				checks++
			} else {
				collection++
			}
		}
	}
	share := float64(checks) / float64(checks+collection)
	if math.Abs(share-0.2) > 0.01 || collection < 4*checks {
		t.Fatalf("checks %d, collection %d: checks took %.1f%% of the page loads, want 20%%", checks, collection, 100*share)
	}

	// A new window: its share, less one given back, then one more.
	clock = clock.Add(window)
	var key string
	for i := range s.PerWindow {
		k, ok, err := s.Take(ctx)
		if err != nil || !ok {
			t.Fatalf("check %d of a new window: %v %v", i, ok, err)
		}
		key = k
	}
	if _, ok, _ := s.Take(ctx); ok {
		t.Fatal("a check past the window's share")
	}
	if err := s.Give(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Take(ctx); !ok {
		t.Fatal("a check given back wasn't available again")
	}
}
