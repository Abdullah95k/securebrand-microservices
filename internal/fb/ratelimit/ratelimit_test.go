package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// Several identities on one exit, from several pods, send no more than the
// exit's rate together: four of them asking as fast as they can for two
// seconds at 5 a second get about 10 requests through, not four times
// that; another exit has its own rate.
func TestWaitRateHoldsAnExitAcrossIdentities(t *testing.T) {
	rdb := testenv.Redis(t)
	ns := testenv.TopicPrefix()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pods := []*Redis{NewRedis(rdb), NewRedis(rdb)} // two crawler pods sharing Valkey
	var exit1, exit2 atomic.Int64
	var wg sync.WaitGroup
	run := func(l *Redis, key string, n *atomic.Int64) {
		defer wg.Done()
		for l.WaitRate(ctx, key, 5) == nil {
			n.Add(1)
		}
	}
	for i := range 4 { // four identities on exit isp#1
		wg.Add(1)
		go run(pods[i%2], ns+"exit:isp#1", &exit1)
	}
	wg.Add(1)
	go run(pods[0], ns+"exit:isp#2", &exit2)
	wg.Wait()
	// 2s at 5/s, plus the first request at once: at most 11.
	if n := exit1.Load(); n > 11 || n < 8 {
		t.Fatalf("four identities on one exit sent %d requests in 2s at 5/s, want about 10", n)
	}
	if n := exit2.Load(); n < 8 {
		t.Fatalf("another exit was held back by the first: %d requests", n)
	}
}

// A rate below one a second spaces requests: 2 a second is one every 500ms.
func TestWaitRateBelowOneASecond(t *testing.T) {
	rdb := testenv.Redis(t)
	l, key := NewRedis(rdb), testenv.TopicPrefix()+"exit:isp#1"
	ctx := context.Background()
	if err := l.WaitRate(ctx, key, 0.5); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := l.WaitRate(ctx, key, 0.5); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 1800*time.Millisecond {
		t.Fatalf("the second request at 0.5/s came after %s, want about 2s", d)
	}
}
