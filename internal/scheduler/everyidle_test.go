package scheduler

import (
	"context"
	"testing"
	"time"
)

// An idle loop doubles its wait up to the maximum; work brings it back to
// the minimum at once.
func TestEveryIdleBacksOffWhileIdle(t *testing.T) {
	const minWait, maxWait = 20 * time.Millisecond, 160 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var at []time.Time
	work := map[int]bool{8: true} // the 9th run finds work
	done := make(chan struct{})
	go func() {
		_ = EveryIdle(ctx, "test", minWait, maxWait, func(context.Context) (bool, error) {
			at = append(at, time.Now())
			if len(at) == 11 {
				cancel()
				close(done)
			}
			return work[len(at)-1], nil
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop too slow")
	}
	gap := func(i int) time.Duration { return at[i].Sub(at[i-1]) }
	// Idle: 40, 80, 160, 160 ... (doubling from the minimum, capped).
	if gap(1) < 35*time.Millisecond || gap(3) < 150*time.Millisecond || gap(5) > 250*time.Millisecond {
		t.Fatalf("idle gaps %v %v %v", gap(1), gap(3), gap(5))
	}
	// After the run that found work, the next comes after the minimum.
	if gap(9) > 60*time.Millisecond {
		t.Fatalf("gap after work %v, want about %v", gap(9), minWait)
	}
}
