package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func get(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// Readiness checks run at most once a minute; in between the probe gets
// the last result, a failure included. Liveness checks nothing.
func TestReadinessChecksAtMostOnceAMinute(t *testing.T) {
	o := NewOps(":0")
	now := time.Unix(1_800_000_000, 0)
	o.now = func() time.Time { return now }
	var calls atomic.Int32
	var fail atomic.Bool
	o.AddCheck("postgres", func(ctx context.Context) error {
		calls.Add(1)
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > CheckTimeout {
			t.Errorf("check without its own %s timeout", CheckTimeout)
		}
		if fail.Load() {
			return errors.New("down")
		}
		return nil
	})
	h := o.Handler()

	for range 5 {
		if code := get(t, h, "/readyz"); code != http.StatusOK {
			t.Fatalf("readyz %d", code)
		}
		now = now.Add(10 * time.Second) // the kubelet's period
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d checks in 50s, want 1", n)
	}
	now = now.Add(10 * time.Second)
	fail.Store(true)
	if code := get(t, h, "/readyz"); code != http.StatusServiceUnavailable || calls.Load() != 2 {
		t.Fatalf("after a minute: %d, %d checks", code, calls.Load())
	}
	fail.Store(false)
	now = now.Add(30 * time.Second)
	if code := get(t, h, "/readyz"); code != http.StatusServiceUnavailable || calls.Load() != 2 {
		t.Fatalf("a failure is kept until the next check: %d, %d checks", code, calls.Load())
	}
	for range 3 {
		if code := get(t, h, "/healthz"); code != http.StatusOK {
			t.Fatalf("healthz %d", code)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("liveness ran a check")
	}
}

// Checks run at once: three slow ones answer within one check's time.
func TestReadinessChecksRunTogether(t *testing.T) {
	o := NewOps(":0")
	for _, name := range []string{"postgres", "redis", "kafka"} {
		o.AddCheck(name, func(context.Context) error { time.Sleep(300 * time.Millisecond); return nil })
	}
	start := time.Now()
	if code := get(t, o.Handler(), "/readyz"); code != http.StatusOK {
		t.Fatalf("readyz %d", code)
	}
	if d := time.Since(start); d > 800*time.Millisecond {
		t.Fatalf("three 300ms checks took %s", d)
	}
}
