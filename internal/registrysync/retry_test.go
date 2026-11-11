package registrysync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
)

func TestRetryPaced(t *testing.T) {
	ctx := context.Background()
	soon := func() time.Time { return time.Now().Add(time.Second) }
	// tries returns a try that fails with errs in turn, then succeeds.
	tries := func(n *int, errs ...error) func() (int, error) {
		return func() (int, error) {
			*n++
			if *n <= len(errs) {
				return 0, errs[*n-1]
			}
			return 7, nil
		}
	}

	var n int
	v, err := retryPaced(ctx, soon(), time.Millisecond, tries(&n, session.ErrPaced, fmt.Errorf("pool: %w", proxy.ErrExitsPaced), session.ErrExitPaused))
	if err != nil || v != 7 || n != 4 {
		t.Fatalf("paced three times, then loaded: %v %v after %d tries", v, err, n)
	}

	n = 0
	other := errors.New("page load: login_wall")
	if _, err := retryPaced(ctx, soon(), time.Millisecond, tries(&n, other)); !errors.Is(err, other) || n != 1 {
		t.Fatalf("a real failure is not retried: %v after %d tries", err, n)
	}

	n = 0
	if _, err := retryPaced(ctx, time.Now().Add(20*time.Millisecond), 5*time.Millisecond, func() (int, error) {
		n++
		return 0, session.ErrPaced
	}); !errors.Is(err, session.ErrPaced) || n < 2 || n > 5 {
		t.Fatalf("paced until the deadline: %v after %d tries", err, n)
	}

	n = 0
	if _, err := retryPaced(ctx, time.Time{}, time.Millisecond, tries(&n, session.ErrPaced)); !errors.Is(err, session.ErrPaced) || n != 1 {
		t.Fatalf("no wait (canary_wait 0): one try, got %v after %d", err, n)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	n = 0
	if _, err := retryPaced(cctx, soon(), time.Hour, tries(&n, session.ErrPaced)); !errors.Is(err, session.ErrPaced) || n != 1 {
		t.Fatalf("a cancelled run stops waiting: %v after %d tries", err, n)
	}
}
