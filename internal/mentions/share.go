package mentions

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Share holds checks to a share of the page-load capacity of their exits,
// fleet-wide: in each window at most PerWindow checks start, counted in
// Valkey by every crawler. Each static exit loads one page per
// fb.proxy.page_loads.interval, so a window holds exits × window/interval
// page loads; collection always keeps what checks don't take.
type Share struct {
	R         *redis.Client
	Key       string // prefix of the window counters
	Window    time.Duration
	PerWindow int
	Now       func() time.Time
}

// SharePerWindow is how many checks may start in a window: share of the
// exits' page loads in it (one per interval each), at least one. 0 when
// there is no share to hold: no share, no static exits (a gateway or
// direct tier is not paced), or no page-load pacing.
func SharePerWindow(share float64, exits int, interval, window time.Duration) int {
	if share <= 0 || exits <= 0 || interval <= 0 || window <= 0 {
		return 0
	}
	capacity := float64(exits) * float64(window) / float64(interval)
	return max(1, int(math.Floor(share*capacity)))
}

// NewShare returns the share limit for share of exits' page loads, or nil
// when there is none to hold (SharePerWindow).
func NewShare(r *redis.Client, ns string, share float64, exits int, interval, window time.Duration) *Share {
	n := SharePerWindow(share, exits, interval, window)
	if n == 0 {
		return nil
	}
	return &Share{R: r, Key: ns + "fbs:mentions:share:", Window: window, PerWindow: n}
}

func (s *Share) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Take reserves a check in the current window. It returns the window's
// counter, to give the check back with Give, and false when the window's
// share is used up.
func (s *Share) Take(ctx context.Context) (string, bool, error) {
	key := s.Key + strconv.FormatInt(s.now().UnixNano()/int64(s.Window), 10)
	p := s.R.TxPipeline()
	n := p.Incr(ctx, key)
	p.Expire(ctx, key, 2*s.Window)
	if _, err := p.Exec(ctx); err != nil {
		return "", false, err
	}
	if n.Val() > int64(s.PerWindow) {
		// Refused: the counter goes back to the checks taken, so one given
		// back frees a place.
		return "", false, s.R.Decr(ctx, key).Err()
	}
	return key, true, nil
}

// Give returns a check that sent nothing to its window.
func (s *Share) Give(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	return s.R.Decr(ctx, key).Err()
}
