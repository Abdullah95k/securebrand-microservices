package ratelimit

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// The fleet-wide request ceiling is adjusted by AIMD (additive increase,
// multiplicative decrease) on the block rate Facebook shows the whole
// fleet: crawlers count outcomes into 10-second windows in Redis, the
// leader scheduler reads each finished window and moves the ceiling, and
// crawlers read the ceiling back for the global GCRA limiter. Adding pods
// therefore never adds load: they share the same ceiling.

// Window is the AIMD measurement window.
const Window = 10 * time.Second

// settle is how long after a window ends the controller waits for the last
// reporter flushes.
const settle = 3 * time.Second

func windowKey(ns string, start time.Time) string {
	return ns + "aimd:w:" + strconv.FormatInt(start.Unix(), 10)
}

func ceilingKey(ns string) string { return ns + "aimd:global_rps" }

// Reporter counts GraphQL outcomes and flushes them to Redis. It is safe for
// concurrent use; Observe is a pair of atomic adds.
type Reporter struct {
	rdb            *redis.Client
	ns             string
	total, blocked atomic.Int64
	now            func() time.Time
}

// NewReporter returns a Reporter writing under namespace ns.
func NewReporter(rdb *redis.Client, ns string) *Reporter {
	return &Reporter{rdb: rdb, ns: ns, now: time.Now}
}

// Observe counts one response. Transport failures are the proxy's, not
// Facebook's answer, and are left out.
func (r *Reporter) Observe(c detect.Class) {
	if c == detect.Transport {
		return
	}
	r.total.Add(1)
	if c.Blocking() {
		r.blocked.Add(1)
	}
}

// Flush writes the counts gathered since the last flush.
func (r *Reporter) Flush(ctx context.Context) error {
	total, blocked := r.total.Swap(0), r.blocked.Swap(0)
	if total == 0 {
		return nil
	}
	key := windowKey(r.ns, r.now().Truncate(Window))
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HIncrBy(ctx, key, "total", total)
		p.HIncrBy(ctx, key, "blocked", blocked)
		p.Expire(ctx, key, 10*time.Minute)
		return nil
	})
	if err != nil {
		// Keep the counts for the next attempt.
		r.total.Add(total)
		r.blocked.Add(blocked)
	}
	return err
}

// Run flushes every two seconds until ctx is done.
func (r *Reporter) Run(ctx context.Context) error {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = r.Flush(fctx)
			cancel()
			return nil
		case <-t.C:
			if err := r.Flush(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("aimd: flush failed", "err", err)
			}
		}
	}
}

// Ceiling caches the fleet ceiling for the GraphQL client's global limiter.
type Ceiling struct {
	rdb      *redis.Client
	ns       string
	fallback int
	cur      atomic.Int64
}

// NewCeiling returns a Ceiling that reports fallback until a ceiling has
// been published.
func NewCeiling(rdb *redis.Client, ns string, fallback int) *Ceiling {
	c := &Ceiling{rdb: rdb, ns: ns, fallback: fallback}
	c.cur.Store(int64(fallback))
	return c
}

// RPS returns the current ceiling in requests per second.
func (c *Ceiling) RPS() int { return int(c.cur.Load()) }

// Refresh reads the published ceiling. A missing key keeps the fallback;
// an unreachable Redis keeps the last value read.
func (c *Ceiling) Refresh(ctx context.Context) error {
	v, err := c.rdb.Get(ctx, ceilingKey(c.ns)).Int()
	switch {
	case errors.Is(err, redis.Nil):
		c.cur.Store(int64(c.fallback))
		return nil
	case err != nil:
		return err
	case v > 0:
		c.cur.Store(int64(v))
	}
	return nil
}

// Run refreshes every five seconds until ctx is done.
func (c *Ceiling) Run(ctx context.Context) error {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("aimd: ceiling refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Decision is one AIMD step.
type Decision struct {
	From, To    int
	Total       int64
	BlockRate   float64
	Utilization float64 // requests / (ceiling x window)
	Reason      string  // increase | decrease | hold:<why>
}

// Decide applies one AIMD step to ceiling cur for a window with total
// responses of which blocked were blocks:
//
//   - block rate above HighBlockRate: the rate actually sent (at most cur)
//     x Decrease, so the cut lowers real load even when the fleet was below
//     the ceiling (multiplicative decrease);
//   - below LowBlockRate while the fleet uses at least MinUtilization of the
//     ceiling: cur + max(1, cur x Increase) (additive increase);
//   - otherwise, or with fewer than MinSamples responses: hold.
//
// The result stays within [MinRPS, MaxRPS].
func Decide(cfg config.AIMD, cur int, total, blocked int64, window time.Duration) Decision {
	d := Decision{From: cur, To: cur, Total: total}
	if total > 0 {
		d.BlockRate = float64(blocked) / float64(total)
	}
	if cur > 0 {
		d.Utilization = float64(total) / (float64(cur) * window.Seconds())
	}
	switch {
	case total < int64(cfg.MinSamples):
		d.Reason = "hold:few_samples"
	case d.BlockRate > cfg.HighBlockRate:
		sent := math.Min(float64(cur), float64(total)/window.Seconds())
		d.To, d.Reason = int(math.Floor(sent*cfg.Decrease)), "decrease"
	case d.BlockRate < cfg.LowBlockRate && d.Utilization >= cfg.MinUtilization:
		d.To, d.Reason = cur+max(1, int(float64(cur)*cfg.Increase)), "increase"
	case d.BlockRate < cfg.LowBlockRate:
		d.Reason = "hold:underused"
	default:
		d.Reason = "hold:block_rate"
	}
	if clamped := min(max(d.To, cfg.MinRPS), cfg.MaxRPS); clamped != d.To {
		d.To = clamped
		if d.To != d.From && d.Reason != "increase" && d.Reason != "decrease" {
			d.Reason = "clamp"
		}
	}
	return d
}

// Controller runs AIMD on the leader scheduler.
type Controller struct {
	rdb   *redis.Client
	ns    string
	cfg   config.AIMD
	start int
	now   func() time.Time
	last  time.Time // last window processed
}

// NewController returns a controller that starts the ceiling at start. The
// configured start is always allowed, even above cfg.MaxRPS.
func NewController(rdb *redis.Client, ns string, cfg config.AIMD, start int) *Controller {
	cfg.MaxRPS = max(cfg.MaxRPS, start)
	return &Controller{rdb: rdb, ns: ns, cfg: cfg, start: start, now: time.Now}
}

// Tick processes the latest finished window, once.
func (c *Controller) Tick(ctx context.Context) error {
	w := c.now().Add(-settle).Truncate(Window).Add(-Window)
	if !w.After(c.last) {
		return nil
	}
	c.last = w
	counts, err := c.rdb.HGetAll(ctx, windowKey(c.ns, w)).Result()
	if err != nil {
		return err
	}
	total, _ := strconv.ParseInt(counts["total"], 10, 64)
	blocked, _ := strconv.ParseInt(counts["blocked"], 10, 64)
	cur, err := c.rdb.Get(ctx, ceilingKey(c.ns)).Int()
	missing := errors.Is(err, redis.Nil)
	if missing || (err == nil && cur <= 0) {
		cur, err = c.start, nil
	}
	if err != nil {
		return err
	}
	d := Decide(c.cfg, cur, total, blocked, Window)
	metrics.FleetRPS.Set(float64(total) / Window.Seconds())
	metrics.FleetBlockRate.Set(d.BlockRate)
	metrics.AIMDCeiling.Set(float64(d.To))
	if d.To != d.From || missing {
		if err := c.rdb.Set(ctx, ceilingKey(c.ns), d.To, 0).Err(); err != nil {
			return err
		}
	}
	if d.To != d.From {
		metrics.AIMDAdjustments.WithLabelValues(d.Reason).Inc()
		slog.Info("aimd: ceiling", "from", d.From, "to", d.To, "reason", d.Reason, "responses", total,
			"block_rate", d.BlockRate, "utilization", d.Utilization)
	}
	return nil
}
