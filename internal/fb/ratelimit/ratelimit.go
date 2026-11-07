// Package ratelimit provides the fleet-wide limiters (Redis GCRA) that keep
// the request rate to Facebook bounded no matter how many pods run.
package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

// Limiter blocks until a request under key is allowed at perSecond.
type Limiter interface {
	Wait(ctx context.Context, key string, perSecond int) error
}

// Redis is a distributed GCRA limiter. If Redis is unreachable it fails
// open after logging, relying on per-identity pacing, so a Redis blip does
// not stop crawling.
type Redis struct {
	l *redis_rate.Limiter
}

func NewRedis(c *redis.Client) *Redis { return &Redis{l: redis_rate.NewLimiter(c)} }

func (r *Redis) Wait(ctx context.Context, key string, perSecond int) error {
	if perSecond <= 0 {
		return nil
	}
	return r.wait(ctx, key, redis_rate.PerSecond(perSecond))
}

// WaitRate blocks until a request under key is allowed at perSecond, which
// may be below one: requests are spaced 1/perSecond apart, without burst,
// whichever pod sends them.
func (r *Redis) WaitRate(ctx context.Context, key string, perSecond float64) error {
	if perSecond <= 0 {
		return nil
	}
	return r.wait(ctx, key, redis_rate.Limit{Rate: 1, Burst: 1, Period: time.Duration(float64(time.Second) / perSecond)})
}

func (r *Redis) wait(ctx context.Context, key string, limit redis_rate.Limit) error {
	for {
		res, err := r.l.Allow(ctx, "rl:"+key, limit)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("ratelimit: redis unavailable, failing open", "key", key, "err", err)
			return nil
		}
		if res.Allowed > 0 {
			return nil
		}
		wait := res.RetryAfter
		if wait <= 0 {
			wait = 10 * time.Millisecond
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Noop never waits.
type Noop struct{}

func (Noop) Wait(context.Context, string, int) error { return nil }
