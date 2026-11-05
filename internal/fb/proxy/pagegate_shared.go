package proxy

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// SharePageGate makes the page-load gate one for every process that shares
// it (rdb and ns): crawler pods, their stages and registry-sync. Without it
// each process paces the exits on its own, so N processes load up to N
// pages per interval on an exit, and one that walls an exit doesn't pause
// the others there. The state lives in Valkey and is read with Valkey's
// clock, so processes on different machines agree on it. While Valkey
// can't be reached a process paces on its own again (as before), and says
// so (fbs_proxy_page_gate_errors_total).
func (p *Pool) SharePageGate(rdb *redis.Client, ns string) {
	if rdb == nil || p.pages.off() {
		return
	}
	p.pages.shared = &sharedGate{rdb: rdb, prefix: ns + "fbs:pagegate:"}
}

// sharedGate keeps each exit's pacing in a Valkey hash: next (no page load
// before it), paused (none before it, after a failure) and backoff, the
// last pause; times are Unix milliseconds by Valkey's clock.
type sharedGate struct {
	rdb    *redis.Client
	prefix string
	warned atomic.Int64 // Unix seconds of the last fallback warning
}

const (
	// sharedTimeout bounds one call: the gate is consulted while picking an
	// exit, so a slow Valkey must not stall the crawl.
	sharedTimeout = 300 * time.Millisecond
	// sharedTTL keeps an exit's state while it is in use: far longer than
	// any interval or pause, so only an exit idle for a day forgets its
	// backoff.
	sharedTTL = 24 * time.Hour
)

// Every script reads the time from Valkey, never from the caller.
const nowLua = `local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function ms(x) return string.format('%.0f', x) end
`

// takeScript returns 1, no reason, prev and set when it takes the page
// load (next moved from prev to set); 0, the reason ("paced" or "backoff")
// and zeros when the exit must wait.
var takeScript = redis.NewScript(nowLua + `
local h = redis.call('HMGET', KEYS[1], 'next', 'paused')
local nxt, paused = tonumber(h[1]) or 0, tonumber(h[2]) or 0
if now < paused then return {0, 'backoff', 0, 0} end
if now < nxt then return {0, 'paced', 0, 0} end
local set = now + tonumber(ARGV[1])
redis.call('HSET', KEYS[1], 'next', ms(set))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, '', nxt, set}
`)

// restore gives a take back: next returns to prev unless another take
// moved it since.
var restoreScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'next') == ARGV[1] then
	redis.call('HSET', KEYS[1], 'next', ARGV[2])
end
return 0
`)

var readyScript = redis.NewScript(nowLua + `
local h = redis.call('HMGET', KEYS[1], 'next', 'paused')
if now < (tonumber(h[2]) or 0) or now < (tonumber(h[1]) or 0) then return 0 end
return 1
`)

// observe: ARGV[1] is 1 for a failed page load, 0 for one that loaded.
// It returns the pause started (ms), 0 for none.
var observeScript = redis.NewScript(nowLua + `
if ARGV[1] == '0' then
	if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('HSET', KEYS[1], 'backoff', '0') end
	return 0
end
local base, cap = tonumber(ARGV[2]), tonumber(ARGV[3])
local h = redis.call('HMGET', KEYS[1], 'backoff', 'paused')
local b, paused = tonumber(h[1]) or 0, tonumber(h[2]) or 0
if b == 0 then b = base else b = math.min(2 * b, math.max(cap, base)) end
if now + b > paused then paused = now + b end
redis.call('HSET', KEYS[1], 'backoff', ms(b), 'paused', ms(paused))
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return b
`)

var errBadReply = errors.New("proxy: shared page-load gate: unexpected reply")

func (s *sharedGate) key(exit string) string { return s.prefix + exit }

func (s *sharedGate) take(exit string, interval time.Duration) (ok bool, why string, restore func(), err error) {
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	res, err := takeScript.Run(ctx, s.rdb, []string{s.key(exit)}, interval.Milliseconds(), sharedTTL.Milliseconds()).Slice()
	if err != nil {
		return false, "", nil, err
	}
	if len(res) != 4 {
		return false, "", nil, errBadReply
	}
	if took, _ := res[0].(int64); took != 1 {
		why, _ := res[1].(string)
		return false, why, func() {}, nil
	}
	prev, _ := res[2].(int64)
	set, _ := res[3].(int64)
	return true, "", func() {
		ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
		defer cancel()
		if err := restoreScript.Run(ctx, s.rdb, []string{s.key(exit)}, strconv.FormatInt(set, 10), strconv.FormatInt(prev, 10)).Err(); err != nil {
			// The exit waits out the interval it didn't use: slower, never faster.
			s.failed(err)
		}
	}, nil
}

func (s *sharedGate) ready(exit string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	n, err := readyScript.Run(ctx, s.rdb, []string{s.key(exit)}).Int()
	return n == 1, err
}

func (s *sharedGate) observe(exit string, failed bool, backoff, maxBackoff time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	f := "0"
	if failed {
		f = "1"
	}
	ms, err := observeScript.Run(ctx, s.rdb, []string{s.key(exit)}, f, backoff.Milliseconds(), maxBackoff.Milliseconds(),
		sharedTTL.Milliseconds()).Int64()
	return time.Duration(ms) * time.Millisecond, err
}

// failed counts a call that fell back to this process's own pacing, and
// warns at most once a minute.
func (s *sharedGate) failed(err error) {
	metrics.PageGateErrors.Inc()
	now := time.Now().Unix()
	if last := s.warned.Load(); now-last >= 60 && s.warned.CompareAndSwap(last, now) {
		slog.Warn("proxy: shared page-load gate unreachable; pacing this process's page loads on its own", "err", err)
	}
}
