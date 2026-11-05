package proxy

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
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
	p.pages.shared = &sharedGate{rdb: rdb, prefix: ns + "fbs:pagegate:", now: time.Now, seen: map[string]readyAt{},
		mirror: p.pages.mirror}
}

// sharedGate keeps each exit's pacing in a Valkey hash: next (no page load
// before it), paused (none before it, after a failure) and backoff, the
// last pause; times are Unix milliseconds by Valkey's clock.
type sharedGate struct {
	rdb    *redis.Client
	prefix string
	now    func() time.Time
	warned atomic.Int64 // Unix seconds of the last fallback warning
	// downUntil (Unix nanoseconds): after a failed call, every call skips
	// Valkey until then, so an unreachable Valkey costs one timeout per
	// sharedRetry rather than one per call.
	downUntil atomic.Int64

	// mirror receives every exit state Valkey returns (the page-load gate's
	// own pacing), so a process that loses Valkey keeps the pauses and
	// intervals it last saw instead of starting from none.
	mirror func(exit string, next, paused time.Time, backoff time.Duration)

	mu sync.Mutex
	// seen remembers ready's answer per exit for readyFor: the session pool
	// asks it, holding its lock, for every idle identity in turn, and many
	// share an exit. take and observe forget the exit's answer.
	seen map[string]readyAt
}

type readyAt struct {
	ok bool
	at time.Time
}

const (
	// sharedTimeout bounds one call: the gate is consulted while picking an
	// exit, so a slow Valkey must not stall the crawl.
	sharedTimeout = 300 * time.Millisecond
	// sharedRetry is how long a process paces on its own after a failed
	// call before it tries Valkey again.
	sharedRetry = 5 * time.Second
	// readyFor is how long ready's answer for an exit stands. It is advice:
	// the page load itself takes the slot, and waits when it was wrong.
	readyFor = 500 * time.Millisecond
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

// Every script ends with the exit's state: next, paused and backoff
// (ms), which the process mirrors into its own pacing for when Valkey
// can't be reached.

// takeScript returns 1, no reason and prev when it takes the page load
// (next moved from prev); 0, the reason ("paced" or "backoff") and next
// when the exit must wait; then the state.
var takeScript = redis.NewScript(nowLua + `
local h = redis.call('HMGET', KEYS[1], 'next', 'paused', 'backoff')
local nxt, paused, b = tonumber(h[1]) or 0, tonumber(h[2]) or 0, tonumber(h[3]) or 0
if now < paused then return {0, 'backoff', nxt, nxt, paused, b} end
if now < nxt then return {0, 'paced', nxt, nxt, paused, b} end
local set = now + tonumber(ARGV[1])
redis.call('HSET', KEYS[1], 'next', ms(set))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, '', nxt, set, paused, b}
`)

// restore gives a take back: next returns to prev unless another take
// moved it since.
var restoreScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'next') == ARGV[1] then
	redis.call('HSET', KEYS[1], 'next', ARGV[2])
end
return 0
`)

// readyScript returns 1 when the exit may load a page now, else 0; then
// the state.
var readyScript = redis.NewScript(nowLua + `
local h = redis.call('HMGET', KEYS[1], 'next', 'paused', 'backoff')
local nxt, paused, b = tonumber(h[1]) or 0, tonumber(h[2]) or 0, tonumber(h[3]) or 0
local ok = 1
if now < paused or now < nxt then ok = 0 end
return {ok, nxt, paused, b}
`)

// observeScript records a page load: ARGV[1] is 1 for a failed one, 0 for
// one that loaded. It returns the pause it started (ms, 0 for none), then
// the state.
var observeScript = redis.NewScript(nowLua + `
local h = redis.call('HMGET', KEYS[1], 'next', 'paused', 'backoff')
local nxt, paused, b = tonumber(h[1]) or 0, tonumber(h[2]) or 0, tonumber(h[3]) or 0
if ARGV[1] == '0' then
	if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('HSET', KEYS[1], 'backoff', '0') end
	return {0, nxt, paused, 0}
end
local base, cap = tonumber(ARGV[2]), tonumber(ARGV[3])
if b == 0 then b = base else b = math.min(2 * b, math.max(cap, base)) end
if now + b > paused then paused = now + b end
redis.call('HSET', KEYS[1], 'backoff', ms(b), 'paused', ms(paused))
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {b, nxt, paused, b}
`)

var errBadReply = errors.New("proxy: shared page-load gate: unexpected reply")

func (s *sharedGate) key(exit string) string { return s.prefix + exit }

// up reports whether to call Valkey: not within sharedRetry of a failure.
func (s *sharedGate) up() bool { return s.now().UnixNano() >= s.downUntil.Load() }

func (s *sharedGate) forget(exit string) {
	s.mu.Lock()
	delete(s.seen, exit)
	s.mu.Unlock()
}

// ints reads a script's reply of n values from first on, all integers.
func ints(res []any, first, n int) ([]int64, error) {
	if len(res) != first+n {
		return nil, errBadReply
	}
	out := make([]int64, n)
	for i := range out {
		v, ok := res[first+i].(int64)
		if !ok {
			return nil, errBadReply
		}
		out[i] = v
	}
	return out, nil
}

// state hands an exit's state (next, paused, backoff, in ms) to mirror.
func (s *sharedGate) state(exit string, v []int64) {
	if s.mirror != nil {
		s.mirror(exit, time.UnixMilli(v[0]), time.UnixMilli(v[1]), time.Duration(v[2])*time.Millisecond)
	}
}

func (s *sharedGate) take(exit string, interval time.Duration) (ok bool, why string, restore func(), err error) {
	s.forget(exit)
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	res, err := takeScript.Run(ctx, s.rdb, []string{s.key(exit)}, interval.Milliseconds(), sharedTTL.Milliseconds()).Slice()
	if err != nil {
		return false, "", nil, err
	}
	v, err := ints(res, 2, 4) // prev, next, paused, backoff
	if err != nil {
		return false, "", nil, err
	}
	s.state(exit, v[1:])
	if took, _ := res[0].(int64); took != 1 {
		why, _ := res[1].(string)
		return false, why, func() {}, nil
	}
	prev, set := v[0], v[1]
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
	now := s.now()
	s.mu.Lock()
	r, ok := s.seen[exit]
	s.mu.Unlock()
	if ok && now.Sub(r.at) < readyFor {
		return r.ok, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	res, err := readyScript.Run(ctx, s.rdb, []string{s.key(exit)}).Slice()
	if err != nil {
		return false, err
	}
	v, err := ints(res, 0, 4) // ready, next, paused, backoff
	if err != nil {
		return false, err
	}
	s.state(exit, v[1:])
	s.mu.Lock()
	s.seen[exit] = readyAt{ok: v[0] == 1, at: now}
	s.mu.Unlock()
	return v[0] == 1, nil
}

func (s *sharedGate) observe(exit string, failed bool, backoff, maxBackoff time.Duration) (time.Duration, error) {
	s.forget(exit)
	ctx, cancel := context.WithTimeout(context.Background(), sharedTimeout)
	defer cancel()
	f := "0"
	if failed {
		f = "1"
	}
	res, err := observeScript.Run(ctx, s.rdb, []string{s.key(exit)}, f, backoff.Milliseconds(), maxBackoff.Milliseconds(),
		sharedTTL.Milliseconds()).Slice()
	if err != nil {
		return 0, err
	}
	v, err := ints(res, 0, 4) // pause, next, paused, backoff
	if err != nil {
		return 0, err
	}
	s.state(exit, v[1:])
	return time.Duration(v[0]) * time.Millisecond, nil
}

// failed counts a call that fell back to this process's own pacing, skips
// Valkey for sharedRetry, and warns at most once a minute.
func (s *sharedGate) failed(err error) {
	s.downUntil.Store(s.now().Add(sharedRetry).UnixNano())
	metrics.PageGateErrors.Inc()
	now := time.Now().Unix()
	if last := s.warned.Load(); now-last >= 60 && s.warned.CompareAndSwap(last, now) {
		slog.Warn("proxy: shared page-load gate unreachable; pacing this process's page loads on its own", "err", err)
	}
}
