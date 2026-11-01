package session

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
)

// Store keeps sessions and the exits' page-load pacing across a restart: a
// crawler that stops saves them (Pool.Save), and the next one resumes them
// (Pool.Resume) instead of loading a page on every exit at once, the burst
// Facebook walls.
type Store interface {
	Save(ctx context.Context, sessions []Saved, exits map[string]proxy.ExitState) error
	// Load takes the saved sessions (each goes to one crawler) and reads the
	// exits' pacing (every crawler waits it out).
	Load(ctx context.Context) ([]Saved, map[string]proxy.ExitState, error)
}

// Saved is a session kept across a restart. It names its exit, never the
// proxy URL, which holds the proxy's credentials.
type Saved struct {
	ID           string            `json:"id"`
	Tier         string            `json:"tier"`
	Exit         string            `json:"exit"` // tier|host:port; empty for direct egress
	Profile      string            `json:"profile"`
	SessionID    string            `json:"session_id"`
	Req          uint64            `json:"req"`
	Requests     int               `json:"requests"`
	MaxRequests  int               `json:"max_requests"`
	Tokens       htmlx.Tokens      `json:"tokens"`
	Bootstrapped time.Time         `json:"bootstrapped"`
	Cookies      map[string]string `json:"cookies"`
	SavedAt      time.Time         `json:"saved_at"`
}

// SavedSessionTTL is how long a saved session stays good to resume: past
// it, its tokens have likely expired, and a page load is due anyway.
const SavedSessionTTL = 12 * time.Hour

// RedisStore keeps saved sessions in a Redis list and the exits' pacing in
// a hash. NS (the Kafka topic prefix) keeps deployments sharing a Redis
// apart.
type RedisStore struct {
	R  redis.Cmdable
	NS string
}

// keys are in one hash slot.
func (s *RedisStore) keys() (sessions, exits string) {
	tag := "{" + s.NS + "fbs:sessions}"
	return tag + ":saved", tag + ":exits"
}

func (s *RedisStore) Save(ctx context.Context, sessions []Saved, exits map[string]proxy.ExitState) error {
	savedSessionsKey, savedExitsKey := s.keys()
	_, err := s.R.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, v := range sessions {
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			p.RPush(ctx, savedSessionsKey, b)
		}
		if len(sessions) > 0 {
			p.Expire(ctx, savedSessionsKey, SavedSessionTTL)
		}
		for exit, st := range exits {
			b, err := json.Marshal(st)
			if err != nil {
				return err
			}
			p.HSet(ctx, savedExitsKey, exit, b)
		}
		if len(exits) > 0 {
			// Longer than any pause (fb.proxy.page_loads.max_backoff).
			p.Expire(ctx, savedExitsKey, 24*time.Hour)
		}
		return nil
	})
	return err
}

func (s *RedisStore) Load(ctx context.Context) ([]Saved, map[string]proxy.ExitState, error) {
	savedSessionsKey, savedExitsKey := s.keys()
	exits := map[string]proxy.ExitState{}
	m, err := s.R.HGetAll(ctx, savedExitsKey).Result()
	if err != nil {
		return nil, nil, err
	}
	for exit, b := range m {
		var st proxy.ExitState
		if json.Unmarshal([]byte(b), &st) == nil {
			exits[exit] = st
		}
	}
	var out []Saved
	for {
		b, err := s.R.LPop(ctx, savedSessionsKey).Bytes()
		if errors.Is(err, redis.Nil) {
			return out, exits, nil
		}
		if err != nil {
			return out, exits, err
		}
		var v Saved
		if json.Unmarshal(b, &v) != nil || time.Since(v.SavedAt) > SavedSessionTTL {
			continue
		}
		out = append(out, v)
	}
}
