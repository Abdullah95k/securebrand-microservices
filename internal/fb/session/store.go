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
	// TakeSessions removes and returns the saved sessions: each goes to
	// one crawler.
	TakeSessions(ctx context.Context) ([]Saved, error)
	// TakeExits removes and returns the exits' saved pacing, so a later
	// start never re-applies it over what page loads have since shown.
	TakeExits(ctx context.Context) (map[string]proxy.ExitState, error)
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
			// Past every pause: a later start has nothing to wait out.
			keep := 24 * time.Hour
			for _, st := range exits {
				keep = max(keep, time.Until(st.PausedUntil)+time.Hour)
			}
			p.Expire(ctx, savedExitsKey, keep)
		}
		return nil
	})
	return err
}

func (s *RedisStore) TakeExits(ctx context.Context) (map[string]proxy.ExitState, error) {
	_, savedExitsKey := s.keys()
	var all *redis.MapStringStringCmd
	if _, err := s.R.TxPipelined(ctx, func(p redis.Pipeliner) error {
		all = p.HGetAll(ctx, savedExitsKey)
		p.Del(ctx, savedExitsKey)
		return nil
	}); err != nil {
		return nil, err
	}
	exits := map[string]proxy.ExitState{}
	for exit, b := range all.Val() {
		var st proxy.ExitState
		if json.Unmarshal([]byte(b), &st) == nil {
			exits[exit] = st
		}
	}
	return exits, nil
}

func (s *RedisStore) TakeSessions(ctx context.Context) ([]Saved, error) {
	savedSessionsKey, _ := s.keys()
	var out []Saved
	for {
		b, err := s.R.LPop(ctx, savedSessionsKey).Bytes()
		if errors.Is(err, redis.Nil) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		var v Saved
		if json.Unmarshal(b, &v) != nil || time.Since(v.SavedAt) > SavedSessionTTL {
			continue
		}
		out = append(out, v)
	}
}
