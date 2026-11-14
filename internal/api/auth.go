package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Scopes.
const (
	ScopeJobsWrite  = "jobs:write"
	ScopePagesWrite = "pages:write"
	ScopeDataRead   = "data:read"
	ScopePIIRead    = "pii:read"
	ScopeAdmin      = "admin"
	// ScopeDiscoveryWrite lets another tool send candidate accounts
	// (POST /v1/discovery/candidates), and nothing else.
	ScopeDiscoveryWrite = "discovery:write"
)

// Client is an authenticated API client.
type Client struct {
	ID            uuid.UUID
	Name          string
	Scopes        []string
	RatePerMinute int
	hash          []byte
	fetched       time.Time
}

// Has reports whether the client holds scope (admin holds all).
func (c *Client) Has(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope || s == ScopeAdmin {
			return true
		}
	}
	return false
}

// GenerateKey returns a new API key "fbs_<prefix>_<secret>", its prefix and
// the hash to store.
func GenerateKey() (key, prefix string, hash []byte) {
	b := make([]byte, 30)
	_, _ = rand.Read(b)
	s := base64.RawURLEncoding.EncodeToString(b)
	prefix, secret := strings.ToLower(strings.NewReplacer("-", "a", "_", "b").Replace(s[:8])), s[8:]
	h := sha256.Sum256([]byte(secret))
	return "fbs_" + prefix + "_" + secret, prefix, h[:]
}

// keyPrefixRe and keySecretRe are an importable key's two parts
// (fbs_<prefix>_<secret>): generated here, or by the platform inside the
// cluster in the same shape.
var (
	keyPrefixRe = regexp.MustCompile(`^[a-z0-9]{8}$`)
	keySecretRe = regexp.MustCompile(`^[A-Za-z0-9_-]{32,128}$`)
)

// ParseKey splits a key into its prefix and secret, false when it isn't
// fbs_<8 lowercase letters or digits>_<32 to 128 URL-safe characters>.
func ParseKey(key string) (prefix, secret string, ok bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(key), "fbs_")
	if !found {
		return "", "", false
	}
	prefix, secret, found = strings.Cut(rest, "_")
	if !found || !keyPrefixRe.MatchString(prefix) || !keySecretRe.MatchString(secret) {
		return "", "", false
	}
	return prefix, secret, true
}

// Results of ImportClient.
const (
	ImportCreated   = "created"
	ImportUpdated   = "updated"   // the same key: name, scopes or rate brought to what was asked
	ImportUnchanged = "unchanged" // the same key, as asked
	ImportRevoked   = "revoked"   // the same key, revoked: it stays revoked
)

// ErrKeyClash is a key whose prefix belongs to another key.
var ErrKeyClash = errors.New("api: a different key already has this prefix")

// ImportClient stores a client for a key made elsewhere (a Secret written
// inside the cluster), keeping only its hash, as CreateClient does. Run
// again with the same key it changes nothing, or brings the name, scopes
// and rate to what is asked; a revoked key is never revived.
func ImportClient(ctx context.Context, db *pgxpool.Pool, name string, scopes []string, ratePerMinute int, key string) (string, error) {
	prefix, secret, ok := ParseKey(key)
	if !ok {
		return "", errors.New("api: not a key of the form fbs_<8 lowercase letters or digits>_<32 to 128 URL-safe characters>")
	}
	h := sha256.Sum256([]byte(secret))
	var rate *int
	if ratePerMinute > 0 {
		rate = &ratePerMinute
	}
	var hash []byte
	var revoked bool
	err := db.QueryRow(ctx, `SELECT key_hash, revoked_at IS NOT NULL FROM api_clients WHERE key_prefix = $1`, prefix).Scan(&hash, &revoked)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		id, _ := uuid.NewV7()
		_, err := db.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes, rate_per_minute)
			VALUES ($1,$2,$3,$4,$5,$6)`, id, name, prefix, h[:], scopes, rate)
		return ImportCreated, err
	case err != nil:
		return "", err
	case subtle.ConstantTimeCompare(hash, h[:]) != 1:
		return "", ErrKeyClash
	case revoked:
		return ImportRevoked, nil
	}
	tag, err := db.Exec(ctx, `UPDATE api_clients SET name = $2, scopes = $3, rate_per_minute = $4
		WHERE key_prefix = $1 AND (name, scopes, rate_per_minute) IS DISTINCT FROM ($2, $3::text[], $4::int)`,
		prefix, name, scopes, rate)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() > 0 {
		return ImportUpdated, nil
	}
	return ImportUnchanged, nil
}

// CreateClient stores a new API client and returns its key.
func CreateClient(ctx context.Context, db *pgxpool.Pool, name string, scopes []string, ratePerMinute int) (string, uuid.UUID, error) {
	key, prefix, hash := GenerateKey()
	id, _ := uuid.NewV7()
	var rate *int
	if ratePerMinute > 0 {
		rate = &ratePerMinute
	}
	_, err := db.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes, rate_per_minute)
		VALUES ($1,$2,$3,$4,$5,$6)`, id, name, prefix, hash, scopes, rate)
	return key, id, err
}

type authenticator struct {
	db          *pgxpool.Pool
	limiter     *redis_rate.Limiter
	defaultRate int
	mu          sync.Mutex
	cache       map[string]*Client
}

var errUnauthorized = errors.New("missing or invalid API key")

func (a *authenticator) lookup(ctx context.Context, key string) (*Client, error) {
	parts := strings.SplitN(strings.TrimPrefix(key, "fbs_"), "_", 2)
	if !strings.HasPrefix(key, "fbs_") || len(parts) != 2 {
		return nil, errUnauthorized
	}
	prefix, secret := parts[0], parts[1]
	a.mu.Lock()
	c := a.cache[prefix]
	a.mu.Unlock()
	if c == nil || time.Since(c.fetched) > time.Minute {
		c = &Client{}
		var rate *int
		err := a.db.QueryRow(ctx, `SELECT id, name, scopes, rate_per_minute, key_hash FROM api_clients
			WHERE key_prefix = $1 AND revoked_at IS NULL`, prefix).Scan(&c.ID, &c.Name, &c.Scopes, &rate, &c.hash)
		if err != nil {
			return nil, errUnauthorized
		}
		c.RatePerMinute = a.defaultRate
		if rate != nil {
			c.RatePerMinute = *rate
		}
		c.fetched = time.Now()
		a.mu.Lock()
		a.cache[prefix] = c
		a.mu.Unlock()
	}
	h := sha256.Sum256([]byte(secret))
	if subtle.ConstantTimeCompare(h[:], c.hash) != 1 {
		return nil, errUnauthorized
	}
	return c, nil
}

type clientKey struct{}

func clientFrom(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey{}).(*Client)
	return c
}

func (a *authenticator) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		c, err := a.lookup(r.Context(), key)
		if err != nil {
			problem(w, http.StatusUnauthorized, "unauthorized", err.Error())
			return
		}
		if a.limiter != nil && c.RatePerMinute > 0 {
			res, err := a.limiter.Allow(r.Context(), "rl:api:"+c.ID.String(), redis_rate.PerMinute(c.RatePerMinute))
			if err == nil && res.Allowed == 0 {
				w.Header().Set("Retry-After", strconvItoa(int(res.RetryAfter.Seconds())+1))
				problem(w, http.StatusTooManyRequests, "rate limited", "API rate limit exceeded")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientKey{}, c)))
	})
}

func require(scope string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c := clientFrom(r.Context()); c == nil || !c.Has(scope) {
			problem(w, http.StatusForbidden, "forbidden", "this API key lacks the "+scope+" scope")
			return
		}
		h(w, r)
	}
}
