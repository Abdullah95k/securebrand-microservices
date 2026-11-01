package session

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices/configs"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
)

var storeExits = []string{"http://u:secret@203.0.113.10:8000", "http://u:secret@198.51.100.10:8000", "http://u:secret@192.0.2.10:8000"}

func storePool(t *testing.T, store Store, endpoints ...string) (*Pool, *proxy.Pool) {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{
		Tiers:     []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: endpoints}},
		PageLoads: config.PageLoads{Interval: 2 * time.Minute, Backoff: 15 * time.Minute, MaxBackoff: 2 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 8, Store: store}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return pool, proxies
}

// A crawler that stops saves its idle sessions and its exits' page-load
// pauses; the next one resumes them: the same session on the same exit,
// with its tokens and cookies, and no page load. Sessions without tokens,
// retired or still in use are not saved, and the proxy's credentials never
// are.
func TestSessionsResumeAfterARestart(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	base, _ := url.Parse("https://www.facebook.com")

	before, _ := storePool(t, store, storeExits...)
	good, _ := before.Acquire(ctx, "isp")
	walled, _ := before.Acquire(ctx, "isp")
	busy, err := before.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	tokens := htmlx.Tokens{LSD: "lsd-1", DTSG: "dtsg-1"}
	good.SetTokens(tokens)
	good.Doer.SetCookie(base, "datr", "datr-1")
	good.NextReq()
	Outcome(good, detect.OK)
	walled.SetTokens(htmlx.Tokens{LSD: "lsd-2"})
	PageOutcome(walled, detect.LoginWall) // retired, and its exit pauses page loads
	busy.SetTokens(htmlx.Tokens{LSD: "lsd-3"})
	before.Release(good)
	before.Release(walled)
	idle, _ := before.Acquire(ctx, "isp") // the one released last before: good
	if idle != good {
		t.Fatal("the idle identity is not reused")
	}
	before.Release(idle)
	if n, err := before.Save(ctx); err != nil || n != 1 {
		t.Fatalf("saved %d: %v", n, err)
	}
	if before.live["isp"] != 1 || len(before.idle["isp"]) != 0 {
		t.Fatalf("saved identities stay in the pool: live=%d idle=%d", before.live["isp"], len(before.idle["isp"]))
	}
	if strings.Contains(mr.Dump(), "secret") {
		t.Fatal("the proxy's credentials are saved")
	}

	after, proxies := storePool(t, store, storeExits...)
	if n, err := after.Resume(ctx); err != nil || n != 1 {
		t.Fatalf("resumed %d: %v", n, err)
	}
	got, err := after.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != good.ID || got.Endpoint != good.Endpoint || got.Tokens() != tokens || got.SessionParam() != good.SessionParam() {
		t.Fatalf("resumed %s on %s with %+v, want %s on %s", got.ID, got.Endpoint.Exit, got.Tokens(), good.ID, good.Endpoint.Exit)
	}
	if c := got.Doer.Cookie(base, "datr"); c != "datr-1" {
		t.Fatalf("cookie %q", c)
	}
	if got.NextReq() != "2" || got.requests != 1 || got.maxRequests != good.maxRequests {
		t.Fatalf("the session's counters restart: requests=%d max=%d", got.requests, got.maxRequests)
	}
	if got.TakeReservation() {
		t.Fatal("a resumed session holds a page load")
	}
	if ok, why := proxies.PageLoad(walled.Endpoint); ok || why != "backoff" {
		t.Fatalf("the walled exit loads a page after a restart: %v %q", ok, why)
	}
	if ok, why := proxies.PageLoad(busy.Endpoint); ok || why != "paced" {
		t.Fatalf("an exit that just loaded a page loads another after a restart: %v %q", ok, why)
	}
	if after.live["isp"] != 1 {
		t.Fatalf("live %d", after.live["isp"])
	}
	// Its outcomes reach its exit like any identity's.
	PageOutcome(got, detect.LoginWall)
	if ok, why := proxies.PageLoad(got.Endpoint); ok || why != "backoff" {
		t.Fatalf("a resumed session's walled page load leaves its exit loading pages: %v %q", ok, why)
	}

	// Each saved session resumes once.
	// Each saved session resumes once, and so does the exits' pacing: a
	// later start never re-applies it over what page loads showed since.
	again, againProxies := storePool(t, store, storeExits...)
	if n, _ := again.Resume(ctx); n != 0 {
		t.Fatalf("a saved session resumed twice: %d", n)
	}
	if ok, _ := againProxies.PageLoad(walled.Endpoint); !ok {
		t.Fatal("the exits' saved pacing is restored twice")
	}

	// A session whose exit is no longer configured is dropped.
	s := Saved{ID: good.ID, Tier: "isp", Exit: good.Endpoint.Exit, Profile: "chrome_146", Tokens: tokens, SavedAt: time.Now()}
	if err := store.Save(ctx, []Saved{s}, nil); err != nil {
		t.Fatal(err)
	}
	other, _ := storePool(t, store, "http://u:secret@192.0.2.99:8000")
	if n, err := other.Resume(ctx); err != nil || n != 0 || other.live["isp"] != 0 {
		t.Fatalf("resumed %d on another exit: %v", n, err)
	}
}

// Sessions resume up to the tier's cap, and only for the fingerprint they
// were made with.
func TestResumeKeepsToTheCapAndTheProfile(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	var saved []Saved
	for i, exit := range []string{"isp|203.0.113.10:8000", "isp|198.51.100.10:8000", "isp|192.0.2.10:8000"} {
		saved = append(saved, Saved{ID: fmt.Sprint("id-", i), Tier: "isp", Exit: exit, Profile: "chrome_146",
			Tokens: htmlx.Tokens{LSD: "x"}, SavedAt: time.Now()})
	}
	saved[0].Profile = "firefox"
	if err := store.Save(ctx, saved, nil); err != nil {
		t.Fatal(err)
	}
	pool, _ := storePool(t, store, storeExits...)
	pool.cfg.MaxPerTier = 1
	if n, err := pool.Resume(ctx); err != nil || n != 1 || pool.live["isp"] != 1 || pool.idle["isp"][0].ID != "id-1" {
		t.Fatalf("resumed %d (live %d): %v", n, pool.live["isp"], err)
	}
}

// Saved sessions past SavedSessionTTL, and a gateway's (its sticky session
// expires), are not resumed.
func TestSavedSessionsThatCannotResume(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	s := Saved{ID: "id-1", Tier: "isp", Exit: "isp|203.0.113.10:8000", Profile: "chrome_146", Tokens: htmlx.Tokens{LSD: "x"}}
	s.SavedAt = time.Now().Add(-SavedSessionTTL - time.Minute)
	fresh := s
	fresh.ID, fresh.SavedAt = "id-2", time.Now()
	if err := store.Save(ctx, []Saved{s, fresh}, nil); err != nil {
		t.Fatal(err)
	}
	got, err := store.TakeSessions(ctx)
	if err != nil || len(got) != 1 || got[0].ID != "id-2" {
		t.Fatalf("%+v %v", got, err)
	}

	base, _ := url.Parse("https://www.facebook.com")
	gw := newIdentity("g", proxy.Endpoint{Tier: "res", URL: "http://u:p@gw.example:7777", SessionID: "s"}, Fingerprint{}, transport.NewStd(time.Second), base, 1, 1, 0)
	gw.SetTokens(htmlx.Tokens{LSD: "x"})
	if _, ok := gw.saved(); ok {
		t.Fatal("a gateway session is saved")
	}
	direct := newIdentity("d", proxy.Endpoint{Tier: "direct"}, Fingerprint{}, transport.NewStd(time.Second), base, 1, 1, 0)
	if _, ok := direct.saved(); ok {
		t.Fatal("a session without tokens is saved")
	}
	direct.SetTokens(htmlx.Tokens{LSD: "x"})
	if _, ok := direct.saved(); !ok {
		t.Fatal("a direct session is not saved")
	}
	direct.markRetired("unhealthy")
	if _, ok := direct.saved(); ok {
		t.Fatal("a retired session is saved")
	}
}

// A crawler resumes the sessions of one that stops after it started (a
// rolling update starts the new pod first).
func TestSessionsSavedLaterResume(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()}), NS: "t."}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, _ := storePool(t, store, storeExits...)
	if n, err := pool.Resume(ctx); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	go func() { _ = pool.RunResume(ctx, 10*time.Millisecond) }()
	s := Saved{ID: "id-1", Tier: "isp", Exit: "isp|203.0.113.10:8000", Profile: "chrome_146", Tokens: htmlx.Tokens{LSD: "x"}, SavedAt: time.Now()}
	if err := store.Save(ctx, []Saved{s}, map[string]proxy.ExitState{"isp|203.0.113.10:8000": {PausedUntil: time.Now().Add(48 * time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("{t.fbs:sessions}:exits"); ttl < 48*time.Hour {
		t.Fatalf("the exits' pacing expires before its pause ends: %s", ttl)
	}
	id, err := pool.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); id.ID != "id-1" && time.Now().Before(deadline); {
		pool.Release(id)
		time.Sleep(20 * time.Millisecond)
		id, _ = pool.Acquire(ctx, "isp")
	}
	if id.ID != "id-1" {
		t.Fatal("a session saved after the start is not resumed")
	}
}
