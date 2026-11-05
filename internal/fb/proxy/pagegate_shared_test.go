package proxy

import (
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

const (
	exit1 = "isp|isp.proxy.example:10001"
	exit2 = "isp|isp.proxy.example:10002"
)

// sharedPools returns two pools (two crawler pods) sharing one Valkey, and
// the Valkey, whose clock the gate reads.
func sharedPools(t *testing.T) (a, b *Pool, mr *miniredis.Miniredis) {
	t.Helper()
	mr = miniredis.RunT(t)
	mr.SetTime(time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC))
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	a, _ = gatedPool(t, pilotGate)
	b, _ = gatedPool(t, pilotGate)
	a.SharePageGate(rdb, "pilot.")
	b.SharePageGate(rdb, "pilot.")
	return a, b, mr
}

func ep(exit string) Endpoint { return Endpoint{Tier: "isp", Exit: exit} }

// Two pods share each exit's interval: a page load one takes makes the
// other wait, on that exit only, until the interval has passed by Valkey's
// clock.
func TestSharedGatePacesAcrossPods(t *testing.T) {
	a, b, mr := sharedPools(t)
	if ok, _ := a.PageLoad(ep(exit1)); !ok {
		t.Fatal("first page load on the exit refused")
	}
	if ok, why := b.PageLoad(ep(exit1)); ok || why != "paced" {
		t.Fatalf("the other pod loaded a page within the interval: %v %q", ok, why)
	}
	if b.CanLoadPage(ep(exit1)) || !b.CanLoadPage(ep(exit2)) {
		t.Fatal("CanLoadPage does not follow the shared gate")
	}
	if ok, _ := b.PageLoad(ep(exit2)); !ok {
		t.Fatal("another exit is paced too")
	}
	mr.SetTime(time.Date(2026, 11, 6, 3, 1, 59, 0, time.UTC))
	if ok, _ := b.PageLoad(ep(exit1)); ok {
		t.Fatal("loaded before the interval ended")
	}
	mr.SetTime(time.Date(2026, 11, 6, 3, 2, 0, 0, time.UTC))
	if ok, _ := b.PageLoad(ep(exit1)); !ok {
		t.Fatal("still paced after the interval")
	}
	// A new identity reserves its page load through Next, on the free exit.
	mr.SetTime(time.Date(2026, 11, 6, 3, 2, 30, 0, time.UTC))
	if e, err := a.Next("isp"); err != nil || e.Exit != exit2 {
		t.Fatalf("Next: %v %q, want the exit not paced", err, e.Exit)
	}
	if _, err := b.Next("isp"); !errors.Is(err, ErrExitsPaced) {
		t.Fatalf("Next with both exits paced: %v", err)
	}
}

// A wall seen by one pod pauses the exit for all, doubling while walls
// repeat; a page that loads resets the doubling (the pause running keeps
// running).
func TestSharedGateBackoffAcrossPods(t *testing.T) {
	a, b, mr := sharedPools(t)
	at := func(minutes int) {
		mr.SetTime(time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC).Add(time.Duration(minutes) * time.Minute))
	}
	a.PageLoad(ep(exit1))
	a.ObservePageLoad(ep(exit1), detect.LoginWall)
	at(14)
	if ok, why := b.PageLoad(ep(exit1)); ok || why != "backoff" {
		t.Fatalf("the other pod loaded during the pause: %v %q", ok, why)
	}
	at(15)
	if ok, _ := b.PageLoad(ep(exit1)); !ok {
		t.Fatal("still paused after 15m")
	}
	b.ObservePageLoad(ep(exit1), detect.LoginWall) // second wall: 30m
	at(44)
	if a.CanLoadPage(ep(exit1)) {
		t.Fatal("the second pause did not double")
	}
	at(45)
	if ok, _ := a.PageLoad(ep(exit1)); !ok {
		t.Fatal("still paused after 30m")
	}
	a.ObservePageLoad(ep(exit1), detect.OK)
	b.ObservePageLoad(ep(exit1), detect.LoginWall) // after a load: back to 15m
	at(60)
	if !a.CanLoadPage(ep(exit1)) {
		t.Fatal("a page that loaded did not reset the doubling")
	}
	// The pause stops at max_backoff (1h for pilotGate).
	for range 5 {
		a.ObservePageLoad(ep(exit1), detect.LoginWall)
	}
	at(60 + 59)
	if a.CanLoadPage(ep(exit1)) {
		t.Fatal("paused less than max_backoff")
	}
	at(60 + 60)
	if !a.CanLoadPage(ep(exit1)) {
		t.Fatal("paused beyond max_backoff")
	}
}

// A take given back frees the exit for the other pod, but never undoes a
// take made after it.
func TestSharedGateRestore(t *testing.T) {
	a, b, mr := sharedPools(t)
	ok, _, restore := a.pages.take(exit1)
	if !ok {
		t.Fatal("take refused")
	}
	restore()
	if ok, _ := b.PageLoad(ep(exit1)); !ok {
		t.Fatal("a take given back still paces the exit")
	}
	mr.SetTime(time.Date(2026, 11, 6, 3, 5, 0, 0, time.UTC))
	_, _, late := a.pages.take(exit1)
	mr.SetTime(time.Date(2026, 11, 6, 3, 7, 0, 0, time.UTC))
	if ok, _ := b.PageLoad(ep(exit1)); !ok {
		t.Fatal("paced after the interval")
	}
	late() // a's take is long used up; b's must stand
	if ok, _ := a.PageLoad(ep(exit1)); ok {
		t.Fatal("a stale restore undid another pod's page load")
	}
}

// Without Valkey each process paces on its own again, and counts it.
func TestSharedGateFallsBackWhenValkeyIsDown(t *testing.T) {
	a, _, mr := sharedPools(t)
	mr.Close()
	before := gateErrors()
	if ok, _ := a.PageLoad(ep(exit1)); !ok {
		t.Fatal("no page load without Valkey")
	}
	if ok, why := a.PageLoad(ep(exit1)); ok || why != "paced" {
		t.Fatalf("the process's own pacing is off without Valkey: %v %q", ok, why)
	}
	if gateErrors()-before < 2 {
		t.Fatal("fallbacks not counted")
	}
}

// Another namespace (another deployment on the same Valkey) has its own
// pacing, and a gate turned off shares nothing.
func TestSharedGateNamespaces(t *testing.T) {
	a, _, mr := sharedPools(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	other, _ := gatedPool(t, pilotGate)
	other.SharePageGate(rdb, "staging.")
	a.PageLoad(ep(exit1))
	if ok, _ := other.PageLoad(ep(exit1)); !ok {
		t.Fatal("namespaces share pacing")
	}
	off, _ := gatedPool(t, config.PageLoads{})
	off.SharePageGate(rdb, "pilot.")
	if off.pages.shared != nil {
		t.Fatal("a gate turned off was shared")
	}
}

func gateErrors() float64 {
	var m dto.Metric
	_ = metrics.PageGateErrors.Write(&m)
	return m.GetCounter().GetValue()
}

// The scripts on a real Valkey (FBS_TEST_REDIS_ADDR): its clock can't be
// moved, so only what holds within one interval.
func TestSharedGateOnValkey(t *testing.T) {
	rdb := testenv.Redis(t)
	before := gateErrors()
	a, _ := gatedPool(t, pilotGate)
	b, _ := gatedPool(t, pilotGate)
	a.SharePageGate(rdb, "test.")
	b.SharePageGate(rdb, "test.")
	ok, _, restore := a.pages.take(exit1)
	if !ok {
		t.Fatal("take refused")
	}
	if ok, why := b.PageLoad(ep(exit1)); ok || why != "paced" {
		t.Fatalf("shared interval: %v %q", ok, why)
	}
	restore()
	if !b.CanLoadPage(ep(exit1)) {
		t.Fatal("restore did not free the exit")
	}
	b.ObservePageLoad(ep(exit1), detect.LoginWall)
	if ok, why := a.PageLoad(ep(exit1)); ok || why != "backoff" {
		t.Fatalf("shared pause: %v %q", ok, why)
	}
	if ttl := rdb.PTTL(t.Context(), "test.fbs:pagegate:"+exit1).Val(); ttl <= 23*time.Hour {
		t.Fatalf("state expires in %v", ttl)
	}
	if n := gateErrors() - before; n != 0 {
		t.Fatalf("%v calls fell back", n)
	}
}
