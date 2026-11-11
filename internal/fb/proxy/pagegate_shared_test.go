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
	exit1 = "isp#1"
	exit2 = "isp#2"
)

// valkey is a Valkey stand-in and the one clock it and the gates read.
type valkey struct {
	*miniredis.Miniredis
	t time.Time
}

func (v *valkey) now() time.Time { return v.t }

// SetTime moves the clock of Valkey and of the gates alike.
func (v *valkey) SetTime(t time.Time) {
	v.t = t
	v.Miniredis.SetTime(t)
}

// sharedPools returns two pools (two crawler pods) sharing one Valkey, and
// the Valkey, whose clock the gate reads.
func sharedPools(t *testing.T) (a, b *Pool, mr *valkey) {
	t.Helper()
	mr = &valkey{Miniredis: miniredis.RunT(t)}
	mr.SetTime(time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC))
	a, b = sharedPool(t, mr, "pilot."), sharedPool(t, mr, "pilot.")
	return a, b, mr
}

func sharedPool(t *testing.T, mr *valkey, ns string) *Pool {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	p, _ := gatedPool(t, pilotGate)
	p.SharePageGate(rdb, ns)
	p.pages.shared.now = mr.now
	return p
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

// Without Valkey each process paces on its own again, and counts it; it
// tries Valkey again after sharedRetry, not on every call.
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
	a.CanLoadPage(ep(exit2))
	if n := gateErrors() - before; n != 1 {
		t.Fatalf("%v failed calls, want 1: Valkey is skipped for a while after one", n)
	}
	mr.t = mr.t.Add(sharedRetry)
	a.CanLoadPage(ep(exit2))
	if n := gateErrors() - before; n != 2 {
		t.Fatalf("%v failed calls, want 2: Valkey is tried again after sharedRetry", n)
	}
}

// What Valkey last said holds when it goes away: a pod keeps the pause
// another pod's wall started, and the interval another pod's page load
// began, rather than loading at once on every exit.
func TestSharedGateStateOutlivesValkey(t *testing.T) {
	a, b, mr := sharedPools(t)
	b.PageLoad(ep(exit1))
	b.ObservePageLoad(ep(exit1), detect.LoginWall) // paused 15m
	b.PageLoad(ep(exit2))                          // interval 2m
	if a.CanLoadPage(ep(exit1)) || a.CanLoadPage(ep(exit2)) {
		t.Fatal("not shared")
	}
	mr.Close()
	mr.t = mr.t.Add(time.Minute)
	if ok, why := a.PageLoad(ep(exit1)); ok || why != "backoff" {
		t.Fatalf("the pause was lost with Valkey: %v %q", ok, why)
	}
	if ok, why := a.PageLoad(ep(exit2)); ok || why != "paced" {
		t.Fatalf("the interval was lost with Valkey: %v %q", ok, why)
	}
	a.pages.now = mr.now
	mr.t = mr.t.Add(15 * time.Minute)
	if ok, _ := a.PageLoad(ep(exit1)); !ok {
		t.Fatal("still paused after the pause Valkey gave")
	}
}

// ready's answer per exit stands for readyFor (the session pool asks it
// for each idle identity in turn), and a take or a wall forgets it.
func TestSharedGateReadyIsRemembered(t *testing.T) {
	a, b, mr := sharedPools(t)
	if !a.CanLoadPage(ep(exit1)) {
		t.Fatal("a fresh exit is not ready")
	}
	b.PageLoad(ep(exit1))
	if !a.CanLoadPage(ep(exit1)) {
		t.Fatal("the answer was not remembered")
	}
	mr.t = mr.t.Add(readyFor)
	if a.CanLoadPage(ep(exit1)) {
		t.Fatal("remembered beyond readyFor")
	}
	mr.SetTime(mr.t.Add(2 * time.Minute))
	if !a.CanLoadPage(ep(exit2)) {
		t.Fatal("exit2 not ready")
	}
	a.ObservePageLoad(ep(exit2), detect.LoginWall)
	if a.CanLoadPage(ep(exit2)) {
		t.Fatal("a wall did not forget the answer")
	}
}

// Another namespace (another deployment on the same Valkey) has its own
// pacing, and a gate turned off shares nothing.
func TestSharedGateNamespaces(t *testing.T) {
	a, _, mr := sharedPools(t)
	other := sharedPool(t, mr, "staging.")
	a.PageLoad(ep(exit1))
	if ok, _ := other.PageLoad(ep(exit1)); !ok {
		t.Fatal("namespaces share pacing")
	}
	off, _ := gatedPool(t, config.PageLoads{})
	off.SharePageGate(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "pilot.")
	if off.pages.shared != nil {
		t.Fatal("a gate turned off was shared")
	}
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

func gateErrors() float64 {
	var m dto.Metric
	_ = metrics.PageGateErrors.Write(&m)
	return m.GetCounter().GetValue()
}

// registry-sync's claim puts it first in line: while the claim stands, the
// crawlers take no page load on any exit, so the next exit to come free
// goes to registry-sync. Its own page load uses the claim up; a claim
// nobody uses lapses after its ttl, and UnclaimPageLoad drops it at once.
func TestSharedGateClaimGoesFirst(t *testing.T) {
	crawler, rs, mr := sharedPools(t)
	rs.SetPageLoadHolder("registry-sync")
	at := func(sec int) {
		mr.SetTime(time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC).Add(time.Duration(sec) * time.Second))
	}
	// The crawlers keep both exits at their pace.
	if ok1, _ := crawler.PageLoad(ep(exit1)); !ok1 {
		t.Fatal("setup: exit1")
	}
	if ok2, _ := crawler.PageLoad(ep(exit2)); !ok2 {
		t.Fatal("setup: exit2")
	}
	if err := rs.ClaimPageLoad(time.Minute); err != nil {
		t.Fatal(err)
	}
	at(120) // both exits come free
	if ok, why := crawler.PageLoad(ep(exit1)); ok || why != "claimed" {
		t.Fatalf("a crawler took a free exit while registry-sync claimed it: %v %q", ok, why)
	}
	if crawler.CanLoadPage(ep(exit2)) {
		t.Fatal("a claimed exit is offered to a crawler")
	}
	if _, err := crawler.Next("isp"); !errors.Is(err, ErrExitsPaced) {
		t.Fatalf("a crawler's new identity while claimed: %v, want ErrExitsPaced (a short wait)", err)
	}
	if !rs.CanLoadPage(ep(exit1)) {
		t.Fatal("the claimant is not offered the free exit")
	}
	if ok, why := rs.PageLoad(ep(exit1)); !ok {
		t.Fatalf("the claimant was refused the free exit: %q", why)
	}
	if ok, why := crawler.PageLoad(ep(exit2)); !ok {
		t.Fatalf("the claim outlived the claimant's page load: %q", why)
	}

	// A claim nobody uses lapses after its ttl.
	at(240)
	if err := rs.ClaimPageLoad(time.Minute); err != nil {
		t.Fatal(err)
	}
	if ok, why := crawler.PageLoad(ep(exit1)); ok || why != "claimed" {
		t.Fatalf("second claim: %v %q", ok, why)
	}
	mr.FastForward(61 * time.Second)
	if ok, why := crawler.PageLoad(ep(exit1)); !ok {
		t.Fatalf("a lapsed claim still holds the crawlers: %q", why)
	}

	// UnclaimPageLoad drops it at once; a crawler's call drops nothing.
	at(480)
	if err := rs.ClaimPageLoad(time.Minute); err != nil {
		t.Fatal(err)
	}
	crawler.UnclaimPageLoad()
	if ok, why := crawler.PageLoad(ep(exit1)); ok || why != "claimed" {
		t.Fatalf("a crawler dropped registry-sync's claim: %v %q", ok, why)
	}
	rs.UnclaimPageLoad()
	if ok, why := crawler.PageLoad(ep(exit1)); !ok {
		t.Fatalf("after UnclaimPageLoad: %q", why)
	}
}

// Without a shared gate a claim would be seen by no other process: it does
// nothing.
func TestClaimNeedsTheSharedGate(t *testing.T) {
	p, _ := gatedPool(t, pilotGate)
	p.SetPageLoadHolder("registry-sync")
	if err := p.ClaimPageLoad(time.Minute); err != nil {
		t.Fatal(err)
	}
	if ok, why := p.PageLoad(ep(exit1)); !ok {
		t.Fatalf("a local claim refused a page load: %q", why)
	}
	p.UnclaimPageLoad()
}
