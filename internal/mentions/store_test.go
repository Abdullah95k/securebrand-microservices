package mentions

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

func copyEvents(t *testing.T, db *pgxpool.Pool, evs ...Event) {
	t.Helper()
	if err := pgx.BeginFunc(context.Background(), db, func(tx pgx.Tx) error { return CopyEvents(context.Background(), tx, evs) }); err != nil {
		t.Fatal(err)
	}
}

func aggregate(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	if _, err := Aggregate(context.Background(), db, 1000); err != nil {
		t.Fatal(err)
	}
}

func schedule(t *testing.T, db *pgxpool.Pool, ref string, pageID int64) {
	t.Helper()
	ctx := context.Background()
	if err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := dispatch.Register(ctx, tx, dispatch.ScheduleEntry{Ref: ref, Enabled: true, Interval: time.Hour, NextRun: time.Now()})
		if err == nil && pageID != 0 {
			_, err = tx.Exec(ctx, `UPDATE page_schedule SET page_id = $2 WHERE page_ref = $1`, ref, pageID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, db *pgxpool.Pool, ref string) Account {
	t.Helper()
	a, err := Get(context.Background(), db, ref)
	if err != nil {
		t.Fatalf("%s: %v", ref, err)
	}
	return a
}

func ev(ref string, page, post, comment int64, via string, at time.Time) Event {
	return Event{Target: Target{Ref: ref, ID: RefID(ref), Kind: KindUnknown, Via: via}, PageID: page, PostID: post, CommentID: comment, At: at}
}

// Events become counts per account: every mention counts, each page of
// ours once; the queue is drained; an account on the main list (by
// reference, by id, or by a stored page's vanity) is listed, not queued.
func TestAggregate(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	schedule(t, db, "v:ourpage", 0)
	schedule(t, db, "id:100064000000900", 0)
	schedule(t, db, "id:100064000000901", 100064000000555)
	if _, err := db.Exec(ctx, `INSERT INTO pages (page_id, vanity, status) VALUES (100064000000555, 'ShopVanity', 'active')`); err != nil {
		t.Fatal(err)
	}
	copyEvents(t, db,
		ev("v:someone", 1, 10, 100, ViaAuthor, now.Add(-3*time.Hour)),
		ev("v:someone", 1, 11, 0, ViaTag, now.Add(-2*time.Hour)),
		ev("v:someone", 2, 20, 200, ViaLink, now.Add(-time.Hour)),
		ev("v:ourpage", 1, 12, 0, ViaLink, now),
		ev("id:100064000000900", 1, 12, 0, ViaTag, now),
		ev("v:shopvanity", 1, 12, 0, ViaLink, now),
		ev("id:100064000000777", 2, 21, 0, ViaShare, now),
	)
	aggregate(t, db)
	a := get(t, db, "v:someone")
	if a.Mentions != 3 || a.Pages != 2 || a.Status != StatusUnchecked || !a.InQueue || a.Source != SourceMention {
		t.Fatalf("someone: %+v", a)
	}
	if !a.FirstSeenAt.Equal(now.Add(-3*time.Hour)) || !a.LastSeenAt.Equal(now.Add(-time.Hour)) {
		t.Fatalf("seen: %v %v", a.FirstSeenAt, a.LastSeenAt)
	}
	if a.FirstPostID == nil || *a.FirstPostID != 10 || a.FirstCommentID == nil || *a.FirstCommentID != 100 {
		t.Fatalf("first item: %+v", a)
	}
	if got, want := a.Vias, []string{ViaAuthor, ViaLink, ViaTag}; len(got) != 3 || got[0] != want[0] || got[2] != want[2] {
		t.Fatalf("vias %v", got)
	}
	want := math.Log(4) + 2*math.Log(3) + float64(a.LastSeenAt.Unix())/604800
	if math.Abs(a.Priority-want) > 1e-6 {
		t.Fatalf("priority %v, want %v", a.Priority, want)
	}
	for ref, pageRef := range map[string]string{"v:ourpage": "v:ourpage", "id:100064000000900": "id:100064000000900", "v:shopvanity": "id:100064000000901"} {
		if l := get(t, db, ref); l.Status != StatusListed || l.InQueue || l.PageRef == nil || *l.PageRef != pageRef {
			t.Fatalf("%s: %+v", ref, l)
		}
	}
	if a := get(t, db, "id:100064000000777"); a.ID == nil || *a.ID != 100064000000777 || a.Status != StatusUnchecked {
		t.Fatalf("id:100064000000777: %+v", a)
	}
	var left int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM mention_events`).Scan(&left)
	if left != 0 {
		t.Fatalf("%d events left", left)
	}

	// Again on page 1: one more mention, no new page.
	copyEvents(t, db, ev("v:someone", 1, 13, 0, ViaTag, now))
	aggregate(t, db)
	b := get(t, db, "v:someone")
	if b.Mentions != 4 || b.Pages != 2 || !b.LastSeenAt.Equal(now) || b.Priority <= a.Priority {
		t.Fatalf("again: %+v", b)
	}
	pages, err := PagesOf(ctx, db, "v:someone", 10)
	if err != nil || len(pages) != 2 || pages[0].PageID != 1 || pages[0].Mentions != 3 {
		t.Fatalf("pages %+v %v", pages, err)
	}
}

// Rule 2: an account checked not public within recheck_after only counts a
// new mention; once that has passed, a mention brings a new check.
func TestRecheckOnlyAfterExpiry(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC()
	copyEvents(t, db, ev("v:private.person", 1, 10, 100, ViaAuthor, now))
	aggregate(t, db)
	due, err := Pick(ctx, db, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("pick %v %v", due, err)
	}
	q, err := MarkQueued(ctx, db, due)
	if err != nil || len(q) != 1 {
		t.Fatal(err)
	}
	if err := RecordNotPublic(ctx, db, "v:private.person", Verdict{Status: StatusPrivate, Reason: "no_public_posts", Kind: KindProfile, ID: 100000000004242},
		180*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	a := get(t, db, "v:private.person")
	if a.InQueue || a.Status != StatusPrivate || a.ID == nil || *a.ID != 100000000004242 || a.Kind != KindProfile || a.FirstPostID != nil ||
		a.FirstCommentID != nil || a.RecheckAt == nil || a.RecheckAt.Sub(*a.CheckedAt) != 180*24*time.Hour || a.Checks != 1 {
		t.Fatalf("after check: %+v", a)
	}
	copyEvents(t, db, ev("v:private.person", 2, 20, 200, ViaAuthor, now))
	aggregate(t, db)
	if a := get(t, db, "v:private.person"); a.InQueue || a.Mentions != 2 || a.Pages != 2 {
		t.Fatalf("checked recently, mentioned again: %+v", a)
	}
	if due, _ := Pick(ctx, db, 10); len(due) != 0 {
		t.Fatalf("picked %v", due)
	}
	if _, err := db.Exec(ctx, `UPDATE checked_accounts SET recheck_at = now() - interval '1 day'`); err != nil {
		t.Fatal(err)
	}
	// Expired but not mentioned: nothing.
	if due, _ := Pick(ctx, db, 10); len(due) != 0 {
		t.Fatalf("picked without a mention %v", due)
	}
	copyEvents(t, db, ev("v:private.person", 2, 21, 201, ViaAuthor, now))
	aggregate(t, db)
	if a := get(t, db, "v:private.person"); !a.InQueue || a.Status != StatusPrivate {
		t.Fatalf("expired, mentioned again: %+v", a)
	}
	if due, _ := Pick(ctx, db, 10); len(due) != 1 {
		t.Fatalf("not picked again: %v", due)
	}
}

// Requested rechecks come first, then priority; an account registered
// since it was mentioned is marked listed, not picked; a task's stamp
// tells a stale copy from the live one.
func TestPick(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC()
	copyEvents(t, db,
		ev("v:low.one", 1, 1, 0, ViaTag, now.Add(-30*24*time.Hour)),
		ev("v:high.one", 1, 2, 0, ViaTag, now), ev("v:high.one", 2, 3, 0, ViaTag, now), ev("v:high.one", 3, 4, 0, ViaTag, now),
		ev("v:mid.one", 1, 5, 0, ViaTag, now),
		ev("v:later.listed", 1, 6, 0, ViaTag, now.Add(-24*time.Hour)),
	)
	aggregate(t, db)
	schedule(t, db, "v:later.listed", 0)
	if _, err := RequestRecheck(ctx, db, "v:low.one"); err != nil {
		t.Fatal(err)
	}
	// Four picked: the listed one is marked so and left out.
	due, err := Pick(ctx, db, 4)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range due {
		got = append(got, d.Ref)
	}
	if len(got) != 3 || got[0] != "v:low.one" || got[1] != "v:high.one" || got[2] != "v:mid.one" {
		t.Fatalf("order %v", got)
	}
	if a := get(t, db, "v:later.listed"); a.Status != StatusListed || a.InQueue {
		t.Fatalf("registered since: %+v", a)
	}
	q, err := MarkQueued(ctx, db, due[:1])
	if err != nil || len(q) != 1 {
		t.Fatal(err)
	}
	if _, ok, _ := Begin(ctx, db, q[0].Ref, q[0].QueuedAt); !ok {
		t.Fatal("the live task is stale")
	}
	if _, ok, _ := Begin(ctx, db, q[0].Ref, q[0].QueuedAt.Add(-time.Second)); ok {
		t.Fatal("a stale copy may check")
	}
	if again, _ := MarkQueued(ctx, db, due[:1]); len(again) != 0 {
		t.Fatal("queued twice")
	}
	if n, _ := InFlight(ctx, db); n != 1 {
		t.Fatalf("in flight %d", n)
	}
	if _, err := db.Exec(ctx, `UPDATE checked_accounts SET queued_at = now() - interval '7 hours' WHERE queued_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if n, err := FreeLost(ctx, db, 6*time.Hour); err != nil || n != 1 {
		t.Fatalf("freed %d %v", n, err)
	}
}

// The daily check cap is hard: a claim past it fails; a claim that sent
// nothing is given back; each day starts again.
func TestClaimCheck(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	day := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for i, want := range []bool{true, true, false} {
		if ok, err := ClaimCheck(ctx, db, day, 2); err != nil || ok != want {
			t.Fatalf("claim %d: %v %v", i, ok, err)
		}
	}
	if err := UnclaimCheck(ctx, db, day); err != nil {
		t.Fatal(err)
	}
	if ok, _ := ClaimCheck(ctx, db, day, 2); !ok {
		t.Fatal("a check given back was not available again")
	}
	if ok, _ := ClaimCheck(ctx, db, day.AddDate(0, 0, 1), 2); !ok {
		t.Fatal("the next day starts used up")
	}
	if ok, _ := ClaimCheck(ctx, db, day.AddDate(0, 0, 2), 0); ok {
		t.Fatal("a zero cap allowed a check")
	}
	if n, _ := ChecksOn(ctx, db, day); n != 2 {
		t.Fatalf("used %d", n)
	}
}

// A failed check records nothing about the account: it stays queued,
// unchecked, retried after a doubling backoff.
func TestRecordFailed(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	copyEvents(t, db, ev("v:walled.one", 1, 1, 0, ViaTag, time.Now()))
	aggregate(t, db)
	var prev time.Duration
	for i := 1; i <= 3; i++ {
		start := time.Now()
		if err := RecordFailed(ctx, db, "v:walled.one", "login_wall", true, Retry{Backoff: time.Hour, MaxBackoff: 3 * time.Hour,
			MaxFailures: 4, Park: 30 * 24 * time.Hour}); err != nil {
			t.Fatal(err)
		}
		a := get(t, db, "v:walled.one")
		wait := a.RetryAt.Sub(start).Round(time.Minute)
		want := min(time.Hour<<(i-1), 3*time.Hour)
		if a.Status != StatusUnchecked || a.CheckedAt != nil || !a.InQueue || a.Failures != i || wait != want || wait < prev {
			t.Fatalf("failure %d: wait %v, %+v", i, wait, a)
		}
		prev = wait
	}
	if due, _ := Pick(ctx, db, 10); len(due) != 0 {
		t.Fatal("picked before its retry time")
	}
	// The fourth failure in a row parks it, still unchecked, until a
	// mention after the park.
	if err := RecordFailed(ctx, db, "v:walled.one", "login_wall", true, Retry{Backoff: time.Hour, MaxBackoff: 3 * time.Hour,
		MaxFailures: 4, Park: 30 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	a := get(t, db, "v:walled.one")
	if a.InQueue || a.Status != StatusUnchecked || a.CheckedAt != nil || a.RecheckAt == nil || a.RecheckAt.Before(time.Now().Add(29*24*time.Hour)) {
		t.Fatalf("parked: %+v", a)
	}
	copyEvents(t, db, ev("v:walled.one", 1, 2, 0, ViaTag, time.Now()))
	aggregate(t, db)
	if a := get(t, db, "v:walled.one"); a.InQueue {
		t.Fatal("a mention during the park brought it back")
	}
	if _, err := db.Exec(ctx, `UPDATE checked_accounts SET recheck_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	copyEvents(t, db, ev("v:walled.one", 1, 3, 0, ViaTag, time.Now()))
	aggregate(t, db)
	if a := get(t, db, "v:walled.one"); !a.InQueue || a.Failures != 0 || a.RetryAt != nil {
		t.Fatalf("after the park: %+v", a)
	}
}

// What first mentioned an account follows the earliest mention, also one
// found later (a backfill after a scheduled crawl).
func TestFirstMentionFollowsEarliest(t *testing.T) {
	db := testenv.Postgres(t)
	now := time.Now().UTC()
	copyEvents(t, db, ev("v:some.page", 1, 20, 200, ViaTag, now))
	aggregate(t, db)
	copyEvents(t, db, ev("v:some.page", 2, 10, 100, ViaTag, now.Add(-48*time.Hour)), ev("v:some.page", 3, 30, 0, ViaTag, now.Add(time.Hour)))
	aggregate(t, db)
	a := get(t, db, "v:some.page")
	if *a.FirstPageID != 2 || *a.FirstPostID != 10 || *a.FirstCommentID != 100 || !a.FirstSeenAt.Equal(now.Add(-48*time.Hour).Truncate(time.Microsecond)) {
		t.Fatalf("first: %+v", a)
	}
}

// Public accounts are added under the daily cap with why; the rest wait
// for a later day, in priority order.
func TestAdderCap(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC()
	copyEvents(t, db, ev("v:first.page", 1, 1, 0, ViaTag, now), ev("v:first.page", 2, 2, 0, ViaTag, now),
		ev("v:second.page", 1, 3, 0, ViaShare, now))
	aggregate(t, db)
	loc := time.FixedZone("UTC+3", 3*3600)
	ad := &Adder{MaxNewPerDay: 1, Interval: 3 * time.Hour, Loc: loc}
	if res, err := ad.Public(ctx, db, "v:first.page", Verdict{Status: StatusPublic, Reason: "posts_visible", Kind: KindPage, ID: 100064000005001}); err != nil || res != Added {
		t.Fatalf("first: %v %v", res, err)
	}
	if res, err := ad.Public(ctx, db, "v:second.page", Verdict{Status: StatusPublic, Reason: "page", Kind: KindPage, ID: 100064000005002}); err != nil || res != Capped {
		t.Fatalf("second: %v %v", res, err)
	}
	var src string
	var pageID int64
	var via []byte
	if err := db.QueryRow(ctx, `SELECT source, page_id, discovered_via FROM page_schedule WHERE page_ref = 'id:100064000005001'`).Scan(&src, &pageID, &via); err != nil {
		t.Fatal(err)
	}
	var why Via
	if err := json.Unmarshal(via, &why); err != nil || src != SourceMention || pageID != 100064000005001 || why.AccountRef != "v:first.page" ||
		why.Mentions != 2 || why.Pages != 2 || why.Reason != "posts_visible" || why.FirstPostID == nil {
		t.Fatalf("schedule: %s %d %s %v", src, pageID, via, err)
	}
	if a := get(t, db, "v:first.page"); a.AddedAt == nil || a.PageRef == nil || *a.PageRef != "id:100064000005001" || a.Status != StatusPublic {
		t.Fatalf("first: %+v", a)
	}
	if a := get(t, db, "v:second.page"); a.AddedAt != nil || a.Status != StatusPublic || a.InQueue {
		t.Fatalf("second: %+v", a)
	}
	// Today: still capped. Tomorrow: added.
	if n, _ := ad.AddWaiting(ctx, db, 10); n != 0 {
		t.Fatal("added past the cap")
	}
	ad.Now = func() time.Time { return time.Now().Add(24 * time.Hour) }
	if n, err := ad.AddWaiting(ctx, db, 10); err != nil || n != 1 {
		t.Fatalf("next day: %d %v", n, err)
	}
	// One registered meanwhile under its id is listed, not added twice.
	copyEvents(t, db, ev("v:third.page", 1, 4, 0, ViaTag, now))
	aggregate(t, db)
	schedule(t, db, "id:100064000005003", 0)
	ad.MaxNewPerDay = 10
	if res, _ := ad.Public(ctx, db, "v:third.page", Verdict{Status: StatusPublic, Kind: KindPage, ID: 100064000005003}); res != OnList {
		t.Fatalf("third: %v", res)
	}
	if a := get(t, db, "v:third.page"); a.Status != StatusListed || *a.PageRef != "id:100064000005003" {
		t.Fatalf("third: %+v", a)
	}
}

// A candidate is a mention of its account with what brought it in: new
// ones wait for a check, listed ones stay listed, a private one checked
// recently only counts and keeps no evidence.
func TestAddCandidate(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	schedule(t, db, "v:already.ours", 0)
	a, created, err := AddCandidate(ctx, db, Candidate{Ref: "v:new.page", Source: "google", EvidenceURL: "https://example.org/r?q=1",
		Keyword: "erbil", Boost: 2})
	if err != nil || !created || a.Status != StatusUnchecked || !a.InQueue || a.Source != "google" || *a.Keyword != "erbil" ||
		a.Mentions != 1 || a.Priority < 2 {
		t.Fatalf("new: %+v %v", a, err)
	}
	if a, _, _ := AddCandidate(ctx, db, Candidate{Ref: "v:already.ours", Source: "google", Keyword: "x"}); a.Status != StatusListed ||
		a.Keyword != nil {
		t.Fatalf("listed: %+v", a)
	}
	copyEvents(t, db, ev("v:private.one", 1, 1, 0, ViaTag, time.Now()))
	aggregate(t, db)
	if err := RecordNotPublic(ctx, db, "v:private.one", Verdict{Status: StatusPrivate, Kind: KindProfile}, time.Hour); err != nil {
		t.Fatal(err)
	}
	a, created, err = AddCandidate(ctx, db, Candidate{Ref: "v:private.one", Source: "google", Keyword: "k", EvidenceURL: "https://e.org"})
	if err != nil || created || a.InQueue || a.Mentions != 2 || a.Keyword != nil || a.EvidenceURL != nil || a.Source != SourceMention {
		t.Fatalf("private: %+v %v", a, err)
	}
}

// Retention deletes accounts nobody mentioned or checked for that long,
// with their page counts; recent ones stay.
func TestExpire(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC()
	copyEvents(t, db, ev("v:old.mention", 1, 1, 0, ViaTag, now.AddDate(0, 0, -200)), ev("v:new.mention", 1, 2, 0, ViaTag, now))
	aggregate(t, db)
	n, err := Expire(ctx, db, 180)
	if err != nil || n != 1 {
		t.Fatalf("expired %d %v", n, err)
	}
	if _, err := Get(ctx, db, "v:old.mention"); !errors.Is(err, ErrNotFound) {
		t.Fatal("old account kept")
	}
	var pages int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM mention_pages WHERE account_ref = 'v:old.mention'`).Scan(&pages)
	if pages != 0 {
		t.Fatal("its page counts kept")
	}
	get(t, db, "v:new.mention")
}
