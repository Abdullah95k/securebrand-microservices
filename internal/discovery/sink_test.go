package discovery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/discovery"
	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

func TestKeywordStore(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	k, created, err := discovery.AddKeyword(ctx, db, " Erbil ", "en", "erbil", true)
	if err != nil || !created || k.Term != "Erbil" || k.Normalized != "erbil" || k.Group != "erbil" || !k.Enabled {
		t.Fatalf("%+v %v %v", k, created, err)
	}
	// A spelling that folds to it returns it, unchanged.
	dup, created, err := discovery.AddKeyword(ctx, db, "ERBIL", "en", "other", false)
	if err != nil || created || dup.ID != k.ID || dup.Group != "erbil" || !dup.Enabled {
		t.Fatalf("%+v %v %v", dup, created, err)
	}
	// The same spelling in another language is another keyword.
	if _, created, err := discovery.AddKeyword(ctx, db, "Erbil", "kmr", "", true); err != nil || !created {
		t.Fatalf("%v %v", created, err)
	}
	for _, bad := range [][2]string{{"Erbil", "fr"}, {"  ", "en"}, {string(make([]rune, 101)), "en"}} {
		if _, _, err := discovery.AddKeyword(ctx, db, bad[0], bad[1], "", true); !errors.Is(err, discovery.ErrInvalid) {
			t.Errorf("%q/%s: %v", bad[0], bad[1], err)
		}
	}
	b, _, _ := discovery.AddKeyword(ctx, db, "Basra", "en", "basra", true)
	off := false
	if _, err := discovery.UpdateKeyword(ctx, db, b.ID, discovery.KeywordUpdate{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	term := "erbil"
	if _, err := discovery.UpdateKeyword(ctx, db, b.ID, discovery.KeywordUpdate{Term: &term}); !errors.Is(err, discovery.ErrDuplicate) {
		t.Fatalf("renaming onto another keyword: %v", err)
	}
	term = "Al-Basrah"
	if u, err := discovery.UpdateKeyword(ctx, db, b.ID, discovery.KeywordUpdate{Term: &term}); err != nil || u.Normalized != "al basrah" || u.Enabled {
		t.Fatalf("%+v %v", u, err)
	}
	if _, err := discovery.UpdateKeyword(ctx, db, 999999, discovery.KeywordUpdate{Enabled: &off}); !errors.Is(err, discovery.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	// A seed loaded again adds only what is new and leaves a disabled
	// keyword disabled.
	seed := []discovery.SeedKeyword{{Term: "Al Basrah", Language: "en", Group: "basra", Enabled: true}, {Term: "Mosul", Language: "en", Enabled: true}}
	if n, err := discovery.ImportSeed(ctx, db, seed); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if n, err := discovery.ImportSeed(ctx, db, seed); err != nil || n != 0 {
		t.Fatalf("again: %d %v", n, err)
	}
	enabled := true
	list, err := discovery.ListKeywords(ctx, db, discovery.KeywordFilter{Language: "en", Enabled: &enabled})
	if err != nil || len(list) != 2 || list[0].Term != "Erbil" || list[1].Term != "Mosul" {
		t.Fatalf("%+v %v", list, err)
	}
	if err := discovery.DeleteKeyword(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
	if err := discovery.DeleteKeyword(ctx, db, b.ID); !errors.Is(err, discovery.ErrNotFound) {
		t.Fatalf("%v", err)
	}
}

func schedule(t *testing.T, db *pgxpool.Pool, ref string) {
	t.Helper()
	err := pgx.BeginFunc(context.Background(), db, func(tx pgx.Tx) error {
		_, err := dispatch.Register(context.Background(), tx, dispatch.ScheduleEntry{Ref: ref, Enabled: true, Interval: time.Hour,
			NextRun: time.Now().Add(time.Hour), Adaptive: true})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

type entry struct {
	source string
	by     *int64
	next   time.Time
}

func scheduled(t *testing.T, db *pgxpool.Pool, ref string) (entry, bool) {
	t.Helper()
	var e entry
	err := db.QueryRow(context.Background(), `SELECT source, discovered_by_keyword_id, next_run_at FROM page_schedule WHERE page_ref = $1`, ref).
		Scan(&e.source, &e.by, &e.next)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return e, true
}

// The sink records a mention per post: a poster already scheduled (by
// reference or by page id) is matched, a new one is added to the schedule
// as discovered and due now; groups, posts outside the window, text without
// the keyword, erased pages and (when off) profiles are skipped. Recording
// the same results again records nothing twice.
func TestSinkRecord(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	kw, _, _ := discovery.AddKeyword(ctx, db, "Hewlêr", "kmr", "erbil", true)
	schedule(t, db, "v:known")
	schedule(t, db, "v:byid")
	if _, err := db.Exec(ctx, `UPDATE page_schedule SET page_id = 100064000000077 WHERE page_ref = 'v:byid'`); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	q := discovery.Query{KeywordID: kw.ID, Normalized: kw.Normalized, Since: now.Add(-6 * time.Hour), Until: now}
	at := now.Add(-time.Hour)
	results := []discovery.Result{
		{PostID: "1", PostTime: at, OwnerRef: "https://www.facebook.com/Known", OwnerKind: "page", Text: "li Hewlêr"},
		{PostID: "2", PostTime: at, OwnerID: 100064000000077, OwnerRef: "renamed", OwnerKind: "page"},
		{PostID: "3", PostTime: at, OwnerRef: "newpage", OwnerKind: "page", Text: "HEWLER tonight"},
		{PostID: "4", PostTime: at, OwnerID: 100064000000555, OwnerKind: "profile"},
		{PostID: "5", PostTime: at, OwnerRef: "agroup", OwnerKind: "group"},
		{PostID: "6", PostTime: now.Add(-7 * time.Hour), OwnerRef: "old", OwnerKind: "page"},
		{PostID: "7", PostTime: at, OwnerRef: "offtopic", OwnerKind: "page", Text: "nothing about it"},
		{PostID: "8", PostTime: at, OwnerID: 100064000000666, OwnerRef: "erased", OwnerKind: "page"},
		{PostID: "", PostTime: at, OwnerRef: "noid", OwnerKind: "page"},
	}
	sink := &discovery.Sink{DB: db, Cfg: config.Discovery{MaxNewPerDay: 10, IncludeProfiles: true}, Interval: 3 * time.Hour,
		Suppressed: func(id int64) bool { return id == 100064000000666 }}
	st, err := sink.Record(ctx, q, results)
	if err != nil {
		t.Fatal(err)
	}
	want := discovery.Stats{Results: 9, Mentions: 4, Existing: 2, Added: 2, Skipped: 5}
	if st != want {
		t.Fatalf("%+v, want %+v", st, want)
	}
	for _, ref := range []string{"v:newpage", "id:100064000000555"} {
		e, ok := scheduled(t, db, ref)
		if !ok || e.source != "discovery" || e.by == nil || *e.by != kw.ID || e.next.After(time.Now()) {
			t.Errorf("%s: %+v %v", ref, e, ok)
		}
	}
	for _, ref := range []string{"v:agroup", "v:old", "v:offtopic", "v:erased", "v:noid", "v:renamed"} {
		if _, ok := scheduled(t, db, ref); ok {
			t.Errorf("%s was scheduled", ref)
		}
	}
	if e, _ := scheduled(t, db, "v:known"); e.source != "api" {
		t.Errorf("a client's page became %q", e.source)
	}
	ms, err := discovery.ListMentions(ctx, db, discovery.MentionFilter{Since: now.Add(-time.Hour)})
	if err != nil || len(ms) != 4 {
		t.Fatalf("%+v %v", ms, err)
	}
	byPost := map[string]discovery.Mention{}
	for _, m := range ms {
		byPost[m.PostID] = m
	}
	if m := byPost["2"]; !m.MatchedExisting || m.PageRef == nil || *m.PageRef != "v:byid" {
		t.Errorf("matched by page id: %+v", m)
	}
	if m := byPost["3"]; m.MatchedExisting || m.PageRef == nil || *m.PageRef != "v:newpage" || m.OwnerRef != "v:newpage" {
		t.Errorf("added: %+v", m)
	}
	if news, _ := discovery.ListMentions(ctx, db, discovery.MentionFilter{Since: now.Add(-time.Hour), NewOnly: true}); len(news) != 2 {
		t.Errorf("new only: %+v", news)
	}
	var searched *time.Time
	if err := db.QueryRow(ctx, `SELECT last_searched_at FROM discovery_keywords WHERE id = $1`, kw.ID).Scan(&searched); err != nil || searched == nil {
		t.Errorf("last_searched_at not set: %v", err)
	}

	// A retry records nothing twice.
	if st, err := sink.Record(ctx, q, results); err != nil || st.Mentions != 0 || st.Added != 0 {
		t.Fatalf("retry: %+v %v", st, err)
	}

	// Profiles are skipped when include_profiles is off.
	noProfiles := &discovery.Sink{DB: db, Cfg: config.Discovery{MaxNewPerDay: 10}, Interval: time.Hour}
	if st, err := noProfiles.Record(ctx, q, []discovery.Result{{PostID: "9", PostTime: at, OwnerID: 100064000000888, OwnerKind: "profile"}}); err != nil || st.Skipped != 1 {
		t.Fatalf("%+v %v", st, err)
	}

	// A deleted keyword's queued searches are dropped.
	if ok, err := sink.Active(ctx, kw.ID); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	if ok, err := sink.Active(ctx, 424242); err != nil || ok {
		t.Fatalf("%v %v", ok, err)
	}
}

// Past discovery.max_new_per_day the mention is recorded but its poster is
// not added.
func TestSinkDailyCap(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	kw, _, _ := discovery.AddKeyword(ctx, db, "Basra", "en", "", true)
	now := time.Now()
	q := discovery.Query{KeywordID: kw.ID, Normalized: kw.Normalized, Since: now.Add(-time.Hour), Until: now}
	sink := &discovery.Sink{DB: db, Cfg: config.Discovery{MaxNewPerDay: 1, IncludeProfiles: true}, Interval: time.Hour}
	st, err := sink.Record(ctx, q, []discovery.Result{
		{PostID: "1", PostTime: now.Add(-time.Minute), OwnerRef: "first", OwnerKind: "page"},
		{PostID: "2", PostTime: now.Add(-time.Minute), OwnerRef: "second", OwnerKind: "page"},
	})
	if err != nil || st.Added != 1 || st.Capped != 1 || st.Mentions != 2 {
		t.Fatalf("%+v %v", st, err)
	}
	if _, ok := scheduled(t, db, "v:second"); ok {
		t.Fatal("a page past the cap was scheduled")
	}
	ms, _ := discovery.ListMentions(ctx, db, discovery.MentionFilter{Since: now.Add(-time.Hour), NewOnly: true})
	for _, m := range ms {
		if m.PostID == "2" && m.PageRef != nil {
			t.Errorf("capped mention: %+v", m)
		}
	}
	// Found again while still over the cap: nothing new. Found again once
	// there is room (the cap reset): its poster is added.
	results := []discovery.Result{{PostID: "2", PostTime: now.Add(-time.Minute), OwnerRef: "second", OwnerKind: "page"}}
	if st, err := sink.Record(ctx, q, results); err != nil || st.Mentions != 0 || st.Capped != 0 || st.Added != 0 {
		t.Fatalf("still capped: %+v %v", st, err)
	}
	sink.Cfg.MaxNewPerDay = 2
	if st, err := sink.Record(ctx, q, results); err != nil || st.Added != 1 {
		t.Fatalf("after the cap: %+v %v", st, err)
	}
	if _, ok := scheduled(t, db, "v:second"); !ok {
		t.Fatal("the capped poster was not added once there was room")
	}
	ms, _ = discovery.ListMentions(ctx, db, discovery.MentionFilter{Since: now.Add(-time.Hour)})
	for _, m := range ms {
		if m.PostID == "2" && (m.PageRef == nil || *m.PageRef != "v:second" || m.MatchedExisting) {
			t.Errorf("resolved mention: %+v", m)
		}
	}
	if len(ms) != 2 {
		t.Errorf("%d mentions, want 2", len(ms))
	}
}
