package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/abdullah95k/securebrand-microservices/internal/api"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

type accountView struct {
	Ref       string     `json:"account_ref"`
	Status    string     `json:"status"`
	Reason    *string    `json:"reason"`
	Kind      string     `json:"kind"`
	Vias      []string   `json:"vias"`
	Mentions  int64      `json:"mentions"`
	Pages     int        `json:"pages"`
	InQueue   bool       `json:"in_queue"`
	Checks    int        `json:"checks"`
	Failures  int        `json:"failures"`
	LastError *string    `json:"last_error"`
	CheckedAt *time.Time `json:"checked_at"`
	RecheckAt *time.Time `json:"recheck_at"`
	AddedAt   *time.Time `json:"added_at"`
	PageRef   *string    `json:"page_ref"`
	FirstPost *int64     `json:"first_post_id"`
	Keyword   *string    `json:"keyword"`
}

// TestMentionsCheckAccountsAndAddPublicOnes runs the mentions flow against
// fakefb: a page we crawl tags a public page and a private profile, links a
// restricted page, a missing one and a page already on our list, shares a
// post of another public page, and a public profile comments on it. The
// persister records the mentions, the leader counts them and queues checks,
// and the crawler's check stage loads each account. The public ones are
// added (two a day: the third waits for the next day) and crawled; the
// private profile is recorded until its recheck; the restricted page fails
// its first check (an unconfirmed "names nothing", recorded as nothing) and
// is found restricted by the second; the missing page is unavailable; the
// listed page is only counted. A candidate from another tool goes through
// the same checks; a recheck asked for through the API runs; an erased
// person leaves the list and is never taken in again.
func TestMentionsCheckAccountsAndAddPublicOnes(t *testing.T) {
	s := Start(t, Options{
		Spec:     fakefb.Spec{Seed: 31, Pages: 2, PostsPerPage: 2, PostSpacing: 20 * time.Hour, MaxComments: 0},
		Mentions: true,
		Config: func(c *config.Config) {
			c.Mentions = config.Mentions{Enabled: true, IncludeCommentAuthors: true, MaxChecksPerDay: 1000, MaxNewPerDay: 2,
				TimeZone: "Asia/Baghdad", RecheckAfter: 4380 * time.Hour, RetryBackoff: time.Second, RetryMaxBackoff: 2 * time.Second,
				Concurrency: 2, CandidateWeight: 2, RetentionDays: 180}
		},
	})
	ctx := context.Background()
	w := s.Fake.World
	brands := w.Pages()
	ours, listed := brands[0], brands[1]
	pubPage := w.AddAccount(fakefb.AccountSpec{Vanity: "publicpage1", Name: "Public Page", Posts: 2})
	pubPerson := w.AddAccount(fakefb.AccountSpec{Vanity: "public.person", Name: "Pub Person", Profile: true, Posts: 2})
	privPerson := w.AddAccount(fakefb.AccountSpec{Vanity: "private.person", Name: "Priv Person", Profile: true, Private: true, Posts: 2})
	w.AddAccount(fakefb.AccountSpec{Vanity: "restricted.page", Name: "Restricted", Restricted: true})
	shared := w.AddAccount(fakefb.AccountSpec{Vanity: "sharedpage1", Name: "Shared Page", Posts: 1})
	// A page without posts: public all the same.
	w.AddAccount(fakefb.AccountSpec{Vanity: "candidate.page", Name: "Candidate"})
	id := func(p *fakefb.Page) string { return strconv.FormatInt(p.ID, 10) }

	now := time.Now().UTC()
	post, err := w.AddMentionPost(ours.ID, "With Public Page! facebook.com/restricted.page facebook.com/no.such.page facebook.com/"+listed.Vanity,
		now.Add(-2*time.Hour), []fakefb.Tag{{Offset: 5, Length: 11, Type: "Page", ID: id(pubPage), URL: s.FakeURL + "/" + pubPage.Vanity}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AddMentionPost(ours.ID, "Look", now.Add(-time.Hour), nil, shared.Posts[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AddMentionComment(post.ID, fakefb.Author{ID: pubPerson.ID, Name: pubPerson.Name, Vanity: pubPerson.Vanity},
		"Thanks Priv Person", []fakefb.Tag{{Offset: 7, Length: 11, Type: "User", ID: id(privPerson), URL: s.FakeURL + "/" + privPerson.Vanity}}); err != nil {
		t.Fatal(err)
	}

	writer := s.Key(t, api.ScopePagesWrite, api.ScopeDataRead)
	admin := s.Key(t, api.ScopeAdmin)
	for _, p := range []*fakefb.Page{ours, listed} {
		resp := s.Do(t, writer, "POST", "/v1/pages", []byte(`{"page":"`+p.Vanity+`","backfill":`+strconv.FormatBool(p == ours)+`}`))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Vanity, resp.StatusCode)
		}
	}
	waitFor(t, "our page's backfill", 3*time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM jobs WHERE page_ref = $1 AND status = 'completed'`, "v:"+ours.Vanity) == 1
	})
	refs := map[string]string{
		"page": "id:" + id(pubPage), "person": "id:" + id(pubPerson), "private": "id:" + id(privPerson), "shared": "id:" + id(shared),
		"restricted": "v:restricted.page", "missing": "v:no.such.page", "listed": "v:" + listed.Vanity,
	}
	waitFor(t, "the mentions counted", time.Minute, func() bool {
		if err := s.Mentions.Aggregate(ctx); err != nil {
			t.Fatal(err)
		}
		return count(t, s, `SELECT count(*) FROM checked_accounts`) == len(refs)
	})
	get := func(ref string) accountView {
		t.Helper()
		return decodeBody[struct {
			Account accountView `json:"account"`
		}](t, s.Do(t, admin, "GET", "/v1/discovery/accounts/"+url.PathEscape(ref), nil), http.StatusOK).Account
	}
	if a := get(refs["listed"]); a.Status != mentions.StatusListed || a.InQueue || a.Mentions != 1 {
		t.Fatalf("listed page: %+v", a)
	}
	if a := get(refs["person"]); len(a.Vias) != 1 || a.Vias[0] != mentions.ViaAuthor || a.Pages != 1 {
		t.Fatalf("comment author: %+v", a)
	}

	// First round of checks: every account once.
	queueAndWait := func(what string) {
		t.Helper()
		if err := s.Mentions.Queue(ctx); err != nil {
			t.Fatal(err)
		}
		waitFor(t, what, 2*time.Minute, func() bool {
			return count(t, s, `SELECT count(*) FROM checked_accounts WHERE queued_at IS NOT NULL`) == 0
		})
	}
	queueAndWait("the first checks")
	r := get(refs["restricted"])
	if r.Status != mentions.StatusUnchecked || r.CheckedAt != nil || !r.InQueue || r.Failures != 1 || r.LastError == nil ||
		*r.LastError != "names_nothing" || r.Checks != 1 {
		t.Fatalf("an unconfirmed restriction was recorded as a check: %+v", r)
	}
	if n := testutil.ToFloat64(metrics.MentionCheckFailures.WithLabelValues("page", "names_nothing")); n < 1 {
		t.Fatalf("the failed check isn't counted by step and reason: %v", n)
	}
	time.Sleep(1100 * time.Millisecond) // retry_backoff
	queueAndWait("the restriction's second check")

	want := map[string][2]string{
		"page": {mentions.StatusPublic, "posts_visible"}, "person": {mentions.StatusPublic, "posts_visible"},
		"shared":  {mentions.StatusPublic, "posts_visible"},
		"private": {mentions.StatusPrivate, "no_public_posts"}, "restricted": {mentions.StatusPrivate, "restricted"},
		"missing": {mentions.StatusUnavailable, "not_found"},
	}
	added := 0
	for name, st := range want {
		a := get(refs[name])
		if a.Status != st[0] || a.Reason == nil || *a.Reason != st[1] || a.InQueue {
			t.Fatalf("%s: %+v", name, a)
		}
		if a.Status == mentions.StatusPublic && a.AddedAt != nil {
			added++
		}
		if a.Status != mentions.StatusPublic && (a.RecheckAt == nil || a.RecheckAt.Sub(*a.CheckedAt) != 4380*time.Hour || a.FirstPost != nil) {
			t.Fatalf("%s: not-public record: %+v", name, a)
		}
	}
	if added != 2 {
		t.Fatalf("%d accounts added, want the daily cap of 2", added)
	}
	// The kinds the accounts' own pages show (a person's has no Page
	// object; a page's has one).
	if a := get(refs["person"]); a.Kind != mentions.KindProfile {
		t.Fatalf("public profile: %+v", a)
	}
	if a := get(refs["page"]); a.Kind != mentions.KindPage {
		t.Fatalf("public page: %+v", a)
	}
	if n := count(t, s, `SELECT checks FROM mention_check_days`); n != 7 {
		t.Fatalf("%d checks counted, want 7 (six accounts, the restricted one twice)", n)
	}

	// The private profile is listed as such, without its reference for a
	// key without pii:read.
	type list struct {
		Accounts []accountView `json:"accounts"`
	}
	priv := decodeBody[list](t, s.Do(t, admin, "GET", "/v1/discovery/accounts?status=private&order=checked", nil), http.StatusOK)
	if len(priv.Accounts) != 2 {
		t.Fatalf("private accounts: %+v", priv)
	}
	for _, a := range decodeBody[list](t, s.Do(t, writer, "GET", "/v1/discovery/accounts?status=private", nil), http.StatusOK).Accounts {
		if a.Ref != "" {
			t.Fatalf("a person's reference without pii:read: %+v", a)
		}
	}
	if resp := s.Do(t, writer, "GET", "/v1/discovery/accounts/"+refs["private"], nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a person's record without pii:read: %d", resp.StatusCode)
	}

	// What was added, and why.
	type pageList struct {
		Pages []struct {
			PageRef string          `json:"page_ref"`
			Source  string          `json:"source"`
			Via     json.RawMessage `json:"discovered_via"`
		} `json:"pages"`
	}
	addedPages := decodeBody[pageList](t, s.Do(t, admin, "GET", "/v1/pages?source=mention", nil), http.StatusOK).Pages
	if len(addedPages) != 2 {
		t.Fatalf("added pages: %+v", addedPages)
	}
	for _, p := range addedPages {
		var via mentions.Via
		if err := json.Unmarshal(p.Via, &via); err != nil || via.Mentions < 1 || len(via.Vias) == 0 || via.Source != mentions.SourceMention {
			t.Fatalf("why %s: %s %v", p.PageRef, p.Via, err)
		}
	}

	// A candidate from another tool: the same check; public, but the day's
	// adds are used up. The next day, it is added.
	tool := s.Key(t, api.ScopeDiscoveryWrite)
	type candResult struct {
		Ref     string `json:"account_ref"`
		Created bool   `json:"created"`
		Result  string `json:"result"`
	}
	cr := decodeBody[candResult](t, s.Do(t, tool, "POST", "/v1/discovery/candidates",
		[]byte(`{"ref":"https://www.facebook.com/candidate.page","source":"google_search","keyword":"erbil","evidence_url":"https://example.org/q"}`)),
		http.StatusAccepted)
	if cr.Ref != "v:candidate.page" || !cr.Created || cr.Result != "queued" {
		t.Fatalf("candidate: %+v", cr)
	}
	if resp := s.Do(t, tool, "GET", "/v1/discovery/accounts", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("the candidates key reads the list: %d", resp.StatusCode)
	}
	queueAndWait("the candidate's check")
	if a := get("v:candidate.page"); a.Status != mentions.StatusPublic || a.Reason == nil || *a.Reason != "page" || a.AddedAt != nil ||
		a.Keyword == nil || *a.Keyword != "erbil" {
		t.Fatalf("candidate checked: %+v", a)
	}
	s.Mentions.Adder.Now = func() time.Time { return time.Now().Add(24 * time.Hour) }
	if err := s.Mentions.Queue(ctx); err != nil {
		t.Fatal(err)
	}
	// The next day's two: the public account the cap held back, and the
	// candidate.
	for _, ref := range []string{refs["page"], refs["person"], refs["shared"], "v:candidate.page"} {
		if a := get(ref); a.AddedAt == nil || a.PageRef == nil {
			t.Fatalf("%s the next day: %+v", ref, a)
		}
	}
	s.Mentions.Adder.Now = nil

	// The added page is crawled like any other.
	if err := s.Planner.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the added page's first crawl", 3*time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM jobs WHERE page_ref = $1 AND status = 'completed'`, refs["page"]) == 1
	})
	if n := count(t, s, `SELECT count(*) FROM posts WHERE page_id = $1`, pubPage.ID); n == 0 {
		t.Fatal("the added page's posts were not stored")
	}

	// A recheck asked for: checked again at once, whatever the 6 months.
	resp := s.Do(t, writer, "POST", "/v1/discovery/accounts/"+refs["private"]+"/recheck", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("recheck: %d", resp.StatusCode)
	}
	queueAndWait("the recheck")
	if a := get(refs["private"]); a.Checks != 2 || a.Status != mentions.StatusPrivate || a.InQueue {
		t.Fatalf("after the recheck: %+v", a)
	}
	if resp := s.Do(t, writer, "POST", "/v1/discovery/accounts/"+refs["listed"]+"/recheck", nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("recheck of a listed page: %d", resp.StatusCode)
	}

	// An erased person leaves the list, and is never taken in again.
	er := decodeBody[api.Erasure](t, s.Do(t, admin, "POST", "/v1/erasure-requests",
		[]byte(`{"subject_type":"author","author_id":"`+id(privPerson)+`"}`)), http.StatusAccepted)
	if err := s.Eraser.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT count(*) FROM erasure_requests WHERE id = $1 AND status = 'done'`, er.ID); n != 1 {
		t.Fatal("erasure not done")
	}
	if n := count(t, s, `SELECT count(*) FROM checked_accounts WHERE account_ref = $1 OR account_id = $2`, refs["private"], privPerson.ID); n != 0 {
		t.Fatal("the erased person is still in the list")
	}
	if n := count(t, s, `SELECT count(*) FROM mention_pages WHERE account_ref = $1`, refs["private"]); n != 0 {
		t.Fatal("the erased person's counts are kept")
	}
	again := decodeBody[candResult](t, s.Do(t, tool, "POST", "/v1/discovery/candidates",
		[]byte(`{"ref":"https://www.facebook.com/profile.php?id=`+id(privPerson)+`","source":"google_search"}`)), http.StatusAccepted)
	if again.Result != "ignored" || count(t, s, `SELECT count(*) FROM checked_accounts WHERE account_ref = $1`, refs["private"]) != 0 {
		t.Fatalf("an erased person was taken in again: %+v", again)
	}
}
