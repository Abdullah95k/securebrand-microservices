package e2e

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/api"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices/internal/scheduler"
)

// TestDiscoveryAddsPagesThatMentionKeywords: keywords added through the API
// are searched by the scheduler's discovery loop and the crawler's search
// stage. A post by a page already registered is recorded as a mention of
// it; a page that isn't is added to the schedule as discovered, and the
// planner's next run crawls it. A post older than the window adds nothing,
// and searching again records nothing twice.
func TestDiscoveryAddsPagesThatMentionKeywords(t *testing.T) {
	t.Setenv("FBS_E2E_DEBUG", "1") // TEMP: diagnose in CI
	t.Cleanup(func() { slog.SetLogLoggerLevel(slog.LevelInfo) })
	s := Start(t, Options{
		Spec: fakefb.Spec{Seed: 21, Pages: 3, PostsPerPage: 4, PostSpacing: 20 * time.Hour, MaxComments: 6,
			ReplyProbability: 0.1, MaxReplies: 2},
		Discovery: true,
		Config: func(c *config.Config) {
			c.Discovery = config.Discovery{Enabled: true, Interval: time.Hour, Window: 6 * time.Hour, MaxNewPerDay: 10,
				IncludeProfiles: true, Concurrency: 2}
		},
	})
	ctx := context.Background()
	key := s.Key(t, api.ScopePagesWrite, api.ScopeDataRead)
	admin := s.Key(t, api.ScopeAdmin, api.ScopeDataRead)
	pages := s.Fake.World.Pages()
	known, fresh, quiet := pages[0], pages[1], pages[2]

	resp := s.Do(t, key, "POST", "/v1/pages", []byte(`{"page":"`+known.Vanity+`","backfill":false}`))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register: %d", resp.StatusCode)
	}
	type kwResult struct {
		Keyword struct {
			ID int64 `json:"id"`
		} `json:"keyword"`
		Created bool `json:"created"`
	}
	sorani := decodeBody[kwResult](t, s.Do(t, key, "POST", "/v1/discovery/keywords",
		[]byte(`{"term":"هەولێر","language":"ckb","group":"erbil"}`)), http.StatusCreated)
	// The same word typed with a ZWNJ is the same keyword.
	if again := decodeBody[kwResult](t, s.Do(t, key, "POST", "/v1/discovery/keywords",
		[]byte("{\"term\":\"هه\u200cولێر\",\"language\":\"ckb\"}")), http.StatusOK); again.Created || again.Keyword.ID != sorani.Keyword.ID {
		t.Fatalf("variant spelling: %+v", again)
	}
	decodeBody[kwResult](t, s.Do(t, key, "POST", "/v1/discovery/keywords", []byte(`{"term":"Erbil","language":"en"}`)), http.StatusCreated)

	now := time.Now().UTC()
	for _, p := range []struct {
		page *fakefb.Page
		text string
		at   time.Time
	}{
		{known, "Our new branch in Erbil opens today", now.Add(-time.Hour)},
		{fresh, "ئاهەنگی ئەمشەو لە هه\u200cولێر", now.Add(-30 * time.Minute)},
		{quiet, "Erbil, years ago", now.Add(-30 * 24 * time.Hour)},
	} {
		if _, err := s.Fake.World.AddPost(p.page.ID, p.text, p.at); err != nil {
			t.Fatal(err)
		}
	}

	disc := &scheduler.Discovery{DB: s.DB, Producer: s.Producer, Cfg: s.Cfg.Discovery}
	if err := disc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	freshRef, quietRef := "v:"+fresh.Vanity, "v:"+quiet.Vanity
	waitFor(t, "discovery to add the page that mentioned a keyword", time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM page_schedule WHERE page_ref = $1 AND source = 'discovery'
			AND discovered_by_keyword_id = $2`, freshRef, sorani.Keyword.ID) == 1
	})
	waitFor(t, "both searches to finish", time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM discovery_keywords WHERE last_searched_at IS NOT NULL`) == 2
	})
	type mentions struct {
		Mentions []struct {
			PageRef         *string `json:"page_ref"`
			OwnerKind       string  `json:"owner_kind"`
			MatchedExisting bool    `json:"matched_existing"`
		} `json:"mentions"`
	}
	ms := decodeBody[mentions](t, s.Do(t, key, "GET", "/v1/discovery/mentions", nil), http.StatusOK)
	got := map[string]bool{}
	for _, m := range ms.Mentions {
		if m.PageRef != nil {
			got[*m.PageRef] = m.MatchedExisting
		}
	}
	if len(ms.Mentions) != 2 || !got["v:"+known.Vanity] || got[freshRef] {
		t.Fatalf("mentions: %+v", ms)
	}
	if n := count(t, s, `SELECT count(*) FROM page_schedule WHERE page_ref = $1`, quietRef); n != 0 {
		t.Fatal("a post older than the window added its page")
	}
	type pageList struct {
		Pages []struct {
			PageRef string `json:"page_ref"`
			Source  string `json:"source"`
		} `json:"pages"`
	}
	var listed bool
	for _, p := range decodeBody[pageList](t, s.Do(t, admin, "GET", "/v1/pages", nil), http.StatusOK).Pages {
		if p.PageRef == freshRef {
			listed = p.Source == "discovery"
		}
	}
	if !listed {
		t.Fatal("the discovered page is not listed with source discovery")
	}

	// The planner's next run crawls the discovered page.
	if err := s.Planner.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the discovered page's first crawl", 3*time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM jobs WHERE page_ref = $1 AND status = 'completed'`, freshRef) == 1
	})
	if n := count(t, s, `SELECT count(*) FROM posts WHERE page_id = $1`, fresh.ID); n == 0 {
		t.Fatal("the discovered page's posts were not stored")
	}

	// Searching again records nothing twice.
	if err := disc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the second searches", time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM discovery_keywords WHERE last_searched_at >= last_queued_at`) == 2
	})
	if n := count(t, s, `SELECT count(*) FROM discovery_mentions`); n != 2 {
		t.Fatalf("%d mentions after searching again", n)
	}
}
