package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/api"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
)

func (s *Stack) waitJob(t *testing.T, key, id string, d time.Duration) jobView {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		v := decodeBody[jobView](t, s.Do(t, key, "GET", "/v1/jobs/"+id, nil), http.StatusOK)
		switch v.Status {
		case "completed", "partial", "failed", "cancelled":
			return v
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish within %s", id, d)
	return jobView{}
}

// TestAdaptiveScheduleAndPostRefresh registers a page without an interval:
// after its backfill the page is tiered by its posting rate and its posts
// are registered for refresh. New comments on a post older than the
// timeline overlap are then collected by the next scheduled job, which
// carries that post's refresh, although its timeline scan does not reach
// the post.
func TestAdaptiveScheduleAndPostRefresh(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 11, Pages: 1, PostsPerPage: 8, PostSpacing: 20 * time.Hour,
		MaxComments: 20, ReplyProbability: 0.2, MaxReplies: 5, Depth2Prob: 0.1}})
	ctx := context.Background()
	key := s.Key(t, api.ScopePagesWrite, api.ScopeJobsWrite, api.ScopeDataRead)
	page := s.Fake.World.Pages()[0]

	type registered struct {
		Backfill jobView `json:"backfill_job"`
	}
	reg := decodeBody[registered](t, s.Do(t, key, "POST", "/v1/pages", []byte(fmt.Sprintf(`{"page":%q}`, page.Vanity))), http.StatusCreated)
	bf := s.waitJob(t, key, reg.Backfill.ID, 3*time.Minute)
	if bf.Status != "completed" {
		t.Fatalf("backfill: %+v", bf)
	}

	// 8 posts 20h apart: 1.2 posts a day is tier T1 (12h).
	var tier string
	var interval int
	var adaptive bool
	var rate float64
	if err := s.DB.QueryRow(ctx, `SELECT tier, interval_seconds, adaptive, posts_per_day FROM page_schedule WHERE page_id = $1`, page.ID).
		Scan(&tier, &interval, &adaptive, &rate); err != nil {
		t.Fatal(err)
	}
	t.Logf("tier %s, interval %ds, %.2f posts/day", tier, interval, rate)
	if tier != "T1" || interval != 12*3600 || !adaptive {
		t.Fatalf("want adaptive T1 / 12h, got %s / %ds (adaptive=%v)", tier, interval, adaptive)
	}
	if n := count(t, s, `SELECT count(*) FROM post_refresh WHERE page_id = $1`, page.ID); n != len(page.Posts) {
		t.Fatalf("post_refresh rows %d, want %d", n, len(page.Posts))
	}

	// New comments on a post two to six days old that already has comments,
	// so the refresh reads newest-first from the stored watermark.
	var target *fakefb.Post
	before := 0
	for _, p := range page.Posts {
		age := time.Since(p.CreatedAt)
		if p.Pinned || age < 48*time.Hour || age > 144*time.Hour {
			continue
		}
		if n := count(t, s, `SELECT count(*) FROM comments WHERE post_id = $1`, p.ID); n > 0 {
			target, before = p, n
			break
		}
	}
	if target == nil {
		t.Fatal("no post two to six days old with comments")
	}
	if err := s.Fake.World.AddComments(target.ID, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE page_schedule SET next_run_at = now() - interval '1 second' WHERE page_id = $1`, page.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Planner.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var jobID string
	if err := s.DB.QueryRow(ctx, `SELECT last_job_id::text FROM page_schedule WHERE page_id = $1`, page.ID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	j := s.waitJob(t, key, jobID, 3*time.Minute)
	after := count(t, s, `SELECT count(*) FROM comments WHERE post_id = $1`, target.ID)
	var refreshes int
	if err := s.DB.QueryRow(ctx, `SELECT refreshes FROM post_refresh WHERE post_id = $1`, target.ID).Scan(&refreshes); err != nil {
		t.Fatal(err)
	}
	truth, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	want, wantTarget := 0, 0
	for _, p := range truth.Posts {
		want += p.TotalComments
		if p.PostID == target.ID {
			wantTarget = p.TotalComments
		}
	}
	t.Logf("scheduled job %s %s: %d refreshes of the target, comments %d -> %d (truth %d), counters %v",
		j.ID, j.Status, refreshes, before, after, wantTarget, j.Counters)
	if j.Status != "completed" || refreshes < 1 || after != wantTarget || after <= before {
		t.Fatalf("the refresh did not collect the new comments: status=%s refreshes=%d comments %d -> %d, truth %d",
			j.Status, refreshes, before, after, wantTarget)
	}
	if got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1`, page.ID); got != want {
		t.Fatalf("comments %d, want %d", got, want)
	}
}

// TestOnDemandAdmission checks the fleet-wide cap on running on-demand jobs.
func TestOnDemandAdmission(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 3, Pages: 2, PostsPerPage: 2, PostSpacing: 20 * time.Hour, MaxComments: 3},
		Config: func(c *config.Config) { c.API.MaxRunningP0Jobs = 1 }})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	pages := s.Fake.World.Pages()
	if _, err := s.DB.Exec(context.Background(), `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status)
		VALUES (gen_random_uuid(), 'on_demand', 1, 'full', 'v:busy', '{}', 'running')`); err != nil {
		t.Fatal(err)
	}
	r := s.Do(t, key, "POST", "/v1/jobs", []byte(fmt.Sprintf(`{"page":%q}`, pages[0].Vanity)))
	_ = r.Body.Close()
	if r.StatusCode != http.StatusTooManyRequests || r.Header.Get("Retry-After") == "" {
		t.Fatalf("want 429 with Retry-After, got %d %q", r.StatusCode, r.Header.Get("Retry-After"))
	}
}
