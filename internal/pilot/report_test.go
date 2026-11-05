package pilot

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

func TestFormatProjects(t *testing.T) {
	since := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	r := Report{Since: since, Until: since.Add(48 * time.Hour), Posts: 200, MeanCompleteness: 0.93, SettledPosts: 120, SettledCompleteness: 0.97,
		Rows: []Row{
			{Tier: "T0", Kind: "backfill", Pages: 50, Jobs: 50, Requests: 14000, WireBytes: 1_100_000_000},
			{Tier: "T0", Kind: "scheduled", Pages: 50, Jobs: 800, Requests: 40000, WireBytes: 3_000_000_000, Blocked: 20},
			{Tier: "T2", Kind: "scheduled", Pages: 100, Jobs: 1600, Requests: 8000, WireBytes: 400_000_000},
		}}
	st := r.Steady()
	if st.Pages != 150 || st.Requests != 48000 || st.Blocked != 20 {
		t.Fatalf("steady state sums the scheduled jobs only: %+v", st)
	}
	out := Format(r, Projection{Pages: 5000, IdentityRPS: 0.33, CostPerGB: 4, Headroom: 0.5})
	t.Log("\n" + out)
	// 48000 requests over 150 pages and 2 days = 160 per page per day.
	for _, want := range []string{"over 2.0 days): 160 requests and 11.3 MB per page per day", "block rate 0.04%",
		"93.0% over 200 posts", "97.0% over the 120", "for 5000 pages: 0.80M requests and 56.67 GB per day",
		"about 43 IPs at 0.33 requests/s each (50% headroom)", "$227 a day"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	// The distinct pages count, when known.
	r.SteadyPages = 120
	if st := r.Steady(); st.Pages != 120 {
		t.Fatalf("distinct pages: %+v", st)
	}
	// A pilot younger than --since: the steady state is averaged from the
	// first scheduled job, not over hours that had none.
	r.SteadyPages = 0
	r.Since = since.Add(-24 * time.Hour)
	r.SteadySince = since
	if d := r.SteadyDays(); d != 2 {
		t.Fatalf("steady days %v, want 2", d)
	}
	if out := Format(r, Projection{}); !strings.Contains(out, "over 2.0 days): 160 requests") {
		t.Fatalf("clamped span:\n%s", out)
	}
	// A report run after the pilot stopped: the steady state ends with the
	// last scheduled job, not spread over the idle hours since.
	r.Until, r.SteadyUntil = since.Add(5*24*time.Hour), since.Add(48*time.Hour)
	if d := r.SteadyDays(); d != 2 {
		t.Fatalf("steady days after the pilot stopped: %v, want 2", d)
	}
	if !strings.Contains(Format(Report{Since: since, Until: since.Add(time.Hour)}, Projection{}), "no scheduled jobs yet") {
		t.Fatal("an empty pilot says why")
	}
}

// Each IP loads at most one page per interval, so the page loads can need
// more IPs than the requests: the plan takes the larger. Jobs from before
// page loads were counted say so rather than project none.
func TestFormatProjectsPageLoads(t *testing.T) {
	since := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	r := Report{Since: since, Until: since.Add(24 * time.Hour), Rows: []Row{
		// 300 pages, 4 page loads and 450 requests each a day.
		{Tier: "T1", Kind: "scheduled", Pages: 300, Jobs: 2400, Requests: 135000, PageLoads: 1200, WireBytes: 8_000_000_000},
	}}
	p := Projection{Pages: 5000, IdentityRPS: 0.5, Headroom: 0.5, PageLoadInterval: 2 * time.Minute, ChecksPerDay: 300}
	out := Format(r, p)
	t.Log("\n" + out)
	// 4 x 5000 + 300 = 20,300 page loads; 720 per IP a day; x1.5 = 42.3.
	// Requests: 450 x 5000 / 86400 / 0.5 x 1.5 = 78.1.
	for _, want := range []string{"450 requests, 4.0 page loads and", "about 79 IPs at 0.5 requests/s",
		"page loads: 20.3K a day (300 of them mention checks); at one per 2m per IP (720 a day each), about 43 IPs (50% headroom)",
		"plan on at least 79 IPs"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	// A slower gate (one per 10m): the page loads set the plan.
	p.PageLoadInterval = 10 * time.Minute
	if out := Format(r, p); !strings.Contains(out, "about 212 IPs (50% headroom)") || !strings.Contains(out, "plan on at least 212 IPs") {
		t.Fatalf("page loads should set the plan:\n%s", out)
	}
	r.Rows[0].PageLoads = 0
	if out := Format(r, p); !strings.Contains(out, "page loads: not counted by these jobs") || strings.Contains(out, "plan on") {
		t.Fatalf("uncounted page loads:\n%s", out)
	}
}

// Three cycles of a page every 3 hours cover 9 hours: the daily traffic is
// the cycles' over 9 hours, not over the 6 between the first one's start
// and the last one's end.
func TestSteadyStateCoversWholeCycles(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	t0 := time.Now().UTC().Add(-7 * time.Hour).Truncate(time.Second)
	// Page b, on a long interval of its own (and no requests), doesn't
	// stretch the span.
	if _, err := db.Exec(ctx, `INSERT INTO page_schedule (page_ref, interval_seconds, next_run_at) VALUES
		('v:a', 10800, now()), ('v:b', 2592000, now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status, counters, created_at, finished_at)
		VALUES (gen_random_uuid(), 'scheduled', 1, 'incremental', 'v:b', '{}', 'completed', '{"requests":0}', $1, $1)`, t0); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		created := t0.Add(time.Duration(i) * 3 * time.Hour)
		if _, err := db.Exec(ctx, `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status, counters, created_at, finished_at)
			VALUES (gen_random_uuid(), 'scheduled', 1, 'incremental', 'v:a', '{}', 'completed', '{"requests":100,"page_loads":2}', $1, $2)`,
			created, created.Add(10*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := Load(ctx, db, t0.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if d := r.SteadyDays(); math.Abs(d-9.0/24) > 1e-9 {
		t.Fatalf("steady span %.4f days, want %.4f", d, 9.0/24)
	}
	if perDay := float64(r.Steady().Requests) / r.SteadyDays(); math.Abs(perDay-800) > 1e-6 {
		t.Fatalf("%.1f requests a day, want 800 (100 per 3 hours)", perDay)
	}
	if n := r.Steady().PageLoads; n != 6 {
		t.Fatalf("%d page loads, want 6", n)
	}
}
