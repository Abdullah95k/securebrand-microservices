package spike

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

func jsonl(t *testing.T, recs ...Record) *bytes.Buffer {
	t.Helper()
	var b bytes.Buffer
	for _, r := range recs {
		line, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(append(line, '\n'))
	}
	return &b
}

func ok() *bool { return boolp(true) }

// tierRecords builds one tier's run: pages pages, each with one bootstrap,
// n requests (blocked of them blocked) of wire bytes each, a timeline and a
// comment measurement.
func tierRecords(tier string, cost float64, pages, n, blocked int, wire int64, ratio float64, region func(int) string) []Record {
	recs := []Record{{Type: "run", Tier: tier, RPS: 0.3, CostPerGBUSD: cost}}
	for p := 0; p < pages; p++ {
		page := fmt.Sprintf("v:%s%d", tier, p)
		recs = append(recs,
			Record{Type: "page", Tier: tier, Page: page, Labels: map[string]string{"region": region(p)}},
			Record{Type: "bootstrap", Tier: tier, Page: page, OK: ok(), Wire: wire})
		for i := 0; i < n; i++ {
			class := "ok"
			if i < blocked {
				class = "rate_limited"
			}
			recs = append(recs, Record{Type: "request", Tier: tier, Page: page, Query: "timeline", Class: class, Bytes: int(wire) * 5, Wire: wire})
		}
		recs = append(recs,
			Record{Type: "timeline", Tier: tier, Page: page, Posts: 40, DaysCovered: 400, EndReason: "end"},
			Record{Type: "comments", Tier: tier, Page: page, Ordering: "comments", Reported: 100, Collected: int(ratio * 100)},
			Record{Type: "page_done", Tier: tier, Page: page})
	}
	return recs
}

func TestReportAllPerTier(t *testing.T) {
	alt := func(i int) string {
		if i%2 == 0 {
			return "us"
		}
		return "eu"
	}
	// isp: cheap but blocked 10% of the time; residential: clean.
	isp := tierRecords("isp", 1, 4, 10, 1, 20_000, 0.9, alt)
	res := tierRecords("residential", 5, 4, 10, 0, 20_000, 0.9, alt)
	// A third, clean and cheaper tier with poor completeness must not win.
	dc := tierRecords("datacenter", 0.5, 4, 10, 0, 20_000, 0.3, alt)
	r, err := ReportAll(ReportOptions{Thresholds: DefaultThresholds, RequestsPerDay: 1e6},
		jsonl(t, isp...), jsonl(t, append(res, dc...)...))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Tiers) != 3 || !r.Go || r.Recommended != "residential" {
		t.Fatalf("want GO on residential only: go=%v recommended=%q", r.Go, r.Recommended)
	}
	by := map[string]Summary{}
	for _, s := range r.Tiers {
		by[s.Tier] = s
	}
	if s := by["isp"]; s.Go || len(s.Failed) != 1 || s.Failed[0] != CheckBlockRate || math.Abs(s.BlockRate-0.1) > 1e-9 {
		t.Fatalf("isp: go=%v failed=%v block=%v", s.Go, s.Failed, s.BlockRate)
	}
	if s := by["datacenter"]; s.Go || s.Failed[0] != CheckCompleteness {
		t.Fatalf("datacenter: %v", s.Failed)
	}
	s := by["residential"]
	// 4 pages x (1 bootstrap + 10 requests) x 20 kB.
	if s.WireBytes != 4*11*20_000 || s.WirePerRequest != 20_000 || s.RequestsPerPage != 11 {
		t.Fatalf("traffic: %d %v %v", s.WireBytes, s.WirePerRequest, s.RequestsPerPage)
	}
	// 20 kB x 1M requests/day x 30 days = 600 GB, at $5/GB.
	if math.Abs(s.ProjectedMonthlyUSD-3000) > 1e-6 || math.Abs(s.RunCostUSD-float64(s.WireBytes)/1e9*5) > 1e-12 {
		t.Fatalf("cost: %v %v", s.ProjectedMonthlyUSD, s.RunCostUSD)
	}
	if rows := s.Strata["region"]; len(rows) != 2 || rows[0].Value != "eu" || rows[0].Pages != 2 || rows[0].MedianCompleteness != 0.9 {
		t.Fatalf("strata: %+v", s.Strata)
	}
	out := FormatAll(r)
	for _, want := range []string{"OVERALL: GO (recommended tier: residential)", "by region:", "-> block_rate:", "$3000/month"} {
		if !strings.Contains(out, want) {
			t.Errorf("report misses %q:\n%s", want, out)
		}
	}
}

func TestReportAllNoGo(t *testing.T) {
	one := func(int) string { return "us" }
	r, err := ReportAll(ReportOptions{Thresholds: DefaultThresholds}, jsonl(t, tierRecords("isp", 0, 2, 10, 5, 1000, 1, one)...))
	if err != nil {
		t.Fatal(err)
	}
	if r.Go || r.Recommended != "" || !strings.Contains(FormatAll(r), "OVERALL: NO-GO") {
		t.Fatalf("want NO-GO: %+v", r)
	}
	if !strings.Contains(FormatAll(r), "set cost_per_gb_usd") {
		t.Fatal("missing price hint")
	}
}

func TestRecommendedWithoutPricesUsesBlockRate(t *testing.T) {
	one := func(int) string { return "us" }
	a := tierRecords("a", 0, 2, 100, 3, 1000, 1, one)
	b := tierRecords("b", 0, 2, 100, 1, 1000, 1, one)
	r, _ := ReportAll(ReportOptions{Thresholds: DefaultThresholds}, jsonl(t, append(a, b...)...))
	if r.Recommended != "b" {
		t.Fatalf("want b, got %q", r.Recommended)
	}
}

func TestCappedMeasurementsAreLeftOut(t *testing.T) {
	recs := []Record{
		{Type: "comments", Tier: "a", Page: "v:x", Ordering: "comments", Reported: 100, Collected: 100},
		{Type: "comments", Tier: "a", Page: "v:x", Ordering: "comments", Reported: 100, Collected: 98},
		{Type: "comments", Tier: "a", Page: "v:x", Ordering: "comments", Reported: 1000, Collected: 200, Capped: true},
		{Type: "comments", Tier: "a", Page: "v:x", Ordering: "comments_newest", Reported: 1000, Collected: 300, Capped: true},
	}
	s, err := Report(jsonl(t, recs...), DefaultThresholds)
	if err != nil {
		t.Fatal(err)
	}
	c := s.Completeness["comments"]
	if c.Posts != 3 || c.Capped != 1 || c.Median != 1 || c.MedianAll != 0.98 {
		t.Fatalf("comments: %+v", c)
	}
	// Only capped measurements: the lower bound is reported as is.
	if n := s.Completeness["comments_newest"]; n.Median != 0.3 || n.Capped != 1 {
		t.Fatalf("comments_newest: %+v", n)
	}
	if !strings.Contains(Format(s), "1 hit the spike's limits") {
		t.Fatal(Format(s))
	}
}

// A shallow median passes the depth check only when every timeline really
// ended; one stopped by an error, a block, a drift or the page limit fails it.
func TestDepthCheckNeedsTimelinesThatEnded(t *testing.T) {
	th := Thresholds{MinMedianDays: 365}
	for end, pass := range map[string]bool{"end": true, "no_feed": true, "no_page_id": true, "wall": false, "error": false, "drift": false, "max_pages": false} {
		var b strings.Builder
		for _, p := range []string{"a", "b"} {
			reason := "end"
			if p == "b" {
				reason = end
			}
			fmt.Fprintf(&b, `{"type":"timeline","page":%q,"posts":3,"days_covered":20,"end_reason":%q}`+"\n", p, reason)
		}
		s, err := Report(strings.NewReader(b.String()), th)
		if err != nil {
			t.Fatal(err)
		}
		if failed := slices.Contains(s.Failed, CheckDepth); failed == pass {
			t.Errorf("%s: depth failed %v, want pass %v (%v)", end, failed, pass, s.Verdict)
		}
	}
}

// A page interrupted by shutdown and measured again on resume counts once,
// by its finished attempt; one interrupted and never resumed doesn't count.
func TestReportUsesFinishedAttempts(t *testing.T) {
	in := strings.Join([]string{
		`{"type":"page","page":"a"}`,
		`{"type":"request","page":"a","query":"timeline","class":"Transport","reason":"context canceled"}`,
		`{"type":"bootstrap","page":"a","ok":false,"reason":"context canceled"}`,
		`{"type":"page","page":"b"}`,
		`{"type":"bootstrap","page":"b","ok":false,"reason":"context canceled"}`,
		`{"type":"page","page":"a"}`,
		`{"type":"bootstrap","page":"a","ok":true,"class":"OK"}`,
		`{"type":"request","page":"a","query":"timeline","class":"OK"}`,
		`{"type":"timeline","page":"a","posts":3,"days_covered":400,"end_reason":"end"}`,
		`{"type":"page_done","page":"a"}`,
	}, "\n")
	s, err := Report(strings.NewReader(in), Thresholds{})
	if err != nil {
		t.Fatal(err)
	}
	if s.BootstrapSuccess != 1 || s.BlockRate != 0 || s.TimelineEnd["end"] != 1 {
		t.Fatalf("bootstrap %v, block rate %v, ends %v", s.BootstrapSuccess, s.BlockRate, s.TimelineEnd)
	}
}

// A post whose reply thread hit a wall wasn't read whole: it is left out of
// the ordering's completeness and counted as a wall.
func TestThreadWallsAreNotTheOrderings(t *testing.T) {
	in := strings.Join([]string{
		`{"type":"comments","page":"a","post_id":1,"ordering":"comments","reported":10,"collected":10}`,
		`{"type":"comments","page":"a","post_id":2,"ordering":"comments","reported":10,"collected":2,"thread_end":"wall"}`,
	}, "\n")
	s, err := Report(strings.NewReader(in), Thresholds{})
	if err != nil {
		t.Fatal(err)
	}
	if o := s.Completeness["comments"]; o.Median != 1 || o.Walls != 1 || o.Capped != 1 {
		t.Fatalf("completeness %+v", o)
	}
}
