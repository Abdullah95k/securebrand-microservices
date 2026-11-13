package spike

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
)

// The keyword-search feasibility test (D-065; the master's note of 13 Nov):
// whether Facebook's search answers logged-out visitors at all. One
// identity on one exit loads Facebook's search pages for a few keywords,
// one page load at a time, at least Interval apart (the exits' own pace),
// and stops at the first wall, rate limit or checkpoint: nothing is worked
// around. It records counts only: never the keywords (keyword 1, 2, ...),
// never names.

// SearchSurface is a search page: Path plus the escaped keyword.
type SearchSurface struct{ Name, Path string }

// SearchSurfaces are the search pages loaded for each keyword.
var SearchSurfaces = []SearchSurface{
	{"search_pages", "/search/pages/?q="},
	{"search_top", "/search/top/?q="},
}

// Search probe verdicts.
const (
	SearchWalled    = "walled"     // Facebook stopped it: no search logged out
	SearchResults   = "results"    // pages came back without a wall
	SearchNoResults = "no_results" // loaded, but no page in the answers
	SearchError     = "error"      // fbs or the proxy failed; says nothing about Facebook
)

// SearchProbeOptions configure a probe.
type SearchProbeOptions struct {
	Keywords []string
	Tier     string
	Interval time.Duration   // between page loads; default 2m
	Surfaces []SearchSurface // default SearchSurfaces
	Sleep    func(context.Context, time.Duration) error
}

// SearchLoad is one page load's outcome.
type SearchLoad struct {
	Keyword int    `json:"keyword"` // 1-based; the keyword itself is not recorded
	Surface string `json:"surface"`
	Class   string `json:"class"`
	Status  int    `json:"status"`
	Bytes   int64  `json:"bytes"`
	PageIDs int    `json:"page_ids"` // distinct ids typed Page in the answer
	Error   string `json:"error,omitempty"`
}

// SearchProbeResult is the probe's report.
type SearchProbeResult struct {
	Verdict string       `json:"verdict"`
	Stopped string       `json:"stopped,omitempty"` // the class that stopped it
	Loads   []SearchLoad `json:"loads"`
}

var searchPageIDRe = regexp.MustCompile(`"__typename":"Page","id":"(\d+)"|"id":"(\d+)","__typename":"Page"`)

// searchStops are the classes that end the probe: Facebook said no.
func searchStops(c detect.Class) bool {
	return c == detect.LoginWall || c == detect.Checkpoint || c == detect.RateLimited
}

// SearchProbe runs the test with an identity of o.Tier.
func SearchProbe(ctx context.Context, pool *session.Pool, loader *session.Loader, o SearchProbeOptions) SearchProbeResult {
	if o.Interval <= 0 {
		o.Interval = 2 * time.Minute
	}
	sleep := o.Sleep
	if sleep == nil {
		sleep = func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		}
	}
	surfaces := o.Surfaces
	if len(surfaces) == 0 {
		surfaces = SearchSurfaces
	}
	res := SearchProbeResult{Verdict: SearchNoResults, Loads: []SearchLoad{}}
	id, err := pool.Acquire(ctx, o.Tier)
	if err != nil {
		res.Verdict = SearchError
		res.Loads = append(res.Loads, SearchLoad{Error: Redact(err.Error())})
		return res
	}
	defer pool.Release(id)
	first := true
	for i, kw := range o.Keywords {
		for _, s := range surfaces {
			if !first {
				if err := sleep(ctx, o.Interval); err != nil {
					res.Verdict = SearchError
					return res
				}
			}
			first = false
			l := SearchLoad{Keyword: i + 1, Surface: s.Name}
			u, _ := id.Base.Parse(s.Path + url.QueryEscape(strings.TrimSpace(kw)))
			pl, err := loader.Load(ctx, id, u.String())
			if err != nil {
				l.Class, l.Error = detect.Transport.String(), Redact(err.Error())
				res.Loads = append(res.Loads, l)
				res.Verdict, res.Stopped = SearchError, l.Class
				return res
			}
			l.Class, l.Status, l.Bytes = pl.Class.String(), pl.Status, pl.Bytes
			l.PageIDs = countPageIDs(pl.HTML)
			res.Loads = append(res.Loads, l)
			if searchStops(pl.Class) {
				res.Verdict, res.Stopped = SearchWalled, l.Class
				return res
			}
			if l.PageIDs > 0 {
				res.Verdict = SearchResults
			}
		}
	}
	return res
}

func countPageIDs(html []byte) int {
	seen := map[string]bool{}
	for _, m := range searchPageIDRe.FindAllSubmatch(html, -1) {
		id := string(m[1]) + string(m[2])
		seen[id] = true
	}
	return len(seen)
}

// FormatSearchProbe prints the report.
func FormatSearchProbe(r SearchProbeResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "verdict: %s", r.Verdict)
	if r.Stopped != "" {
		fmt.Fprintf(&b, " (stopped by %s)", r.Stopped)
	}
	b.WriteString("\n")
	for _, l := range r.Loads {
		fmt.Fprintf(&b, "keyword %d  %-12s  %-20s  status %3d  %8d bytes  %d page ids", l.Keyword, l.Surface, l.Class, l.Status, l.Bytes, l.PageIDs)
		if l.Error != "" {
			fmt.Fprintf(&b, "  %s", l.Error)
		}
		b.WriteString("\n")
	}
	return b.String()
}
