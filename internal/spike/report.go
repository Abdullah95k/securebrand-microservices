package spike

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

// Thresholds are the Phase 0 go/no-go criteria.
type Thresholds struct {
	MinMedianCompleteness float64 // e.g. 0.70
	MinBootstrapSuccess   float64 // e.g. 0.90
	MaxBlockRate          float64 // e.g. 0.05
	MinMedianDays         float64 // e.g. 365 (or the reduced default you accept)
}

// DefaultThresholds match the plan.
var DefaultThresholds = Thresholds{MinMedianCompleteness: 0.70, MinBootstrapSuccess: 0.90, MaxBlockRate: 0.05, MinMedianDays: 365}

// Summary is the aggregated result.
type Summary struct {
	Pages              int                        `json:"pages"`
	BootstrapSuccess   float64                    `json:"bootstrap_success"`
	ConsentWalls       int                        `json:"consent_walls"`
	TokensFoundRate    map[string]float64         `json:"tokens_found_rate"`
	Requests           int                        `json:"requests"`
	ClassRate          map[string]float64         `json:"class_rate"`
	BlockRate          float64                    `json:"block_rate"`
	TimelineEnd        map[string]int             `json:"timeline_end"`
	MedianPosts        float64                    `json:"median_posts"`
	MedianDays         float64                    `json:"median_days"`
	Completeness       map[string]OrderingSummary `json:"completeness"`
	BytesByQuery       map[string]ByteStats       `json:"bytes_by_query"`
	Portability        float64                    `json:"cursor_portability"`
	MediaSuccess       float64                    `json:"media_success"`
	MedianMediaExpiryH float64                    `json:"median_media_expiry_hours"`
	Verdict            []string                   `json:"verdict"`
	Go                 bool                       `json:"go"`
}

type OrderingSummary struct {
	Posts  int     `json:"posts"`
	Median float64 `json:"median"`
	P10    float64 `json:"p10"`
	Walls  int     `json:"walls"`
}

type ByteStats struct {
	N    int     `json:"n"`
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
}

func quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Round(q * float64(len(s)-1)))
	return s[i]
}

// Report aggregates JSONL records.
func Report(in io.Reader, th Thresholds) (Summary, error) {
	s := Summary{TokensFoundRate: map[string]float64{}, ClassRate: map[string]float64{}, TimelineEnd: map[string]int{},
		Completeness: map[string]OrderingSummary{}, BytesByQuery: map[string]ByteStats{}}
	var boots, bootOK, port, portOK, med, medOK int
	tokens := map[string]int{}
	classes := map[string]int{}
	var posts, days, expiry []float64
	ratios := map[string][]float64{}
	walls := map[string]int{}
	bytes := map[string][]float64{}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	pages := map[string]bool{}
	for sc.Scan() {
		var r Record
		if json.Unmarshal(sc.Bytes(), &r) != nil {
			continue
		}
		pages[r.Page] = true
		switch r.Type {
		case "bootstrap":
			boots++
			if r.OK != nil && *r.OK {
				bootOK++
			}
			if r.Consent {
				s.ConsentWalls++
			}
			for k, v := range r.TokensFound {
				if v {
					tokens[k]++
				}
			}
		case "request":
			s.Requests++
			classes[r.Class]++
			if r.Bytes > 0 {
				bytes[r.Query] = append(bytes[r.Query], float64(r.Bytes))
			}
		case "timeline":
			s.TimelineEnd[r.EndReason]++
			posts = append(posts, float64(r.Posts))
			days = append(days, r.DaysCovered)
		case "comments":
			ratio := 1.0
			if r.Reported > 0 {
				ratio = math.Min(1, float64(r.Collected)/float64(r.Reported))
			}
			ratios[r.Ordering] = append(ratios[r.Ordering], ratio)
			if r.EndReason == "wall" {
				walls[r.Ordering]++
			}
		case "portability":
			port++
			if r.OK != nil && *r.OK {
				portOK++
			}
		case "media":
			med++
			if r.OK != nil && *r.OK {
				medOK++
			}
			if r.ExpiresInH != 0 {
				expiry = append(expiry, r.ExpiresInH)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return s, err
	}
	s.Pages = len(pages)
	if boots > 0 {
		s.BootstrapSuccess = float64(bootOK) / float64(boots)
		for k, v := range tokens {
			s.TokensFoundRate[k] = float64(v) / float64(boots)
		}
	}
	for k, v := range classes {
		s.ClassRate[k] = float64(v) / float64(max(1, s.Requests))
	}
	s.BlockRate = s.ClassRate["rate_limited"] + s.ClassRate["login_wall"] + s.ClassRate["checkpoint"]
	s.MedianPosts, s.MedianDays = quantile(posts, 0.5), quantile(days, 0.5)
	for ord, rs := range ratios {
		s.Completeness[ord] = OrderingSummary{Posts: len(rs), Median: quantile(rs, 0.5), P10: quantile(rs, 0.1), Walls: walls[ord]}
	}
	for q, bs := range bytes {
		sum := 0.0
		for _, b := range bs {
			sum += b
		}
		s.BytesByQuery[q] = ByteStats{N: len(bs), Mean: sum / float64(len(bs)), P50: quantile(bs, 0.5), P95: quantile(bs, 0.95)}
	}
	if port > 0 {
		s.Portability = float64(portOK) / float64(port)
	}
	if med > 0 {
		s.MediaSuccess = float64(medOK) / float64(med)
	}
	s.MedianMediaExpiryH = quantile(expiry, 0.5)

	s.Go = true
	check := func(ok bool, msg string) {
		mark := "PASS"
		if !ok {
			mark, s.Go = "FAIL", false
		}
		s.Verdict = append(s.Verdict, mark+" "+msg)
	}
	best := 0.0
	bestOrd := ""
	for ord, o := range s.Completeness {
		if o.Median > best {
			best, bestOrd = o.Median, ord
		}
	}
	check(best >= th.MinMedianCompleteness, fmt.Sprintf("median comment completeness %.2f (%s) >= %.2f", best, bestOrd, th.MinMedianCompleteness))
	check(s.BootstrapSuccess >= th.MinBootstrapSuccess, fmt.Sprintf("HTTP bootstrap success %.2f >= %.2f", s.BootstrapSuccess, th.MinBootstrapSuccess))
	check(s.BlockRate < th.MaxBlockRate, fmt.Sprintf("block rate %.3f < %.3f", s.BlockRate, th.MaxBlockRate))
	check(s.MedianDays >= th.MinMedianDays || s.TimelineEnd["wall"] == 0,
		fmt.Sprintf("median timeline depth %.0f days >= %.0f (or no login walls)", s.MedianDays, th.MinMedianDays))
	return s, nil
}

// Format renders a summary for humans.
func Format(s Summary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "pages: %d  requests: %d\n", s.Pages, s.Requests)
	fmt.Fprintf(&b, "bootstrap success: %.1f%%  consent walls: %d\n", 100*s.BootstrapSuccess, s.ConsentWalls)
	fmt.Fprintf(&b, "block rate: %.2f%%  classes: %v\n", 100*s.BlockRate, s.ClassRate)
	fmt.Fprintf(&b, "timeline: median %.0f posts, %.0f days; ends: %v\n", s.MedianPosts, s.MedianDays, s.TimelineEnd)
	for ord, o := range s.Completeness {
		fmt.Fprintf(&b, "comments [%s]: %d posts, median %.2f, p10 %.2f, walls %d\n", ord, o.Posts, o.Median, o.P10, o.Walls)
	}
	for q, bs := range s.BytesByQuery {
		fmt.Fprintf(&b, "bytes [%s]: n=%d mean=%.0f p50=%.0f p95=%.0f\n", q, bs.N, bs.Mean, bs.P50, bs.P95)
	}
	fmt.Fprintf(&b, "cursor portability: %.1f%%  media success: %.1f%%  median media expiry: %.0fh\n",
		100*s.Portability, 100*s.MediaSuccess, s.MedianMediaExpiryH)
	for _, v := range s.Verdict {
		fmt.Fprintln(&b, v)
	}
	if s.Go {
		b.WriteString("VERDICT: GO\n")
	} else {
		b.WriteString("VERDICT: NO-GO (re-scope: see docs/runbooks/phase0-spike.md)\n")
	}
	return b.String()
}
